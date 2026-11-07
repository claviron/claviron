//! The one component a process's modules live behind: the registry takes one
//! instance per Rust type, and every native component would be the same type.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::c_void;
use std::mem::ManuallyDrop;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use claviron_runtime::congif::{self, ConfigFile, Configurable};
use claviron_runtime::registry::{self, Reloadable, RunnableInstance};
use claviron_runtime::{
    AppState, Bootable, Component, ConfigCheck, Finalizable, Runnable,
};
use libloading::os::unix::{Library, RTLD_LOCAL, RTLD_NOW};

use crate::abi;
use crate::application::{Controller, ModuleApplication, ModuleBackend};
use crate::config::{CONFIG_FILE, ModulesConfig};
use crate::handlers::{self, Handlers};
use crate::registrar::{Registration, contained, register};
use crate::service::{ModuleService, SendPtr};

fn error(message: impl Into<String>) -> registry::Error {
    registry::Error::msg(message.into())
}

/// A registered lifecycle, readable from the blocking pool.
#[derive(Clone, Copy)]
struct Lifecycle {
    context: SendPtr,
    boot:
        Option<unsafe extern "C" fn(*const abi::BootInfo, *mut c_void) -> u32>,
    reload: Option<unsafe extern "C" fn(*mut c_void) -> u32>,
    finalize: Option<unsafe extern "C" fn(*mut c_void)>,
}

impl From<abi::Lifecycle> for Lifecycle {
    fn from(table: abi::Lifecycle) -> Self {
        Self {
            context: SendPtr(table.context),
            boot: table.boot,
            reload: table.reload,
            finalize: table.finalize,
        }
    }
}

struct Loaded {
    name: String,
    lifecycle: Option<Lifecycle>,
    /// Held for the process. A library is never unloaded: its threads and
    /// thread-local destructors may outlive anything that could safely close it.
    _library: Option<ManuallyDrop<Library>>,
}

#[derive(Default)]
struct Modules {
    loaded: Vec<Loaded>,
    booted: usize,
    services: Vec<RunnableInstance<AppState>>,
    applications: BTreeMap<String, Arc<ModuleApplication>>,
    operations: Vec<(String, abi::Operation)>,
    subscriptions: Vec<(String, abi::Subscription)>,
    handlers: Option<Arc<Handlers>>,
}

// Safety: the tables hold a module's function pointers and contexts, which its
// contract makes usable from the threads it is called on.
unsafe impl Send for Modules {}

pub struct ModuleProvider {
    source: PathBuf,
    applied: Mutex<Option<ModulesConfig>>,
    modules: Mutex<Modules>,
}

impl ModuleProvider {
    pub(crate) fn new(state: &AppState) -> Self {
        Self {
            source: state.config_dir().join(CONFIG_FILE),
            applied: Mutex::new(None),
            modules: Mutex::default(),
        }
    }

    /// Load one library and run its register function.
    fn load(
        path: &std::path::Path
    ) -> registry::Result<(Library, Registration)> {
        let shown = path.display().to_string();
        // Safety: loading runs the library's initializers. Naming a library in
        // modules.yaml is trusting it; RTLD_LOCAL keeps its symbols to itself.
        let library =
            unsafe { Library::open(Some(path), RTLD_NOW | RTLD_LOCAL) }
                .map_err(|failure| error(format!("{shown}: {failure}")))?;
        // Safety: the symbols' types are this ABI's, checked by version first.
        let registration = unsafe {
            let version = library
                .get::<abi::AbiVersionFn>(abi::ABI_VERSION_SYMBOL)
                .map_err(|failure| error(format!("{shown}: {failure}")))?;
            let found = version();
            if found != abi::ABI_VERSION {
                return Err(error(format!(
                    "{shown}: module ABI {found}, this server has {}",
                    abi::ABI_VERSION
                )));
            }
            let entry = library
                .get::<abi::RegisterFn>(abi::REGISTER_SYMBOL)
                .map_err(|failure| error(format!("{shown}: {failure}")))?;
            register(*entry, &shown).map_err(error)?
        };
        Ok((library, registration))
    }

    /// Hold what the libraries registered against what the file declares. Each
    /// mismatch is an error, so a typo cannot silently leave something unplaced.
    fn collect(
        config: &ModulesConfig,
        registered: Vec<(String, Option<Library>, Registration)>,
        role_is_master: bool,
    ) -> registry::Result<Modules> {
        let mut modules = Modules::default();
        let mut names = BTreeSet::new();
        for (library_name, library, registration) in registered {
            let mut runs_here = !role_is_master;
            for (name, service) in registration.services {
                if !names.insert(name.clone()) {
                    return Err(error(format!(
                        "service '{name}' is registered by more than one module"
                    )));
                }
                let Some(declared) = config.services.get(&name) else {
                    return Err(error(format!(
                        "{library_name} registered service '{name}', which {CONFIG_FILE} does not declare"
                    )));
                };
                let placement = declared.placement();
                runs_here |= role_is_master
                    && placement.scope != continuo::fleet::Scope::Worker;
                let service = ModuleService::new(name.clone(), service)
                    .ok_or_else(|| {
                        error(format!("service '{name}' has no run"))
                    })?;
                let service: Arc<dyn Runnable<AppState>> = Arc::new(service);
                modules.services.push(
                    RunnableInstance::new(name, service).placed(placement),
                );
            }
            for (name, application, http, websocket) in
                registration.applications
            {
                let Some(settings) = config.applications.get(&name) else {
                    return Err(error(format!(
                        "{library_name} registered application '{name}', which {CONFIG_FILE} does not declare"
                    )));
                };
                runs_here = runs_here || !role_is_master;
                let module = Arc::new(ModuleApplication::new(
                    name.clone(),
                    application,
                    http,
                    websocket,
                    settings.clone(),
                ));
                if modules.applications.insert(name.clone(), module).is_some() {
                    return Err(error(format!(
                        "application '{name}' is registered by more than one module"
                    )));
                }
            }
            modules.operations.extend(registration.operations);
            modules.subscriptions.extend(registration.subscriptions);
            modules.loaded.push(Loaded {
                name: library_name,
                // A process with nothing of this module to run boots nothing of it.
                lifecycle: registration
                    .lifecycle
                    .filter(|_| runs_here)
                    .map(Lifecycle::from),
                _library: library.map(ManuallyDrop::new),
            });
        }
        for name in config.services.keys() {
            if !names.contains(name) {
                return Err(error(format!(
                    "{CONFIG_FILE} declares service '{name}', which no module registered"
                )));
            }
        }
        for name in config.applications.keys() {
            if !modules.applications.contains_key(name) {
                return Err(error(format!(
                    "{CONFIG_FILE} declares application '{name}', which no module registered"
                )));
            }
        }
        let mut seen = BTreeSet::new();
        for (name, _) in &modules.operations {
            if !seen.insert(name) {
                return Err(error(format!(
                    "operation '{name}' is registered by more than one module"
                )));
            }
        }
        let needs_threads =
            !modules.operations.is_empty() || !modules.subscriptions.is_empty();
        match (needs_threads, &config.handlers) {
            (true, None) => {
                return Err(error(format!(
                    "a module registered operations or subscriptions: {CONFIG_FILE} must declare handlers"
                )));
            }
            (false, Some(_)) => {
                return Err(error(format!(
                    "{CONFIG_FILE} declares handlers, but no module registered an operation or a subscription"
                )));
            }
            _ => {}
        }
        match (modules.subscriptions.is_empty(), &config.events) {
            (false, None) => {
                return Err(error(format!(
                    "a module registered subscriptions: {CONFIG_FILE} must declare events"
                )));
            }
            (true, Some(_)) => {
                return Err(error(format!(
                    "{CONFIG_FILE} declares events, but no module subscribed to anything"
                )));
            }
            _ => {}
        }
        Ok(modules)
    }

    /// The owner a route's `module:` selects. Called while hosts compile, after
    /// this provider has booted; it is called many times and only looks up.
    pub(crate) fn backend(
        &self,
        name: &str,
        h1_upgrade: bool,
    ) -> registry::Result<Arc<dyn claviron_pipeline::BackendHandler>> {
        let modules = self.modules.lock().expect("modules lock");
        let application = modules.applications.get(name).ok_or_else(|| {
            error(format!("no module registered an application '{name}'"))
        })?;
        Ok(Arc::new(ModuleBackend {
            application: Arc::clone(application),
            h1_upgrade,
        }))
    }

    fn applications(&self) -> Vec<Arc<ModuleApplication>> {
        let modules = self.modules.lock().expect("modules lock");
        modules.applications.values().cloned().collect()
    }

    /// Load every library the file lists and hold them against it.
    fn load_all(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        let config = state.config_reader().snapshot::<Self>()?;
        let mut registered = Vec::new();
        for path in &config.modules {
            let (library, registration) = Self::load(path)?;
            registered.push((
                path.display().to_string(),
                Some(library),
                registration,
            ));
        }
        let collected = Self::collect(&config, registered, state.is_master())?;
        *self.modules.lock().expect("modules lock") = collected;
        Ok(())
    }

    /// Start what operations and subscribers run on, and make them reachable.
    /// They read the module's state, so this follows its `boot`.
    fn start_handlers(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        let config = state.config_reader().snapshot::<Self>()?;
        let mut modules = self.modules.lock().expect("modules lock");
        handlers::bind(state);
        handlers::bind_runtime();
        let Some(declared) = &config.handlers else { return Ok(()) };
        let threads = Arc::new(
            Handlers::start(declared.instances.get())
                .map_err(|failure| error(failure.to_string()))?,
        );
        handlers::set_threads(Arc::clone(&threads));
        let router = state
            .registry_ref()
            .resolve::<claviron_app::invoke::InvocationRouter>()
            .ok_or_else(|| error("the invocation router is not registered"))?;
        for (name, operation) in std::mem::take(&mut modules.operations) {
            handlers::serve_operation(&router, &threads, name, operation)?;
        }
        if let Some(events) = &config.events {
            for (topic, subscription) in
                std::mem::take(&mut modules.subscriptions)
            {
                let subscriber = handlers::subscriber(
                    state,
                    &threads,
                    topic,
                    subscription,
                    events.capacity.get(),
                );
                modules.services.push(subscriber);
            }
        }
        modules.handlers = Some(threads);
        Ok(())
    }

    fn lifecycles(&self) -> Vec<(String, Lifecycle)> {
        self.modules
            .lock()
            .expect("modules lock")
            .loaded
            .iter()
            .filter_map(|module| Some((module.name.clone(), module.lifecycle?)))
            .collect()
    }

    fn take_booted(&self) -> Vec<(String, Lifecycle)> {
        let mut lifecycles = self.lifecycles();
        let mut modules = self.modules.lock().expect("modules lock");
        lifecycles.truncate(std::mem::take(&mut modules.booted));
        lifecycles.reverse();
        lifecycles
    }
}

#[async_trait]
impl Configurable for ModuleProvider {
    type Config = ModulesConfig;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.source)
    }

    async fn configure(
        &self,
        config: &ModulesConfig,
    ) -> congif::Result<()> {
        // A library cannot be unloaded and a running one is not swapped: a
        // change takes a restart, and saying so is better than half applying it.
        let mut applied = self.applied.lock().expect("applied lock");
        match &*applied {
            Some(running) if running != config => Err(congif::Error::msg(
                format!("{CONFIG_FILE} changed: this takes a server restart"),
            )),
            Some(_) => Ok(()),
            None => {
                *applied = Some(config.clone());
                Ok(())
            }
        }
    }
}

impl Component<AppState> for ModuleProvider {
    type Deps = ();

    fn name(&self) -> &'static str {
        "ModuleProvider"
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<AppState>> {
        Some(self)
    }

    fn runnables(self: Arc<Self>) -> Vec<RunnableInstance<AppState>> {
        self.modules.lock().expect("modules lock").services.clone()
    }
}

#[async_trait]
impl Bootable<AppState, ()> for ModuleProvider {
    async fn boot(
        &self,
        _: (),
        state: &AppState,
    ) -> registry::Result<()> {
        self.load_all(state)?;

        // A check validates what would run and runs nothing.
        if state.registry_ref().resolve::<ConfigCheck>().is_some() {
            self.modules.lock().expect("modules lock").services.clear();
            return Ok(());
        }
        let config_dir = state.config_dir().display().to_string();
        let environment = state.environment().to_string();
        for (name, lifecycle) in self.lifecycles() {
            let Some(boot) = lifecycle.boot else {
                self.modules.lock().expect("modules lock").booted += 1;
                continue;
            };
            let (dir, env) = (config_dir.clone(), environment.clone());
            let status = tokio::task::spawn_blocking(move || {
                let lifecycle = lifecycle;
                let info = abi::BootInfo {
                    struct_size: size_of::<abi::BootInfo>() as u32,
                    config_dir: abi::Slice::of(dir.as_bytes()),
                    environment: abi::Slice::of(env.as_bytes()),
                };
                contained(|| unsafe { boot(&info, lifecycle.context.0) })
            })
            .await
            .map_err(|failure| error(failure.to_string()))?;
            if status != abi::OK {
                return Err(error(format!(
                    "{name}: boot returned status {status}"
                )));
            }
            self.modules.lock().expect("modules lock").booted += 1;
        }
        // A master serves nothing. Its workers list and run the applications,
        // and answer the operations and events.
        if state.is_master() {
            return Ok(());
        }
        self.start_handlers(state)?;
        let applications =
            self.modules.lock().expect("modules lock").applications.clone();
        if applications.is_empty() {
            return Ok(());
        }
        for application in applications.values() {
            application
                .serve()
                .map_err(|failure| error(failure.to_string()))?;
            application.begin().await.map_err(error)?;
        }
        let manager = state
            .registry_ref()
            .resolve::<claviron_app::ApplicationManager>()
            .ok_or_else(|| {
                error("the application manager is not registered")
            })?;
        let controller = Controller::new(applications)
            .ok_or_else(|| error("the module runtime name is invalid"))?;
        manager
            .add(Arc::new(controller))
            .map_err(|failure| error(failure.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl Reloadable<AppState> for ModuleProvider {
    async fn reload(
        &self,
        _: &AppState,
    ) -> registry::Result<()> {
        for (name, lifecycle) in self.lifecycles() {
            let Some(reload) = lifecycle.reload else { continue };
            let status = tokio::task::spawn_blocking(move || {
                let lifecycle = lifecycle;
                contained(|| unsafe { reload(lifecycle.context.0) })
            })
            .await
            .map_err(|failure| error(failure.to_string()))?;
            if status != abi::OK {
                return Err(error(format!(
                    "{name}: reload returned status {status}"
                )));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Finalizable<AppState> for ModuleProvider {
    async fn finalize(
        &self,
        _: &AppState,
    ) -> registry::Result<()> {
        // Whatever is still running has drained by now; a handler that hung
        // holds this up, since a native call cannot be interrupted.
        for application in self.applications() {
            if let Err(failure) = application.end().await {
                tracing::error!(%failure, "module application did not stop cleanly");
            }
            tokio::task::spawn_blocking(move || application.shutdown_threads())
                .await
                .map_err(|failure| error(failure.to_string()))?;
        }
        let threads =
            self.modules.lock().expect("modules lock").handlers.take();
        if let Some(threads) = threads {
            tokio::task::spawn_blocking(move || threads.shutdown())
                .await
                .map_err(|failure| error(failure.to_string()))?;
        }
        for (_, lifecycle) in self.take_booted() {
            let Some(finalize) = lifecycle.finalize else { continue };
            tokio::task::spawn_blocking(move || {
                let lifecycle = lifecycle;
                contained(|| {
                    unsafe { finalize(lifecycle.context.0) };
                    abi::OK
                })
            })
            .await
            .map_err(|failure| error(failure.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/provider.rs"]
mod tests;
