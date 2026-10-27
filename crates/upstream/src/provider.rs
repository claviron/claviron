//! Runtime provider for `config/upstream.yaml`.

use std::path::PathBuf;
#[cfg(feature = "metrics")]
use std::sync::RwLock;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use claviron_orbit::FleetEventRegistry;
use claviron_orbit::lock::OrbitLock;
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron_runtime::registry::{
    Bootable, Component, Reloadable, Resolve, Runnable, priority,
};
use claviron_runtime::{AppState, Register, RunContext, ServiceStartPolicy};

use crate::app::UpstreamApp;
use crate::compile::Validation;
use crate::compile::profile::Profile;
use crate::config::UpstreamYaml;
#[cfg(feature = "metrics")]
use crate::config::metrics::UpstreamMetricsConfig;
use crate::health::event::{
    OriginHealthEvent, OriginServingEvent, RecoveryVacatedEvent,
};
use crate::health::lock::UpstreamLock;
use crate::proxy::service::UpstreamService;

pub fn upstreams_config_path(state: &AppState) -> PathBuf {
    state.config_dir().join("upstream.yaml")
}

pub struct UpstreamProvider {
    config_path: PathBuf,
    state: AppState,
    rebuild_lock: tokio::sync::Mutex<()>,
    warm: Arc<crate::proxy::service::warm::Owner>,
    pub(crate) resolution: Arc<crate::resolve::control::Control>,
    resolution_table: OnceLock<Arc<crate::resolve::ResolutionTable>>,
    state_table: OnceLock<Arc<crate::state::UpstreamStateTable>>,
    locks: OnceLock<Option<UpstreamLock>>,
    pub(crate) registry: Arc<registry::Registry>,
    #[cfg(feature = "metrics")]
    metrics: RwLock<UpstreamMetricsConfig>,
}

impl UpstreamProvider {
    pub fn new(state: &AppState) -> Self {
        Self {
            config_path: upstreams_config_path(state),
            state: state.clone(),
            rebuild_lock: tokio::sync::Mutex::new(()),
            warm: Arc::new(crate::proxy::service::warm::Owner::default()),
            resolution: Arc::new(crate::resolve::control::Control::default()),
            resolution_table: OnceLock::new(),
            state_table: OnceLock::new(),
            locks: OnceLock::new(),
            registry: registry::Registry::new(),
            #[cfg(feature = "metrics")]
            metrics: RwLock::new(UpstreamMetricsConfig::default()),
        }
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn metrics_options(&self) -> UpstreamMetricsConfig {
        self.metrics
            .read()
            .expect("upstream metrics config lock poisoned")
            .clone()
    }

    pub fn profile(
        &self,
        name: &str,
    ) -> Option<Arc<Profile>> {
        self.registry.profile(name)
    }
    pub fn service(
        &self,
        name: &str,
    ) -> Option<UpstreamService> {
        self.registry.service(name)
    }
    /// Publish a compiled member-only candidate against the immutable view/epoch
    /// target captured before preparation with membership_target(). A late candidate cannot replace a newer
    /// membership or a reloaded/removed profile. The process retirement owner
    /// joins the replaced view even when the publishing caller is dropped.
    ///
    /// This local operation does not acknowledge fleet health-slot retirement.
    pub fn publish_membership(
        &self,
        expected: &crate::proxy::service::MembershipTarget,
        profile: Arc<Profile>,
    ) -> Result<UpstreamService, crate::proxy::service::MembershipError> {
        self.registry.publish_membership(expected, profile)
    }
    /// Publish the complete named member view while excluding authoritative
    /// revision changes through its synchronous seal/transfer. Stale/invalid
    /// preparation preserves serving/app bindings and aborts only inert new owners.
    pub fn publish_resolved_membership(
        &self,
        expected: &crate::proxy::service::MembershipTarget,
        profile: Arc<Profile>,
        publications: &[(crate::resolve::ResolutionEntry, u64)],
    ) -> Result<UpstreamService, crate::proxy::service::MembershipError> {
        self.registry.publish_resolved_membership(
            expected,
            profile,
            publications,
        )
    }
    /// Runtime context belongs to this provider's boot state; host calls share it.
    pub fn service_for_state(
        &self,
        name: &str,
        _state: &AppState,
    ) -> Option<UpstreamService> {
        self.service(name)
    }
    pub fn app(
        &self,
        name: &str,
    ) -> Option<UpstreamApp> {
        self.registry.app(name)
    }
    pub fn app_for_state(
        &self,
        name: &str,
        _state: &AppState,
    ) -> Option<UpstreamApp> {
        self.app(name)
    }
    fn runtime(
        &self,
        state: &AppState,
        deps: &UpstreamDeps,
    ) -> claviron_runtime::registry::Result<
        crate::proxy::service::runtime::ServiceRuntime,
    > {
        let state_table = match &deps.1 {
            Some(provider) => provider.handle().ok_or_else(|| {
                claviron_runtime::registry::Error::msg(
                    "UpstreamStateProvider is not booted",
                )
            })?,
            None if fleet_of(deps).is_some() => {
                return Err(claviron_runtime::registry::Error::msg(
                    "UpstreamStateProvider is required for an Orbit fleet",
                ));
            }
            None => self
                .state_table
                .get_or_init(|| {
                    crate::state::UpstreamStateTable::memory(
                        crate::state::layout::CAPACITY,
                    )
                })
                .clone(),
        };
        if self.locks.get().is_none() {
            let lock = first_lock_for_state(state, deps)?;
            let _ = self.locks.set(lock);
        }
        Ok(crate::proxy::service::runtime::ServiceRuntime {
            state_table: state_table.clone(),
            lock: self
                .locks
                .get()
                .expect("provider lock capability initialized")
                .clone(),
            serving_role: !state.is_master(),
            // Workers are numbered from one.
            worker: state
                .worker_id()
                .map_or(0, |id| usize::from(id).saturating_sub(1)),
            shutdown: state.shutdown_token(),
            warm_owner: Some(Arc::clone(&self.warm)),
            events: Some(state.events().bus().clone()),
            ..crate::proxy::service::runtime::ServiceRuntime::with_table(
                state_table.clone(),
            )
        })
    }
    async fn rebuild(
        &self,
        state: &AppState,
        deps: &UpstreamDeps,
        validation: Validation,
    ) -> claviron_runtime::registry::Result<()> {
        let _rebuild = self.rebuild_lock.lock().await;
        // Congif prepared this before boot or as the reload batch; an absent
        // `upstream.yaml` arrives as the empty document.
        let config = state.config_reader().snapshot::<Self>()?;
        if config.is_empty() {
            let pause = self.resolution.pause();
            pause.joined().await;
            self.resolution.publish(Default::default());
            let retired = self.registry.adopt(None, self.runtime(state, deps)?);
            for service in retired {
                service.wait_stopped().await;
                self.release_retired_l7(&service);
            }
            #[cfg(feature = "metrics")]
            if let Ok(mut metrics) = self.metrics.write() {
                *metrics = config.metrics.clone();
            }
            tracing::debug!(
                path = %self.config_path.display(),
                "no upstreams declared; HTTP upstream disabled"
            );
            return Ok(());
        }
        self.rebuild_dns(state, deps, &config, &validation).await
    }

    fn release_retired_l7(
        &self,
        retired: &UpstreamService,
    ) {
        if !retired.inner.profile.health.l7.enabled {
            return;
        }
        let next = self.registry.profile(&retired.inner.profile.name);
        let observer = self.registry.service(&retired.inner.profile.name);
        for (index, origin) in retired.inner.profile.origins.iter().enumerate()
        {
            if next.as_ref().is_some_and(|profile| {
                profile.health.l7.enabled
                    && profile
                        .origins
                        .iter()
                        .any(|current| current.state_key == origin.state_key)
            }) {
                continue;
            }
            // The old local writer has joined; removed/disabled L7 policy no
            // longer owns a deny bit. Preserve all independent serving causes.
            let before = origin.slot.get().serving();
            if !origin.slot.get().set_l7_serving(crate::origin::Serving::Yes) {
                continue;
            }
            let after = origin.slot.get().serving();
            if let Some(observer) = &observer
                && let Some(current) =
                    observer.inner.profile.origins.iter().position(|current| {
                        current.state_key == origin.state_key
                    })
            {
                observer.refresh_origin_health(current);
            }
            retired.publish_origin_serving_event(index, before, after);
        }
    }
}

/// The DNS counters a fleet process records resolutions in.
#[cfg(feature = "metrics")]
type CounterDeps = Option<Arc<claviron_orbit::CounterProvider>>;
#[cfg(not(feature = "metrics"))]
type CounterDeps = ();

/// What upstream keeps its state, resolutions and DNS counters in: the
/// fleet's tables when the process has an Orbit fleet, memory when it has
/// none. Boot is handed it; a reload resolves it.
pub(crate) type UpstreamDeps = (
    Option<Arc<claviron_orbit::FleetProvider>>,
    Option<Arc<crate::state::provider::UpstreamStateProvider>>,
    Option<Arc<crate::resolve::table::provider::ResolutionProvider>>,
    CounterDeps,
);

/// The fleet, when the process has one.
pub(crate) fn fleet_of(
    (fleet, ..): &UpstreamDeps
) -> Option<Arc<claviron_orbit::Fleet>> {
    fleet.as_ref().and_then(|provider| provider.handle())
}

fn first_lock_for_state(
    state: &AppState,
    deps: &UpstreamDeps,
) -> claviron_runtime::registry::Result<Option<UpstreamLock>> {
    let Some(fleet) = fleet_of(deps) else {
        return Ok(None);
    };
    let owner = format!("node:{}", fleet.node_id().get());
    let locks = OrbitLock::new(fleet).map_err(|error| {
        claviron_runtime::registry::Error::msg(format!(
            "upstream fleet locks are unavailable: {error}"
        ))
    })?;
    Ok(Some(UpstreamLock::new(locks, owner, state.clone())))
}

impl Component<AppState> for UpstreamProvider {
    type Deps = UpstreamDeps;

    fn name(&self) -> &'static str {
        "UpstreamProvider"
    }

    fn boot_priority(&self) -> Option<u8> {
        Some(priority::NORMAL)
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    /// A profile that names an identity or our own authority reads the
    /// certificate material `CertResolverProvider` publishes in its boot, not
    /// the provider itself, so this is an ordering rather than a dependency.
    /// The edge is silently absent when no certificate manager is registered,
    /// which is the same condition under which such a profile fails with its
    /// own message.
    fn order(&self) -> claviron_runtime::ComponentOrder {
        claviron_runtime::ComponentOrder::new()
            .after::<claviron_tls::CertResolverProvider>()
    }

    fn as_finalizable(
        &self
    ) -> Option<&dyn claviron_runtime::registry::Finalizable<AppState>> {
        Some(self)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, UpstreamDeps> for UpstreamProvider {
    async fn boot(
        &self,
        deps: UpstreamDeps,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        // Boot precedes the first supervisor snapshot. This is a config
        // validation hint, never a live pool budget or peer routing table.
        self.rebuild(
            state,
            &deps,
            Validation {
                healthy_worker_count: Some(
                    state.serving_worker_count() as usize
                ),
            },
        )
        .await?;

        register_upstream_orbit_events(state)?;
        if state.is_worker() {
            tracing::debug!(
                "📦 UpstreamProvider loaded {}",
                self.config_path.display()
            );
        } else {
            tracing::info!(
                "📦 UpstreamProvider loaded {}",
                self.config_path.display()
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Runnable<AppState, UpstreamDeps> for UpstreamProvider {
    fn start_policy(
        &self,
        _state: &AppState,
    ) -> ServiceStartPolicy {
        ServiceStartPolicy::Automatic
    }

    async fn run(
        self: Arc<Self>,
        _deps: UpstreamDeps,
        state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        let generation = self
            .warm
            .start(&context.cancellation_token())
            .map_err(claviron_runtime::registry::Error::msg)?;
        let shutdown = state.shutdown_token();
        loop {
            // Subscribe before reading the registry so a concurrent reload
            // cannot publish a service between the warm pass and the wait.
            let changed = self.registry.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            for service in self.registry.services() {
                service.warm_pools();
            }
            tokio::select! {
                _ = context.cancelled() => break,
                _ = shutdown.cancelled() => break,
                _ = &mut changed => {}
            }
        }
        generation.drain().await;
        Ok(())
    }
}

#[async_trait]
impl Reloadable<AppState> for UpstreamProvider {
    fn priority(&self) -> Option<u8> {
        Some(priority::EARLY + 25)
    }

    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let deps =
            <UpstreamDeps as Resolve<AppState>>::resolve(state.registry_ref())?;
        // An empty upstream configuration remains a no-op even if the
        // supervisor observation is temporarily unavailable.
        if state.config_reader().snapshot::<Self>()?.is_empty() {
            return self.rebuild(state, &deps, Validation::default()).await;
        }
        let members = claviron_orbit::service::current_members(state)
            .map_err(claviron_runtime::registry::Error::msg)?;
        self.rebuild(
            state,
            &deps,
            Validation {
                healthy_worker_count: Some(
                    members
                        .iter()
                        .filter(|m| m.role != claviron_orbit::service::FleetMemberRole::Master)
                        .count()
                )
            }
        )
        .await?;
        tracing::info!(
            "🔁 UpstreamProvider reloaded {}",
            self.config_path.display()
        );
        Ok(())
    }
}

#[async_trait]
impl Configurable for UpstreamProvider {
    type Config = UpstreamYaml;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    /// No file, no upstreams: the proxy stays registered and disabled.
    fn absent_source(&self) -> Option<UpstreamYaml> {
        Some(UpstreamYaml::default())
    }

    async fn configure(
        &self,
        _config: &UpstreamYaml,
    ) -> congif::Result<()> {
        // Compilation waits until boot: it needs the fleet, resolvers and the
        // certificate manager, and a worker-count hint only boot has.
        Ok(())
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for UpstreamProvider {
    async fn config_changed(
        &self,
        _config: &UpstreamYaml,
    ) -> congif::Result<()> {
        Reloadable::reload(self, &self.state).await.map_err(congif::Error::msg)
    }
}

impl Register<AppState> for UpstreamProvider {
    fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
        register(state)
    }
}

pub fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
    claviron_orbit::register_lock(state)?;
    crate::state::provider::register(state)?;
    crate::resolve::table::provider::register(state)?;
    #[cfg(feature = "metrics")]
    claviron_orbit::register_counter(state)?;
    state.bind_config_environment::<UpstreamProvider>()?;
    let provider = Arc::new(UpstreamProvider::new(state));
    state.registry_ref().register(provider.clone())?;
    state.registry_ref().register(
        crate::health::active::UpstreamActiveProbe::new(
            provider.registry.clone(),
        ),
    )?;
    state.registry_ref().register(
        crate::health::recovery::UpstreamRecoveryProbe::new(
            provider.registry.clone(),
        ),
    )?;
    state.registry_ref().register(
        crate::health::fleet::UpstreamHealthBridge::new(
            provider.registry.clone(),
        ),
    )?;
    state.registry_ref().register(
        crate::resolve::runnable::UpstreamResolver::new(
            provider.resolution.clone(),
            provider.registry.clone(),
        ),
    )?;
    #[cfg(feature = "metrics")]
    crate::metrics::register(state, provider)?;
    Ok(())
}

fn register_upstream_orbit_events(
    state: &AppState
) -> claviron_runtime::registry::Result<()> {
    let Some(registry) = state.registry_ref().resolve::<FleetEventRegistry>()
    else {
        tracing::debug!(
            "FleetEventRegistry is not available; upstream health events stay local"
        );
        return Ok(());
    };
    registry.register_bridge::<OriginHealthEvent>(state)?;
    registry.register_bridge::<OriginServingEvent>(state)?;
    registry.register_bridge::<RecoveryVacatedEvent>(state)?;
    registry.register_bridge::<crate::resolve::ResolutionChanged>(state)?;
    Ok(())
}

pub(crate) mod registry;

mod finalize;

mod dns;
