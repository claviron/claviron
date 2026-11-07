//! A module's application: a backend whose handlers run on its own threads, and
//! the controller that lists and manages it like any other runtime's.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use claviron_app::{
    ApplicationControlResult, ApplicationController, ApplicationDescription,
    ApplicationId, ApplicationLifecycle, ApplicationName,
    ApplicationPoolStatus, ApplicationRuntime, ApplicationSnapshot,
};
use claviron_http::policy::HttpPolicy;
use claviron_http::socket::{Carrier, Offer, OfferInfo, SocketKind};
use claviron_http::{RouteBody, ServiceResult, err_status};
use claviron_pipeline::{
    BackendContract, BackendHandler, BackendIdentity, BackendReply,
    DispatchCall, DispatchStep, HostContext,
};
use http::{Request, StatusCode};
use tokio::sync::{oneshot, watch};

use crate::abi::{self, Application, HttpHandler, SessionHandler};
use crate::config::ApplicationConfig;
use crate::pool::Pool;
use crate::registrar::contained;
use crate::request::Exchange;
use crate::service::SendPtr;

pub(crate) const RUNTIME: &str = "module";

pub(crate) struct ModuleApplication {
    name: String,
    settings: ApplicationConfig,
    table: HttpHandler,
    websocket: Option<SessionHandler>,
    context: SendPtr,
    start: Option<unsafe extern "C" fn(*mut std::ffi::c_void) -> u32>,
    stop: Option<unsafe extern "C" fn(*mut std::ffi::c_void) -> u32>,
    pool: OnceLock<Pool>,
    accepting: AtomicBool,
    enabled: AtomicBool,
    generation: AtomicU64,
    waiting: AtomicUsize,
    open: watch::Sender<usize>,
    /// Bumped when the application stops, for sessions to hear it.
    retiring: watch::Sender<u64>,
    last_error: Mutex<Option<String>>,
}

// Safety: tables and contexts are the module's, usable from the threads it is
// called on by its contract.
unsafe impl Send for ModuleApplication {}
unsafe impl Sync for ModuleApplication {}

impl ModuleApplication {
    pub(crate) fn new(
        name: String,
        application: Application,
        table: HttpHandler,
        websocket: Option<SessionHandler>,
        settings: ApplicationConfig,
    ) -> Self {
        Self {
            name,
            settings,
            table,
            websocket,
            context: SendPtr(application.context),
            start: application.start,
            stop: application.stop,
            pool: OnceLock::new(),
            accepting: AtomicBool::new(false),
            enabled: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            waiting: AtomicUsize::new(0),
            open: watch::Sender::new(0),
            retiring: watch::Sender::new(0),
            last_error: Mutex::new(None),
        }
    }

    pub(crate) fn table(
        &self
    ) -> (HttpHandler, Option<SessionHandler>, SendPtr) {
        (self.table, self.websocket, self.context)
    }

    pub(crate) fn retiring(&self) -> watch::Receiver<u64> {
        self.retiring.subscribe()
    }

    pub(crate) fn submit(
        &self,
        task: impl FnOnce() + Send + 'static,
    ) -> bool {
        self.pool.get().is_some_and(|pool| pool.submit(task))
    }

    pub(crate) fn left_queue(&self) {
        self.waiting.fetch_sub(1, Ordering::AcqRel);
    }

    pub(crate) fn exchange_closed(&self) {
        self.open.send_modify(|open| *open -= 1);
    }

    /// Start its threads, once, in a process that serves.
    pub(crate) fn serve(&self) -> std::io::Result<()> {
        if self.pool.get().is_none() {
            let pool = Pool::start(&self.name, self.settings.instances.get())?;
            let _ = self.pool.set(pool);
        }
        Ok(())
    }

    /// Call `start` on a thread that may block, then take requests.
    pub(crate) async fn begin(self: &Arc<Self>) -> Result<(), String> {
        if self.enabled.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if let Some(start) = self.start {
            let context = self.context;
            let status = tokio::task::spawn_blocking(move || {
                let context = context;
                contained(|| unsafe { start(context.0) })
            })
            .await
            .map_err(|failure| failure.to_string())?;
            if status != abi::OK {
                let failure = format!(
                    "application '{}': start returned status {status}",
                    self.name
                );
                self.enabled.store(false, Ordering::Release);
                self.set_error(Some(failure.clone()));
                return Err(failure);
            }
        }
        self.set_error(None);
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.accepting.store(true, Ordering::Release);
        Ok(())
    }

    /// Refuse new requests, wait for the last to close, then call `stop`. Nothing
    /// bounds the wait: a native call cannot be interrupted.
    pub(crate) async fn end(self: &Arc<Self>) -> Result<(), String> {
        // Nothing was started here, or it already stopped.
        if !self.enabled.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        self.accepting.store(false, Ordering::Release);
        self.retiring.send_modify(|retiring| *retiring += 1);
        let mut open = self.open.subscribe();
        let _ = open.wait_for(|open| *open == 0).await;
        let Some(stop) = self.stop else { return Ok(()) };
        let context = self.context;
        let status = tokio::task::spawn_blocking(move || {
            let context = context;
            contained(|| unsafe { stop(context.0) })
        })
        .await
        .map_err(|failure| failure.to_string())?;
        if status != abi::OK {
            let failure = format!(
                "application '{}': stop returned status {status}",
                self.name
            );
            self.set_error(Some(failure.clone()));
            return Err(failure);
        }
        Ok(())
    }

    pub(crate) fn shutdown_threads(&self) {
        if let Some(pool) = self.pool.get() {
            pool.shutdown();
        }
    }

    fn set_error(
        &self,
        failure: Option<String>,
    ) {
        *self.last_error.lock().expect("application error") = failure;
    }

    fn snapshot(
        &self,
        runtime: &ApplicationRuntime,
    ) -> Option<ApplicationSnapshot> {
        let mut description =
            ApplicationDescription::new(RUNTIME, "application");
        description.frameworks.clone_from(&self.settings.frameworks);
        description.icon.clone_from(&self.settings.icon);
        let accepting = self.accepting.load(Ordering::Acquire);
        let last_error =
            self.last_error.lock().expect("application error").clone();
        Some(ApplicationSnapshot {
            id: ApplicationId::new(
                runtime.clone(),
                ApplicationName::new(self.name.clone()).ok()?,
            ),
            description,
            lifecycle: match (accepting, &last_error) {
                (true, _) => ApplicationLifecycle::Running,
                (false, Some(_)) => ApplicationLifecycle::Failed,
                (false, None) => ApplicationLifecycle::Stopped,
            },
            enabled: self.enabled.load(Ordering::Acquire),
            generation: self.generation.load(Ordering::Acquire),
            pool: Some(ApplicationPoolStatus {
                accepting: Some(accepting),
                instances: self.settings.instances.get().into(),
                ready: None,
                busy: None,
                retiring: None,
                backing_off: None,
                exhausted: None,
            }),
            last_error,
        })
    }
}

/// What a route's `module:` directive selects.
pub(crate) struct ModuleBackend {
    pub application: Arc<ModuleApplication>,
    /// The transport's `h1.upgrade`: what the connection's terms allow.
    pub h1_upgrade: bool,
}

impl BackendHandler for ModuleBackend {
    fn contract(&self) -> BackendContract {
        BackendContract {
            // A handler runs on another thread, where a borrowed context cannot
            // go: the connection and policy travel in the request.
            identity: BackendIdentity::Request,
            reply: BackendReply::Owner,
        }
    }
}

fn answered(status: StatusCode) -> DispatchCall {
    DispatchCall::ready(ServiceResult::Handled(err_status(status)))
}

impl DispatchStep for ModuleBackend {
    fn call(
        &self,
        request: Request<RouteBody>,
        _: Option<HostContext<'_>>,
    ) -> DispatchCall {
        let application = &self.application;
        if !application.accepting.load(Ordering::Acquire) {
            return answered(StatusCode::SERVICE_UNAVAILABLE);
        }
        let offer = request.extensions().get::<Offer>().cloned();
        if let Some(offer) = &offer {
            let OfferInfo { kind, carrier } = offer.info();
            if kind != SocketKind::WebSocket || application.websocket.is_none()
            {
                return answered(StatusCode::NOT_IMPLEMENTED);
            }
            if carrier == Carrier::H1 && !self.h1_upgrade {
                return answered(StatusCode::FORBIDDEN);
            }
        }
        let waiting = application.waiting.fetch_add(1, Ordering::AcqRel);
        if waiting >= application.settings.max_waiting {
            application.left_queue();
            return DispatchCall::ready(ServiceResult::Handled(err_status(
                StatusCode::SERVICE_UNAVAILABLE,
            )));
        }
        let queue_timeout = request
            .extensions()
            .get::<HttpPolicy>()
            .and_then(|policy| policy.requests.queue_timeout);
        let (head, answered) = oneshot::channel();
        application.open.send_modify(|open| *open += 1);
        let exchange =
            Exchange::new(request, head, offer, Arc::clone(application));
        let turn = Arc::clone(&exchange);
        if !application.submit(move || turn.run()) {
            exchange.abandon();
            application.left_queue();
            exchange.close(abi::CLOSED);
            return DispatchCall::ready(ServiceResult::Handled(err_status(
                StatusCode::SERVICE_UNAVAILABLE,
            )));
        }
        DispatchCall::response(Box::pin(async move {
            let mut guard = Unanswered(Some(exchange));
            tokio::pin!(answered);
            if let Some(limit) = queue_timeout {
                tokio::select! {
                    answer = &mut answered => return Ok(reply(answer, &mut guard)),
                    () = tokio::time::sleep(limit) => {
                        if let Some(exchange) = guard.0.as_ref().filter(|e| e.abandon()) {
                            exchange.close_abandoned();
                            guard.0 = None;
                            return Ok(err_status(StatusCode::SERVICE_UNAVAILABLE));
                        }
                    }
                }
            }
            let answer = answered.await;
            Ok(reply(answer, &mut guard))
        }))
    }
}

fn reply(
    answer: Result<claviron_http::RouteResponse, oneshot::error::RecvError>,
    guard: &mut Unanswered,
) -> claviron_http::RouteResponse {
    guard.0 = None;
    answer.unwrap_or_else(|_| err_status(StatusCode::INTERNAL_SERVER_ERROR))
}

/// While it holds the exchange, nobody has answered: if this is dropped the client
/// went away first, and the exchange ends here.
struct Unanswered(Option<Arc<Exchange>>);

impl Drop for Unanswered {
    fn drop(&mut self) {
        let Some(exchange) = self.0.take() else { return };
        if exchange.abandon() {
            exchange.close_abandoned();
        } else {
            exchange.close(abi::CLOSED);
        }
    }
}

/// Lists and manages this process's module applications.
pub(crate) struct Controller {
    runtime: ApplicationRuntime,
    applications: BTreeMap<String, Arc<ModuleApplication>>,
}

impl Controller {
    pub(crate) fn new(
        applications: BTreeMap<String, Arc<ModuleApplication>>
    ) -> Option<Self> {
        Some(Self {
            runtime: ApplicationRuntime::new(RUNTIME).ok()?,
            applications,
        })
    }

    fn find(
        &self,
        name: &ApplicationName,
    ) -> Result<&Arc<ModuleApplication>, Box<dyn std::error::Error + Send + Sync>>
    {
        self.applications
            .get(name.as_str())
            .ok_or_else(|| format!("no module application '{name}'").into())
    }
}

#[async_trait]
impl ApplicationController for Controller {
    fn runtime(&self) -> &ApplicationRuntime {
        &self.runtime
    }

    fn list(&self) -> Vec<ApplicationSnapshot> {
        self.applications
            .values()
            .filter_map(|application| application.snapshot(&self.runtime))
            .collect()
    }

    fn status(
        &self,
        name: &ApplicationName,
    ) -> Option<ApplicationSnapshot> {
        self.applications.get(name.as_str())?.snapshot(&self.runtime)
    }

    async fn start(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        Ok(self.find(name)?.begin().await?)
    }

    async fn stop(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        Ok(self.find(name)?.end().await?)
    }

    async fn restart(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        let application = self.find(name)?;
        application.end().await?;
        Ok(application.begin().await?)
    }
}
