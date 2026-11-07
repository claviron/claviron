//! What a module answers beside its applications: invocations from anywhere and
//! the application events it subscribed to, on threads declared for them. The
//! host table reaches the process through what `bind` records at boot.

use std::ffi::c_void;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use claviron_app::invoke::InvocationRouter;
use claviron_app::{ApplicationEvent, EventValue};
use claviron_runtime::registry::{self, RunnableInstance};
use claviron_runtime::{AppState, RunContext, Runnable};
use continuo::fleet::{Dispatch, InstancePlacement, Scope};
use tokio::sync::{broadcast, oneshot};

use crate::abi::{self, Call as AbiCall, Slice};
use crate::pool::Pool;
use crate::registrar::contained;
use crate::service::SendPtr;

/// A process has one set of modules, and a C function has no receiver.
static PROCESS: OnceLock<AppState> = OnceLock::new();
static THREADS: Mutex<Option<Arc<Handlers>>> = Mutex::new(None);

pub(crate) fn bind(state: &AppState) {
    let _ = PROCESS.set(state.clone());
}

pub(crate) fn set_threads(threads: Arc<Handlers>) {
    *THREADS.lock().expect("handler threads") = Some(threads);
}

pub(crate) struct Handlers {
    pool: Pool,
}

impl Handlers {
    pub(crate) fn start(instances: u16) -> std::io::Result<Self> {
        Ok(Self { pool: Pool::start("handlers", instances)? })
    }

    fn submit(
        &self,
        task: impl FnOnce() + Send + 'static,
    ) -> bool {
        self.pool.submit(task)
    }

    pub(crate) fn shutdown(&self) {
        THREADS.lock().expect("handler threads").take();
        self.pool.shutdown();
    }
}

/// An invocation waiting for its one reply.
struct Call(Mutex<Option<oneshot::Sender<Result<serde_json::Value, String>>>>);

impl Call {
    fn answer(
        &self,
        result: Result<serde_json::Value, String>,
    ) -> u32 {
        let Some(reply) = self.0.lock().expect("call").take() else {
            return abi::CLOSED;
        };
        let _ = reply.send(result);
        abi::OK
    }
}

/// Serve an operation through the router everything else invokes.
pub(crate) fn serve_operation(
    router: &InvocationRouter,
    threads: &Arc<Handlers>,
    name: String,
    operation: abi::Operation,
) -> registry::Result<()> {
    let Some(call) = operation.call else {
        return Err(registry::Error::msg(format!(
            "operation '{name}' has no call"
        )));
    };
    let context = SendPtr(operation.context);
    let threads = Arc::clone(threads);
    router.register_untyped(name, move |request: serde_json::Value| {
        let threads = Arc::clone(&threads);
        async move {
            let payload = request.to_string();
            let (reply, answered) = oneshot::channel();
            let pending = Arc::new(Call(Mutex::new(Some(reply))));
            let turn = Arc::clone(&pending);
            let submitted = threads.submit(move || {
                let context = context;
                // The module holds this until it replies, perhaps after returning,
                // so the reference is its own, released by that reply.
                let handle = Arc::into_raw(Arc::clone(&turn));
                let status = contained(|| unsafe {
                    call(
                        handle.cast_mut().cast::<AbiCall>(),
                        Slice::of(payload.as_bytes()),
                        context.0,
                    )
                });
                // A failure with no reply leaves the module nothing to reply to,
                // so the reference it was given comes back here.
                if status != abi::OK
                    && turn.answer(Err(format!(
                        "the operation returned status {status}"
                    ))) == abi::OK
                {
                    drop(unsafe { Arc::from_raw(handle) });
                }
            });
            if !submitted {
                return Err("the module is shutting down".to_owned());
            }
            answered
                .await
                .map_err(|_| "the operation ended without a reply".to_owned())?
        }
    })
}

/// A subscription, as a managed runnable: it is real work, in this process, that
/// stops with everything else.
pub(crate) fn subscriber(
    state: &AppState,
    threads: &Arc<Handlers>,
    topic: String,
    subscription: abi::Subscription,
    capacity: usize,
) -> RunnableInstance<AppState> {
    let selected = topic.clone();
    let receiver =
        state.events().subscribe_filtered_with_capacity::<ApplicationEvent>(
            capacity,
            move |event| {
                event.topic().as_str() == selected && event.target().is_none()
            },
        );
    let name = format!("module-event:{topic}");
    let subscriber: Arc<dyn Runnable<AppState>> = Arc::new(Subscriber {
        topic,
        receiver: Mutex::new(Some(receiver)),
        threads: Arc::clone(threads),
        event: subscription.event,
        context: SendPtr(subscription.context),
    });
    RunnableInstance::new(name, subscriber).placed(InstancePlacement {
        scope: Scope::Any,
        dispatch: Dispatch::Each,
        policy: None,
    })
}

struct Subscriber {
    topic: String,
    receiver: Mutex<Option<broadcast::Receiver<ApplicationEvent>>>,
    threads: Arc<Handlers>,
    event: Option<unsafe extern "C" fn(Slice, Slice, *mut c_void)>,
    context: SendPtr,
}

#[async_trait]
impl Runnable<AppState> for Subscriber {
    async fn run(
        self: Arc<Self>,
        _: (),
        _: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let Some(event) = self.event else { return Ok(()) };
        let receiver = self.receiver.lock().expect("subscriber").take();
        let Some(mut receiver) = receiver else {
            return Err(registry::Error::recoverable(format!(
                "subscription '{}' cannot be started twice",
                self.topic
            )));
        };
        loop {
            let received = tokio::select! {
                () = context.cancelled() => break,
                received = receiver.recv() => received,
            };
            let application_event = match received {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(lost)) => {
                    tracing::error!(topic = %self.topic, lost, "a module's subscription fell behind and lost events");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };
            let payload = match application_event.payload().to_json() {
                Ok(payload) => payload.to_string(),
                Err(failure) => {
                    tracing::error!(topic = %self.topic, %failure, "an application event has no JSON form");
                    continue;
                }
            };
            let (done, handled) = oneshot::channel();
            let topic = self.topic.clone();
            let module_context = self.context;
            let submitted = self.threads.submit(move || {
                let module_context = module_context;
                contained(|| {
                    unsafe {
                        event(
                            Slice::of(topic.as_bytes()),
                            Slice::of(payload.as_bytes()),
                            module_context.0,
                        );
                    }
                    abi::OK
                });
                let _ = done.send(());
            });
            if !submitted {
                break;
            }
            // One at a time: a slow subscriber falls behind, it does not pile up.
            let _ = handled.await;
        }
        // A restarted generation hears from here on.
        Ok(())
    }
}

/// Take back the reference a module was given, ending the call's handle.
unsafe fn finish(
    call: *mut AbiCall,
    result: Result<serde_json::Value, String>,
) -> u32 {
    if call.is_null() {
        return abi::INVALID_ARGUMENT;
    }
    // Safety: this handle came from `Arc::into_raw` above, and a module replies
    // exactly once.
    let call = unsafe { Arc::from_raw(call.cast_const().cast::<Call>()) };
    call.answer(result)
}

fn json(slice: Slice) -> Result<serde_json::Value, ()> {
    // Safety: a module passes bytes readable for the call.
    serde_json::from_slice(unsafe { slice.bytes() }).map_err(|_| ())
}

pub(crate) unsafe extern "C" fn call_reply(
    call: *mut AbiCall,
    value: Slice,
) -> u32 {
    contained(|| match json(value) {
        Ok(value) => unsafe { finish(call, Ok(value)) },
        Err(()) => abi::INVALID_ARGUMENT,
    })
}

pub(crate) unsafe extern "C" fn call_fail(
    call: *mut AbiCall,
    reason: Slice,
) -> u32 {
    contained(|| {
        // Safety: a module passes bytes readable for the call.
        let reason = String::from_utf8_lossy(unsafe { reason.bytes() });
        unsafe { finish(call, Err(reason.into_owned())) }
    })
}

pub(crate) unsafe extern "C" fn publish_event(
    topic: Slice,
    value: Slice,
) -> u32 {
    contained(|| {
        let Some(state) = PROCESS.get() else { return abi::CLOSED };
        // Safety: a module passes bytes readable for the call.
        let Ok(topic) = std::str::from_utf8(unsafe { topic.bytes() }) else {
            return abi::INVALID_ARGUMENT;
        };
        let Ok(value) = json(value) else { return abi::INVALID_ARGUMENT };
        let Ok(event) =
            ApplicationEvent::new(topic, EventValue::from_json(&value))
        else {
            return abi::INVALID_ARGUMENT;
        };
        state.events().emit(event);
        abi::OK
    })
}

pub(crate) unsafe extern "C" fn invoke(
    operation: Slice,
    value: Slice,
    callback: Option<abi::InvokeCallback>,
    context: *mut c_void,
) -> u32 {
    contained(|| {
        let Some(callback) = callback else { return abi::INVALID_ARGUMENT };
        let Some(state) = PROCESS.get() else { return abi::CLOSED };
        let Some(threads) = THREADS.lock().expect("handler threads").clone()
        else {
            return abi::UNSUPPORTED;
        };
        let Some(router) = state.registry_ref().resolve::<InvocationRouter>()
        else {
            return abi::UNSUPPORTED;
        };
        // Safety: a module passes bytes readable for the call.
        let Ok(operation) = std::str::from_utf8(unsafe { operation.bytes() })
        else {
            return abi::INVALID_ARGUMENT;
        };
        let operation = operation.to_owned();
        let Ok(request) = json(value) else { return abi::INVALID_ARGUMENT };
        let Ok(runtime) = RUNTIME.get().ok_or(()) else { return abi::CLOSED };
        let context = SendPtr(context);
        runtime.spawn(async move {
            let result = router.invoke(&operation, request).await;
            threads.submit(move || {
                let context = context;
                let (status, data) = match result {
                    Ok(value) => (abi::OK, value.to_string()),
                    Err(reason) => (abi::CLOSED, reason),
                };
                contained(|| {
                    unsafe {
                        callback(status, Slice::of(data.as_bytes()), context.0);
                    }
                    abi::OK
                });
            });
        });
        abi::OK
    })
}

/// A module's thread is not on the runtime, so it is told which one to use.
static RUNTIME: OnceLock<tokio::runtime::Handle> = OnceLock::new();

pub(crate) fn bind_runtime() {
    let _ = RUNTIME.set(tokio::runtime::Handle::current());
}
