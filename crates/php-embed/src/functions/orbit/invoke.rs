//! PHP-facing adapter for runtime-neutral Orbit invocations.
//!
//! `orbit_invoke` publishes under any operation. A persistent program can
//! also subscribe to operations and receive what the fleet submits under
//! them — the consumer half of an invocation, in PHP.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use claviron_app::EventValue;
use claviron_orbit::{
    Invocation, InvocationBus, InvocationProvider, InvocationSubscription,
};
use ext_php_rs::args::Arg;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::class::RegisteredClass;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::{ClassFlags, DataType};
use ext_php_rs::types::{ZendClassObject, Zval};
use ext_php_rs::zend::ExecuteData;
use ext_php_rs::{php_class, php_impl};
use futures_util::future::select_all;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::engine::PhpExecution;
use crate::event::from_zval;
use crate::extension::FunctionEntries;
use crate::functions::event::context;

pub(crate) struct PhpInvokeBridge {
    bus: Arc<InvocationBus>,
    runtime: Handle,
}

static INVOCATIONS: OnceLock<ArcSwapOption<PhpInvokeBridge>> = OnceLock::new();

fn bridge_slot() -> &'static ArcSwapOption<PhpInvokeBridge> {
    INVOCATIONS.get_or_init(ArcSwapOption::const_empty)
}

pub(crate) fn bridge() -> Result<Arc<PhpInvokeBridge>, String> {
    bridge_slot().load_full().ok_or_else(|| {
        "Orbit invocation transport is not configured".to_owned()
    })
}

pub(crate) fn configure(
    provider: Arc<InvocationProvider>
) -> claviron_runtime::registry::Result<()> {
    let bridge = provider
        .handle()
        .map(|bus| {
            Handle::try_current()
                .map(|runtime| Arc::new(PhpInvokeBridge { bus, runtime }))
                .map_err(|error| {
                    claviron_runtime::registry::Error::msg(format!(
                        "PHP invocation runtime handle is unavailable: {error}"
                    ))
                })
        })
        .transpose()?;
    let enabled = bridge.is_some();
    bridge_slot().store(bridge);
    debug!(enabled, "PHP Orbit invocation host functions configured");
    Ok(())
}

/// One invocation, as `orbit_invoke_receive()` hands it to PHP.
#[php_class]
#[php(name = "Claviron\\Runtime\\Invocation")]
#[php(flags = ClassFlags::from_bits_retain(
    ClassFlags::Final.bits()
        | ClassFlags::NoDynamicProperties.bits()
        | ClassFlags::NotSerializable.bits()
))]
pub(crate) struct PhpInvocationObject {
    id: String,
    operation: String,
    payload: Vec<u8>,
    submitted_at: i64,
}

impl From<Invocation> for PhpInvocationObject {
    fn from(invocation: Invocation) -> Self {
        Self {
            id: invocation.id.to_string(),
            operation: invocation.operation,
            payload: invocation.payload,
            submitted_at: invocation.submitted_at.as_unix_ms() as i64,
        }
    }
}

#[php_impl]
impl PhpInvocationObject {
    #[php(getter)]
    pub fn id(&self) -> String {
        self.id.clone()
    }

    #[php(getter)]
    pub fn operation(&self) -> String {
        self.operation.clone()
    }

    /// The payload `orbit_invoke()` was given, decoded; bytes that are not
    /// an event value (a Rust-side codec) come through as a binary string.
    #[php(getter)]
    pub fn payload(&self) -> Zval {
        let mut result = Zval::new();
        match EventValue::from_bytes(&self.payload) {
            Ok(value) => {
                if crate::event::value::set_zval(&value, &mut result).is_err() {
                    result.set_null();
                }
            }
            Err(_) => result.set_binary(self.payload.clone()),
        }
        result
    }

    /// Publication time, unix milliseconds.
    #[php(getter, name = "submitted_at")]
    pub fn submitted_at(&self) -> i64 {
        self.submitted_at
    }
}

/// The operations one execution listens to, one ring cursor each.
///
/// Like event topics, these belong to the execution, not to its reusable ZTS
/// thread: a persistent program keeps them for its lifetime.
pub(crate) struct InvocationSubscriptions {
    subscriptions: HashMap<String, InvocationSubscription>,
}

impl InvocationSubscriptions {
    fn new() -> Self {
        Self { subscriptions: HashMap::new() }
    }

    fn subscribe(
        &mut self,
        bridge: &PhpInvokeBridge,
        operation: String,
    ) -> Result<bool, String> {
        if self.subscriptions.contains_key(&operation) {
            return Ok(false);
        }
        // The subscription registers the ring's readiness fd with Tokio, and
        // this is a PHP thread: give it the runtime's context for that call.
        let subscription = {
            let _runtime = bridge.runtime.enter();
            bridge
                .bus
                .subscribe(operation.clone())
                .map_err(|error| error.to_string())?
        };
        self.subscriptions.insert(operation, subscription);
        Ok(true)
    }

    fn unsubscribe(
        &mut self,
        operation: &str,
    ) -> bool {
        self.subscriptions.remove(operation).is_some()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    /// The next invocation under any subscribed operation.
    pub(crate) async fn receive(&mut self) -> Result<Invocation, String> {
        let waits: Vec<_> = self
            .subscriptions
            .values_mut()
            .map(|subscription| Box::pin(subscription.receive()))
            .collect();
        let (result, _, _) = select_all(waits).await;
        result.map_err(|error| match error {
            claviron_orbit::invoke::Error::Lagged(skipped) => {
                format!("PHP invocation subscription lagged by {skipped} invocation(s)")
            }
            other => other.to_string()
        })
    }
}

impl PhpInvokeBridge {
    pub(crate) fn block_on<F, T>(
        &self,
        future: F,
    ) -> Result<T, String>
    where
        F: Future<Output = T>,
    {
        if Handle::try_current().is_ok() {
            return Err(
                "blocking PHP invocation access must run outside a Tokio runtime thread".into()
            );
        }
        Ok(self.runtime.block_on(future))
    }

    /// `timeout_ms == 0` is one poll of the ring and no wait.
    fn receive(
        &self,
        subscriptions: &mut InvocationSubscriptions,
        timeout_ms: u64,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Option<Invocation>, String> {
        if subscriptions.is_empty() {
            return Err(
                "subscribe to at least one invocation operation before receiving".to_owned()
            );
        }
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Ok(None);
        }
        let timeout = Duration::from_millis(timeout_ms);
        self.block_on(async {
            let wait = tokio::time::timeout(timeout, subscriptions.receive());
            match cancellation {
                Some(cancellation) => {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => Ok(None),
                        result = wait => match result {
                            Ok(result) => result.map(Some),
                            Err(_) => Ok(None)
                        }
                    }
                }
                None => match wait.await {
                    Ok(result) => result.map(Some),
                    Err(_) => Ok(None),
                },
            }
        })?
    }
}

pub(crate) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    let functions = [
        FunctionBuilder::new("orbit_invoke", invoke_handler)
            .arg(Arg::new("operation", DataType::String))
            .arg(Arg::new("payload", DataType::Mixed))
            .returns(DataType::String, false, false)
            .build()
            .map_err(|error| format!("orbit_invoke: {error}"))?,
        FunctionBuilder::new("orbit_invoke_subscribe", subscribe_handler)
            .arg(Arg::new("operation", DataType::String))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("orbit_invoke_subscribe: {error}"))?,
        FunctionBuilder::new("orbit_invoke_unsubscribe", unsubscribe_handler)
            .arg(Arg::new("operation", DataType::String))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("orbit_invoke_unsubscribe: {error}"))?,
        FunctionBuilder::new("orbit_invoke_receive", receive_handler)
            .not_required()
            .arg(Arg::new("timeout_ms", DataType::Long).default("0"))
            .returns(
                DataType::object(PhpInvocationObject::CLASS_NAME),
                true,
                false,
            )
            .build()
            .map_err(|error| format!("orbit_invoke_receive: {error}"))?,
    ];
    entries.extend(functions);
    Ok(())
}

fn throw(error: impl std::fmt::Display) {
    PhpException::from_message(format!("Orbit invocation failed: {error}"))
        .throw();
}

fn parse_operation(ex: &mut ExecuteData) -> Result<String, String> {
    let mut operation = Arg::new("operation", DataType::String);
    ex.parser()
        .arg(&mut operation)
        .parse()
        .map_err(|error| error.to_string())?;
    operation
        .consume::<String>()
        .map_err(|_| "invocation operation must be a string".to_owned())
}

extern "C" fn invoke_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = invoke(ex);
        match result {
            Ok(id) => {
                if let Err(error) = retval.set_string(&id, false) {
                    retval.set_null();
                    throw(error);
                }
            }
            Err(error) => {
                retval.set_null();
                throw(error);
            }
        }
    }));
}

fn invoke(ex: &mut ExecuteData) -> Result<String, String> {
    let mut operation = Arg::new("operation", DataType::String);
    let mut payload = Arg::new("payload", DataType::Mixed);
    ex.parser()
        .arg(&mut operation)
        .arg(&mut payload)
        .parse()
        .map_err(|error| error.to_string())?;
    let operation = operation
        .consume::<String>()
        .map_err(|_| "invocation operation must be a string".to_owned())?;
    let payload = payload
        .zval()
        .ok_or_else(|| "invocation payload is missing".to_owned())
        .and_then(|value| from_zval(value))?
        .to_bytes()?;
    let bridge = bridge()?;
    bridge
        .bus
        .submit(&operation, &payload)
        .map(|id| id.to_string())
        .map_err(|error| error.to_string())
}

extern "C" fn subscribe_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| {
            let operation = parse_operation(ex)?;
            let bridge = bridge()?;
            let context = context()?;
            let mut subscriptions =
                context.invocation_subscription.lock().map_err(|_| {
                    "PHP execution invocation subscription lock is poisoned"
                        .to_owned()
                })?;
            subscriptions
                .get_or_insert_with(InvocationSubscriptions::new)
                .subscribe(&bridge, operation)
        })();
        match result {
            Ok(inserted) => retval.set_bool(inserted),
            Err(error) => {
                retval.set_bool(false);
                throw(error);
            }
        }
    }));
}

extern "C" fn unsubscribe_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| {
            let operation = parse_operation(ex)?;
            let context = context()?;
            let mut subscriptions =
                context.invocation_subscription.lock().map_err(|_| {
                    "PHP execution invocation subscription lock is poisoned"
                        .to_owned()
                })?;
            Ok::<_, String>(subscriptions.as_mut().is_some_and(
                |subscriptions| subscriptions.unsubscribe(&operation),
            ))
        })();
        match result {
            Ok(removed) => retval.set_bool(removed),
            Err(error) => {
                retval.set_bool(false);
                throw(error);
            }
        }
    }));
}

extern "C" fn receive_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| {
            let mut timeout = Arg::new("timeout_ms", DataType::Long);
            ex.parser()
                .not_required()
                .arg(&mut timeout)
                .parse()
                .map_err(|error| error.to_string())?;
            let timeout = match timeout.zval() {
                None => 0,
                Some(_) => timeout.consume::<i64>().map_err(|_| {
                    "PHP invocation timeout must be an integer".to_owned()
                })?,
            };
            let timeout = u64::try_from(timeout).map_err(|_| {
                "PHP invocation timeout must not be negative".to_owned()
            })?;
            let bridge = bridge()?;
            let context = context()?;
            let cancellation = match &context.execution {
                PhpExecution::Program(program) => {
                    Some(program.cancellation.clone())
                }
                PhpExecution::Request(_) => None,
            };
            let mut subscriptions =
                context.invocation_subscription.lock().map_err(|_| {
                    "PHP execution invocation subscription lock is poisoned"
                        .to_owned()
                })?;
            let subscriptions = subscriptions.as_mut().ok_or_else(|| {
                "subscribe to at least one invocation operation before receiving".to_owned()
            })?;
            bridge.receive(subscriptions, timeout, cancellation.as_ref())
        })();

        match result {
            Ok(Some(invocation)) => {
                let object = PhpInvocationObject::from(invocation);
                if let Err(error) =
                    ZendClassObject::new(object).set_zval(retval, false)
                {
                    retval.set_null();
                    throw(error);
                }
            }
            Ok(None) => retval.set_null(),
            Err(error) => {
                retval.set_null();
                throw(error);
            }
        }
    }));
}
