//! What a library's register function says exists, and the host table it says
//! it with. Every call is contained: nothing unwinds into native code.

use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::abi::{
    self, Application, Host, HttpHandler, Lifecycle, Operation, Registrar,
    Service, SessionHandler, Slice, Subscription,
};

/// A library's registrations, filled while its register function runs.
#[derive(Default)]
pub(crate) struct Registration {
    pub lifecycle: Option<Lifecycle>,
    pub services: Vec<(String, Service)>,
    pub applications:
        Vec<(String, Application, HttpHandler, Option<SessionHandler>)>,
    pub operations: Vec<(String, Operation)>,
    pub subscriptions: Vec<(String, Subscription)>,
    pub error: Option<String>,
}

/// A contained call: a panic becomes a status.
pub(crate) fn contained(call: impl FnOnce() -> u32) -> u32 {
    catch_unwind(AssertUnwindSafe(call)).unwrap_or(abi::PANIC)
}

fn name_of(name: Slice) -> Result<String, String> {
    // Safety: a module passes bytes readable for the call.
    let bytes = unsafe { name.bytes() };
    match std::str::from_utf8(bytes) {
        Ok(name) if !name.trim().is_empty() => Ok(name.to_owned()),
        _ => Err("a name must be nonempty UTF-8".to_owned()),
    }
}

unsafe extern "C" fn register_lifecycle(
    registrar: *mut Registrar,
    lifecycle: *const Lifecycle,
) -> u32 {
    contained(|| {
        if registrar.is_null() || lifecycle.is_null() {
            return abi::INVALID_ARGUMENT;
        }
        // Safety: the registrar is the one this module's register call was
        // handed, alive for that call; a table is read once, here.
        let registration = unsafe { &mut *registrar.cast::<Registration>() };
        let lifecycle = unsafe { *lifecycle };
        if lifecycle.struct_size as usize != size_of::<Lifecycle>() {
            registration.error =
                Some("claviron_lifecycle has the wrong struct_size".into());
            return abi::INVALID_ARGUMENT;
        }
        if registration.lifecycle.replace(lifecycle).is_some() {
            registration.error = Some("a module has one lifecycle".into());
            return abi::INVALID_ARGUMENT;
        }
        abi::OK
    })
}

unsafe extern "C" fn register_service(
    registrar: *mut Registrar,
    name: Slice,
    service: *const Service,
) -> u32 {
    contained(|| {
        if registrar.is_null() || service.is_null() {
            return abi::INVALID_ARGUMENT;
        }
        // Safety: as in `register_lifecycle`.
        let registration = unsafe { &mut *registrar.cast::<Registration>() };
        let service = unsafe { *service };
        let checked = name_of(name).and_then(|name| {
            if service.struct_size as usize != size_of::<Service>() {
                return Err(format!(
                    "service '{name}': claviron_service has the wrong struct_size"
                ));
            }
            if service.run.is_none() {
                return Err(format!("service '{name}' has no run"));
            }
            Ok(name)
        });
        match checked {
            Ok(name) => {
                registration.services.push((name, service));
                abi::OK
            }
            Err(error) => {
                registration.error = Some(error);
                abi::INVALID_ARGUMENT
            }
        }
    })
}

unsafe extern "C" fn register_application(
    registrar: *mut Registrar,
    name: Slice,
    application: *const Application,
) -> u32 {
    contained(|| {
        if registrar.is_null() || application.is_null() {
            return abi::INVALID_ARGUMENT;
        }
        // Safety: as in `register_lifecycle`; tables are copied here and the
        // module's pointers are not kept.
        let registration = unsafe { &mut *registrar.cast::<Registration>() };
        let application = unsafe { *application };
        let checked = name_of(name).and_then(|name| {
            if application.struct_size as usize != size_of::<Application>() {
                return Err(format!(
                    "application '{name}': claviron_application has the wrong struct_size"
                ));
            }
            if application.http.is_null() {
                return Err(format!("application '{name}' has no http handler"));
            }
            let http = unsafe { *application.http };
            if http.struct_size as usize != size_of::<HttpHandler>() {
                return Err(format!(
                    "application '{name}': claviron_http_handler has the wrong struct_size"
                ));
            }
            if http.request.is_none() {
                return Err(format!(
                    "application '{name}': its http handler has no request"
                ));
            }
            let websocket = if application.websocket.is_null() {
                None
            } else {
                let websocket = unsafe { *application.websocket };
                if websocket.struct_size as usize != size_of::<SessionHandler>() {
                    return Err(format!(
                        "application '{name}': claviron_session_handler has the wrong struct_size"
                    ));
                }
                if websocket.open.is_none() || websocket.message.is_none() {
                    return Err(format!(
                        "application '{name}': its websocket handler needs open and message"
                    ));
                }
                Some(websocket)
            };
            Ok((name, http, websocket))
        });
        match checked {
            Ok((name, http, websocket)) => {
                registration.applications.push((
                    name,
                    application,
                    http,
                    websocket,
                ));
                abi::OK
            }
            Err(error) => {
                registration.error = Some(error);
                abi::INVALID_ARGUMENT
            }
        }
    })
}

unsafe extern "C" fn register_operation(
    registrar: *mut Registrar,
    name: Slice,
    operation: *const Operation,
) -> u32 {
    contained(|| {
        if registrar.is_null() || operation.is_null() {
            return abi::INVALID_ARGUMENT;
        }
        // Safety: as in `register_lifecycle`.
        let registration = unsafe { &mut *registrar.cast::<Registration>() };
        let operation = unsafe { *operation };
        let checked = name_of(name).and_then(|name| {
            if operation.struct_size as usize != size_of::<Operation>() {
                return Err(format!(
                    "operation '{name}': claviron_operation has the wrong struct_size"
                ));
            }
            if operation.call.is_none() {
                return Err(format!("operation '{name}' has no call"));
            }
            Ok(name)
        });
        match checked {
            Ok(name) => {
                registration.operations.push((name, operation));
                abi::OK
            }
            Err(error) => {
                registration.error = Some(error);
                abi::INVALID_ARGUMENT
            }
        }
    })
}

unsafe extern "C" fn register_subscription(
    registrar: *mut Registrar,
    topic: Slice,
    subscription: *const Subscription,
) -> u32 {
    contained(|| {
        if registrar.is_null() || subscription.is_null() {
            return abi::INVALID_ARGUMENT;
        }
        // Safety: as in `register_lifecycle`.
        let registration = unsafe { &mut *registrar.cast::<Registration>() };
        let subscription = unsafe { *subscription };
        let checked = name_of(topic).and_then(|topic| {
            if subscription.struct_size as usize != size_of::<Subscription>() {
                return Err(format!(
                    "subscription '{topic}': claviron_subscription has the wrong struct_size"
                ));
            }
            if subscription.event.is_none() {
                return Err(format!("subscription '{topic}' has no event"));
            }
            Ok(topic)
        });
        match checked {
            Ok(topic) => {
                registration.subscriptions.push((topic, subscription));
                abi::OK
            }
            Err(error) => {
                registration.error = Some(error);
                abi::INVALID_ARGUMENT
            }
        }
    })
}

unsafe extern "C" fn log(
    level: u32,
    message: Slice,
) {
    let _ = contained(|| {
        // Safety: a module passes bytes readable for the call.
        let bytes = unsafe { message.bytes() };
        let message = String::from_utf8_lossy(bytes);
        match level {
            abi::LOG_ERROR => {
                tracing::error!(target: "claviron::module", "{message}")
            }
            abi::LOG_WARN => {
                tracing::warn!(target: "claviron::module", "{message}")
            }
            abi::LOG_INFO => {
                tracing::info!(target: "claviron::module", "{message}")
            }
            _ => tracing::debug!(target: "claviron::module", "{message}"),
        }
        abi::OK
    });
}

pub(crate) static HOST: Host = Host {
    struct_size: size_of::<Host>() as u32,
    abi_version: abi::ABI_VERSION,
    register_lifecycle,
    register_service,
    register_application,
    register_operation,
    register_subscription,
    log,
    cancelled: crate::service::cancelled,
    cancel_wait: crate::service::cancel_wait,
    cancel_notify: crate::service::cancel_notify,
    request_field: crate::request::request_field,
    request_header_count: crate::request::request_header_count,
    request_policy: crate::request::request_policy,
    request_set_data: crate::request::request_set_data,
    request_data: crate::request::request_data,
    request_read_body: crate::request::request_read_body,
    response_begin: crate::request::response_begin,
    response_header: crate::request::response_header,
    response_write: crate::request::response_write,
    response_finish: crate::request::response_finish,
    session_accept: crate::request::session_accept,
    session_send: crate::request::session_send,
    call_reply: crate::handlers::call_reply,
    call_fail: crate::handlers::call_fail,
    publish_event: crate::handlers::publish_event,
    invoke: crate::handlers::invoke,
};

/// Run one library's register function.
pub(crate) fn register(
    entry: abi::RegisterFn,
    library: &str,
) -> Result<Registration, String> {
    let mut registration = Registration::default();
    let registrar = (&raw mut registration).cast::<Registrar>();
    // Safety: a register function is the module's contract, and the registrar
    // outlives it.
    let status = unsafe { entry(&HOST, registrar) };
    if let Some(error) = registration.error.take() {
        return Err(format!("{library}: {error}"));
    }
    if status != abi::OK {
        return Err(format!("{library}: register returned status {status}"));
    }
    let mut seen = BTreeSet::new();
    for (name, _) in &registration.services {
        if !seen.insert(name) {
            return Err(format!(
                "{library}: service '{name}' registered twice"
            ));
        }
    }
    Ok(registration)
}
