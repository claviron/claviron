//! What a library's register function says exists, and the host table it says
//! it with. Every call is contained: nothing unwinds into native code.

use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::abi::{self, Host, Lifecycle, Registrar, Service, Slice};

/// A library's registrations, filled while its register function runs.
#[derive(Default)]
pub(crate) struct Registration {
    pub lifecycle: Option<Lifecycle>,
    pub services: Vec<(String, Service)>,
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
    log,
    cancelled: crate::service::cancelled,
    cancel_wait: crate::service::cancel_wait,
    cancel_notify: crate::service::cancel_notify,
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
