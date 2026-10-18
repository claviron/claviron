//! Runtime presence and immutable identity from the embedding AppState.

use std::sync::OnceLock;

use claviron_runtime::AppState;
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::Zval;
use ext_php_rs::zend::{ExecuteData, SapiGlobals};

use crate::engine::{ClavironContext, PhpExecution, ProgramModel};
use crate::sapi::ServerContext;

static APP_NAME: OnceLock<Box<str>> = OnceLock::new();
static ENVIRONMENT: OnceLock<Box<str>> = OnceLock::new();
static HOST_PREPARED: OnceLock<()> = OnceLock::new();

pub(crate) fn mark_prepared() {
    let _ = HOST_PREPARED.set(());
}

pub(crate) fn configure_from_state(state: &AppState) {
    APP_NAME.get_or_init(|| state.name().into());
    ENVIRONMENT.get_or_init(|| state.environment().into());
}

/// Registered through the shared SAPI startup path in every PHP execution model.
pub(super) extern "C" fn available(
    _ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        retval.set_bool(true);
    }));
}

/// Prepared host bindings are independent from native engine presence.
pub(super) extern "C" fn configured(
    _ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        retval.set_bool(HOST_PREPARED.get().is_some());
    }));
}

/// Embedding application identity, not the PHP program or routed application name.
pub(super) extern "C" fn app_name(
    _ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = APP_NAME
            .get()
            .ok_or_else(|| {
                "Claviron runtime identity is not configured".to_string()
            })
            .and_then(|name| {
                retval
                    .set_string(name, false)
                    .map_err(|error| error.to_string())
            });
        if let Err(error) = result {
            PhpException::from_message(error).throw();
        }
    }));
}

/// The environment the process runs under: `production`, `local`, or the
/// one its launch named. A worker reports its master's.
pub(super) extern "C" fn environment(
    _ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = ENVIRONMENT
            .get()
            .ok_or_else(|| {
                "Claviron runtime environment is not configured".to_string()
            })
            .and_then(|environment| {
                retval
                    .set_string(environment, false)
                    .map_err(|error| error.to_string())
            });
        if let Err(error) = result {
            PhpException::from_message(error).throw();
        }
    }));
}

/// Named PHP application currently executing, or null outside that model.
pub(super) extern "C" fn application_name(
    _ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let globals = SapiGlobals::get();
        if globals.server_context.is_null() {
            retval.set_null();
            return;
        }
        let context =
            ClavironContext::from_server_context(globals.server_context);
        match &context.execution {
            PhpExecution::Program(program)
                if program.model == ProgramModel::Application =>
            {
                if let Err(error) = retval.set_string(&program.name, false) {
                    PhpException::from_message(error.to_string()).throw();
                }
            }
            _ => retval.set_null(),
        }
    }));
}
