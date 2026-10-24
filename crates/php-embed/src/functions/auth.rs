//! Native identity projection, scoped to the actual HTTP request.

use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use claviron_auth::orbit::{AuthenticatedSession, AuthenticationResolved};
use claviron_runtime::AppState;
use ext_php_rs::boxed::ZBox;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::{ZendHashTable, Zval};
use ext_php_rs::zend::{ExecuteData, SapiGlobals};
use http::{Extensions, HeaderMap};
use serde_json::Value;
use tokio::runtime::Handle;

use crate::auth::{ModelAttributes, PhpRequestAuthenticator};
use crate::engine::application::INBOX;
use crate::engine::{ClavironContext, PhpExecution, PhpWorkUnit};
use crate::extension::FunctionEntries;
use crate::sapi::ServerContext;

struct Bridge {
    authenticator: Arc<dyn PhpRequestAuthenticator>,
    runtime: Handle,
}

fn bridge() -> &'static ArcSwapOption<Bridge> {
    static BRIDGE: OnceLock<ArcSwapOption<Bridge>> = OnceLock::new();
    BRIDGE.get_or_init(ArcSwapOption::const_empty)
}

pub(crate) fn configure_from_state(state: &AppState) {
    bridge().store(
        state.registry_ref().resolve_dyn::<dyn PhpRequestAuthenticator>().map(
            |authenticator| {
                Arc::new(Bridge { authenticator, runtime: Handle::current() })
            },
        ),
    );
}

pub(super) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    entries.push(
        FunctionBuilder::new("claviron_auth", handler)
            .returns(DataType::Array, true, false)
            .build()
            .map_err(|error| error.to_string())?,
    );
    Ok(())
}

/// Owned by Parts/Job, never by a reusable ZTS thread or a PHP application.
#[derive(Clone)]
struct CachedAuth(Option<Arc<AuthenticatedSession>>);

fn resolve(
    headers: &HeaderMap,
    extensions: &mut Extensions,
) -> Result<Option<Arc<AuthenticatedSession>>, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs();
    if extensions.get::<CachedAuth>().is_none() {
        // A preceding Rust handler/middleware may already have resolved this
        // exact request. Attaching that typed result avoids another user query.
        if let Some(authenticated) = extensions.get::<AuthenticatedSession>() {
            extensions
                .insert(CachedAuth(Some(Arc::new(authenticated.clone()))));
        } else if extensions.get::<AuthenticationResolved>().is_some() {
            extensions.insert(CachedAuth(None));
        } else {
            let bridge = bridge().load_full().ok_or_else(|| {
                "claviron_auth requires a registered PhpRequestAuthenticator"
                    .to_owned()
            })?;
            if Handle::try_current().is_ok() {
                return Err("synchronous PHP authentication must run outside a Tokio runtime thread".into());
            }
            let session = bridge
                .runtime
                .block_on(bridge.authenticator.authenticate(headers, now))?;
            if let Some(session) = &session {
                extensions.insert(session.clone());
            }
            extensions.insert(CachedAuth(session.map(Arc::new)));
        }
    }
    let session =
        extensions.get::<CachedAuth>().and_then(|cached| cached.0.clone());
    // Host policy may have awaited IO; admission uses current time, not
    // the timestamp from before that wait. Logout/expiry applies on cache hits too.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs();
    Ok(session.filter(|session| session.session.check(now).is_ok()))
}

pub(crate) fn current() -> Result<Option<Arc<AuthenticatedSession>>, String> {
    let globals = SapiGlobals::get();
    if globals.server_context.is_null() {
        return Ok(None);
    }
    let context = ClavironContext::from_server_context(globals.server_context);
    match &mut context.execution {
        PhpExecution::Request(request) => {
            resolve(&request.parts.headers, &mut request.parts.extensions)
        }
        PhpExecution::Program(program)
            if program.current == Some(PhpWorkUnit::Request) =>
        {
            INBOX.with(|cell| {
                let mut inbox = cell.borrow_mut();
                let job = inbox
                    .as_mut()
                    .and_then(|inbox| inbox.current.as_mut())
                    .ok_or_else(|| {
                        "current PHP HTTP request is missing".to_owned()
                    })?;
                let headers =
                    if job.request.extensions().get::<CachedAuth>().is_none() {
                        job.request.headers().clone()
                    } else {
                        HeaderMap::new()
                    };
                resolve(&headers, job.request.extensions_mut())
            })
        }
        _ => Ok(None),
    }
}

extern "C" fn handler(
    _ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| -> Result<(), String> {
            let Some(auth) = current()? else {
                retval.set_null();
                return Ok(());
            };
            let principal = auth.session.principal();
            let mut value = ZendHashTable::new();
            value
                .insert("subject", principal.subject())
                .map_err(|error| error.to_string())?;
            let session_id = principal
                .session_id()
                .map(|id| uuid::Uuid::from_bytes(id.0).to_string());
            value
                .insert("session_id", session_id)
                .map_err(|error| error.to_string())?;
            let expires_at = i64::try_from(principal.claims().expires_at)
                .map_err(|error| error.to_string())?;
            value
                .insert("expires_at", expires_at)
                .map_err(|error| error.to_string())?;
            let bridge = bridge().load_full().ok_or_else(|| {
                "PHP auth projection is not configured".to_owned()
            })?;
            let attributes =
                bridge.authenticator.model_attributes(auth.user.as_ref())?;
            value
                .insert("attributes", encode_attributes(attributes)?)
                .map_err(|error| error.to_string())?;
            retval.set_hashtable(value);
            Ok(())
        })();
        if let Err(error) = result {
            PhpException::from_message(error).throw();
        }
    }));
}

fn encode_attributes(
    attributes: &ModelAttributes
) -> Result<ZBox<ZendHashTable>, String> {
    let mut output = ZendHashTable::new();
    for (name, attribute) in attributes {
        let mut value = Zval::new();
        match attribute {
            Value::Null => value.set_null(),
            Value::Bool(boolean) => value.set_bool(*boolean),
            Value::Number(number) => {
                if let Some(integer) = number.as_i64() {
                    value.set_long(integer);
                } else if number.is_f64() {
                    value.set_double(number.as_f64().ok_or_else(|| {
                        format!("invalid model attribute {name}")
                    })?);
                } else {
                    return Err(format!(
                        "model attribute {name} exceeds PHP integer range; supply a string"
                    ));
                }
            }
            Value::String(string) => value
                .set_string(string, false)
                .map_err(|error| error.to_string())?,
            _ => {
                return Err(format!(
                    "model attribute {name} must be a raw scalar; JSON columns must be strings"
                ));
            }
        }
        output
            .insert(name.as_str(), value)
            .map_err(|error| error.to_string())?;
    }
    Ok(output)
}
