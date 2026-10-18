//! Explicit buffered HTTP messages. These functions never reset SAPI globals
//! or invoke userland across Rust frames; PHP owns its handler/reset/finally loop.

use bytes::Bytes;
use claviron_http::ext::ConnCtx;
use ext_php_rs::args::Arg;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::{ZendHashTable, Zval};
use ext_php_rs::zend::{ExecuteData, SapiGlobals};
use http::{HeaderName, HeaderValue, Request, Response, StatusCode};

use crate::engine::ClavironContext;
use crate::engine::application::{ApplicationInput, INBOX};
use crate::engine::context::RequestEnvironment;
use crate::event::bridge;
use crate::extension::FunctionEntries;
use crate::sapi::ServerContext;

pub(crate) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    entries.push(
        FunctionBuilder::new("claviron_application_receive", receive)
            .returns(DataType::Mixed, false, true)
            .build()
            .map_err(|e| e.to_string())?,
    );
    entries.push(
        FunctionBuilder::new("claviron_application_respond", respond)
            .arg(Arg::new("response", DataType::Array))
            .returns(DataType::Void, false, false)
            .build()
            .map_err(|e| e.to_string())?,
    );
    entries.push(
        FunctionBuilder::new("claviron_application_stream", stream)
            .arg(Arg::new("head", DataType::Array))
            .returns(DataType::Void, false, false)
            .build()
            .map_err(|e| e.to_string())?,
    );
    entries.push(
        FunctionBuilder::new("claviron_application_write", write)
            .arg(Arg::new("chunk", DataType::String))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|e| e.to_string())?,
    );
    entries.push(
        FunctionBuilder::new("claviron_application_end", end)
            .returns(DataType::Void, false, false)
            .build()
            .map_err(|e| e.to_string())?,
    );
    Ok(())
}

fn throw(error: String) {
    PhpException::from_message(error).throw();
}

/// Record what `receive` handed PHP, or that the program is back on its own
/// body.
///
/// This is the state `is_on_http` reads. It sits beside the `activate` /
/// `deactivate` pair on purpose: they mark the same boundary, and keeping them
/// apart is how the fact ended up spelled one way for the interrupt and
/// another way (two flags on the inbox) for everyone else.
fn set_current_work(unit: Option<crate::engine::PhpWorkUnit>) {
    let globals = SapiGlobals::get();
    if globals.server_context.is_null() {
        return;
    }
    let context = ClavironContext::from_server_context(globals.server_context);
    if let Some(program) = context.program_mut() {
        program.current = unit;
    }
}

extern "C" fn receive(
    _ex: &mut ExecuteData,
    value: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        // The guaranteed end of the previous request: the loop has to come back
        // here for work, while a request that failed before responding never
        // reaches `respond`. Anything it left checked out is released now.
        #[cfg(feature = "postgres")]
        crate::functions::postgres::end_request();
        #[cfg(feature = "mysql")]
        crate::functions::mysql::end_request();
        #[cfg(feature = "sqlite")]
        crate::functions::sqlite::end_request();
        // Symmetric with the boundary below: a request that failed before
        // responding never disarmed, and the next one must not inherit its clock.
        crate::engine::interrupt::disarm();
        // And the worker is idle from here until `receive` hands it something, so
        // a drain must not mistake it for work in flight.
        crate::engine::interrupt::deactivate();
        set_current_work(None);

        let result = INBOX.with(|cell| {
            let mut cell = cell.borrow_mut();
            let inbox = cell.as_mut().ok_or(
                "application receive is only available in a PhpApplication worker"
            )?;
            let globals = SapiGlobals::get();
            if globals.server_context.is_null() {
                return Err("application receive has no active Claviron execution"
                    .to_owned());
            }
            // Copied out before `receive` borrows the inbox for the returned job.
            let execution_timeout = inbox.execution_timeout;
            let context =
                ClavironContext::from_server_context(globals.server_context);
            let mut subscription =
                context.event_subscription.lock().map_err(|_| {
                    "PHP execution event subscription lock is poisoned".to_owned()
                })?;
            let input = match subscription.as_mut() {
                Some(subscription) => {
                    let bridge = bridge()?;
                    inbox.receive(Some((bridge.as_ref(), subscription)))?
                }
                None => inbox.receive(None)?
            };
            // A persistent application keeps one PHP request scope alive while it
            // serves many application requests. Apply peer cache invalidations at
            // this explicit work boundary as well as at ordinary SAPI startup, so
            // the application never handles its next message with stale cache.
            if input.is_some() {
                crate::intercept::pre_execute_work_hooks();
            }
            match input {
                Some(ApplicationInput::Request(request)) => {
                    // The program outlives every request it serves, so both the
                    // budget and the activity marker start here rather than around
                    // the script.
                    crate::engine::interrupt::activate();
                    if let Some(budget) = execution_timeout {
                        crate::engine::interrupt::arm(budget);
                    }
                    set_current_work(Some(crate::engine::PhpWorkUnit::Request));

                    request_value(request).map(Some)
                }
                Some(ApplicationInput::Event(event)) => {
                    // An event is a unit of work like a request: it starts here and
                    // ends at the next `receive`, which is where the disarm above
                    // runs. It used to arm nothing, so a handler that never
                    // returned ran unbounded while a request on the same
                    // application was bounded — the same worker, two different
                    // rules.
                    crate::engine::interrupt::activate();
                    if let Some(budget) = execution_timeout {
                        crate::engine::interrupt::arm(budget);
                    }
                    set_current_work(Some(crate::engine::PhpWorkUnit::Event));

                    let mut event_value = Zval::new();
                    event.set_zval(&mut event_value)?;
                    Ok(Some(event_value))
                }
                None => Ok(None)
            }
        });
        match result {
            Ok(Some(request)) => {
                #[cfg(feature = "postgres")]
                crate::functions::postgres::begin_request();
                #[cfg(feature = "mysql")]
                crate::functions::mysql::begin_request();
                #[cfg(feature = "sqlite")]
                crate::functions::sqlite::begin_request();

                *value = request;
            }
            Ok(None) => value.set_null(),
            Err(error) => {
                value.set_null();
                throw(error);
            }
        }
    }));
}

extern "C" fn respond(
    ex: &mut ExecuteData,
    _value: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let mut response = Arg::new("response", DataType::Array);
        if ex.parser().arg(&mut response).parse().is_err() {
            return;
        }
        let result = INBOX.with(|cell| {
            let mut cell = cell.borrow_mut();
            let inbox = cell.as_mut().ok_or(
                "application respond is only available in a PhpApplication worker"
            )?;
            let value = response
                .zval()
                .and_then(|v| v.array())
                .ok_or("response must be an array")?;
            inbox.respond(response_value(value, inbox.max_body_size())?)
        });
        // The response is out; whatever the request was holding is no longer its
        // business, and the next one must not inherit it.
        #[cfg(feature = "postgres")]
        crate::functions::postgres::end_request();
        #[cfg(feature = "mysql")]
        crate::functions::mysql::end_request();
        #[cfg(feature = "sqlite")]
        crate::functions::sqlite::end_request();
        crate::engine::interrupt::disarm();
        crate::engine::interrupt::deactivate();
        set_current_work(None);

        if let Err(error) = result {
            throw(error);
        }
    }));
}

/// `claviron_application_stream(array $head)`: send the reply's head — the
/// same `status` and `headers` as `respond`, no `body` — and write the body
/// piece by piece with `claviron_application_write`. The request is still
/// running: its execution budget, which bounds the time until the head, is
/// disarmed here, and the request ends at `claviron_application_end` (or the
/// next receive).
extern "C" fn stream(
    ex: &mut ExecuteData,
    _value: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let mut head = Arg::new("head", DataType::Array);
        if ex.parser().arg(&mut head).parse().is_err() {
            return;
        }
        let result = INBOX.with(|cell| {
            let mut cell = cell.borrow_mut();
            let inbox = cell.as_mut().ok_or(
                "application stream is only available in a PhpApplication worker"
            )?;
            let value = head
                .zval()
                .and_then(|v| v.array())
                .ok_or("head must be an array")?;
            let (parts, ()) = response_head(value)?.into_parts();
            inbox.respond_head(parts)
        });
        // The head is out: from here a request is bounded by gaps, not by a
        // total, so a stream is not cut by the budget that bounded its start.
        crate::engine::interrupt::disarm();
        if let Err(error) = result {
            throw(error);
        }
    }));
}

/// `claviron_application_write(string $chunk): bool` — one piece of a
/// streamed reply. `false` means the client is gone (or stopped reading past
/// `responses.write_timeout`, or the server is stopping): stop producing.
extern "C" fn write(
    ex: &mut ExecuteData,
    value: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let mut chunk = Arg::new("chunk", DataType::String);
        if ex.parser().arg(&mut chunk).parse().is_err() {
            return;
        }
        let result = INBOX.with(|cell| {
            let mut cell = cell.borrow_mut();
            let inbox = cell.as_mut().ok_or(
                "application write is only available in a PhpApplication worker"
            )?;
            let bytes = chunk
                .zval()
                .and_then(|v| v.binary_slice::<u8>())
                .ok_or("chunk must be a string")?;
            inbox.write(bytes)
        });
        match result {
            Ok(written) => value.set_bool(written),
            Err(error) => {
                value.set_bool(false);
                throw(error);
            }
        }
    }));
}

/// `claviron_application_end()`: the streamed reply is complete, and so is
/// the request — what `respond` does after sending a whole reply.
extern "C" fn end(
    _ex: &mut ExecuteData,
    _value: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        INBOX.with(|cell| {
            if let Some(inbox) = cell.borrow_mut().as_mut() {
                inbox.end();
            }
        });
        #[cfg(feature = "postgres")]
        crate::functions::postgres::end_request();
        #[cfg(feature = "mysql")]
        crate::functions::mysql::end_request();
        #[cfg(feature = "sqlite")]
        crate::functions::sqlite::end_request();
        crate::engine::interrupt::disarm();
        crate::engine::interrupt::deactivate();
        set_current_work(None);
    }));
}

fn binary(bytes: &[u8]) -> Zval {
    let mut value = Zval::new();
    value.set_binary(bytes.to_vec());
    value
}

fn request_value(request: &Request<Bytes>) -> Result<Zval, String> {
    let result = (|| -> ext_php_rs::error::Result<Zval> {
        let mut value = ZendHashTable::new();
        value.insert("method", request.method().as_str())?;
        value.insert("uri", request.uri().to_string())?;
        value.insert("protocol", format!("{:?}", request.version()))?;
        value.insert("body", binary(request.body()))?;
        let mut headers = ZendHashTable::new();
        for name in request.headers().keys() {
            let values: Vec<Zval> = request
                .headers()
                .get_all(name)
                .iter()
                .map(|v| binary(v.as_bytes()))
                .collect();
            headers.insert(name.as_str(), values)?;
        }
        value.insert("headers", headers)?;
        let mut server = ZendHashTable::new();
        // Site values belong to this message, not to the persistent program's
        // boot globals. ConnCtx and the request scheme remain authoritative.
        if let Some(environment) =
            request.extensions().get::<RequestEnvironment>()
        {
            for (key, value) in environment.0.iter() {
                server.insert(key.as_str(), value.as_str())?;
            }
        }
        if let Some(conn) =
            request.extensions().get::<std::sync::Arc<ConnCtx>>()
        {
            server.insert(
                "REMOTE_ADDR",
                conn.real_ip.unwrap_or(conn.remote.ip()).to_string(),
            )?;
            server.insert("REMOTE_PORT", conn.remote.port().to_string())?;
            server.insert("SERVER_ADDR", conn.local_ip.to_string())?;
            server.insert("SERVER_PORT", conn.local_port.to_string())?;
        }
        if let Some(scheme) =
            claviron_http::ext::request_scheme(request.extensions())
        {
            server.insert("REQUEST_SCHEME", scheme.as_str())?;
            server.insert(
                "HTTPS",
                if scheme == http::uri::Scheme::HTTPS { "on" } else { "off" },
            )?;
        }
        value.insert("server", server)?;
        let mut result = Zval::new();
        result.set_hashtable(value);
        Ok(result)
    })();
    result.map_err(|e| e.to_string())
}

fn response_value(
    value: &ZendHashTable,
    limit: Option<usize>,
) -> Result<Response<Bytes>, String> {
    let body = value
        .get("body")
        .and_then(|v| v.binary_slice::<u8>())
        .ok_or("response.body must be a string")?;
    if limit.is_some_and(|limit| body.len() > limit) {
        return Err("application response body exceeds the route's responses.max_body_size".into());
    }
    let body = Bytes::copy_from_slice(body);
    Ok(response_head(value)?.map(|()| body))
}

/// A reply's `status` and `headers`, shared by `respond` and `stream`.
fn response_head(value: &ZendHashTable) -> Result<Response<()>, String> {
    let code = value
        .get("status")
        .and_then(|v| v.long())
        .ok_or("response.status must be an integer")?;
    let status = u16::try_from(code)
        .ok()
        .filter(|n| (200..=599).contains(n))
        .and_then(|n| StatusCode::from_u16(n).ok())
        .ok_or("response.status must be between 200 and 599")?;
    let mut response = Response::new(());
    *response.status_mut() = status;
    let headers = value.get("headers").and_then(|v| v.array()).ok_or(
        "response.headers must be an array of header-name => list<string>",
    )?;
    for (key, values) in headers {
        let name = HeaderName::from_bytes(key.to_string().as_bytes())
            .map_err(|e| e.to_string())?;
        let values = values
            .array()
            .ok_or("each response header must be a list of strings")?;
        for (_, value) in values {
            let bytes = value
                .binary_slice::<u8>()
                .ok_or("response header value must be a string")?;
            response.headers_mut().append(
                name.clone(),
                HeaderValue::from_bytes(bytes).map_err(|e| e.to_string())?,
            );
        }
    }
    Ok(response)
}
