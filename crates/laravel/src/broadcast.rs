//! Native Laravel broadcast driver and its worker-local publication context.

use std::sync::OnceLock;

use arc_swap::ArcSwapOption;
use claviron_broadcast::BroadcastService;
use claviron_php_embed::extension::{
    DataType, ExecuteData, FunctionBuilder, FunctionEntries, PhpEmbedExtension,
    Zval,
};
use claviron_pusher::TriggerEvent;
use claviron_runtime::AppState;
use ext_php_rs::args::Arg;
use ext_php_rs::exception::PhpException;

mod provider;

pub use provider::LaravelBroadcastProvider;

fn context() -> &'static ArcSwapOption<BroadcastService> {
    static CONTEXT: OnceLock<ArcSwapOption<BroadcastService>> = OnceLock::new();
    CONTEXT.get_or_init(ArcSwapOption::const_empty)
}

/// Native PHP extension for Laravel's broadcast driver only.
pub struct BroadcastExtension;

impl PhpEmbedExtension for BroadcastExtension {
    fn name(&self) -> &'static str {
        "claviron-laravel-broadcast"
    }

    fn configure(
        &self,
        state: &AppState,
    ) -> Result<(), String> {
        let broadcast = state
            .registry_ref()
            .resolve::<BroadcastService>()
            .ok_or_else(|| {
                "BroadcastService must be registered before claviron-laravel-broadcast"
                    .to_owned()
            })?;
        context().store(Some(broadcast));
        Ok(())
    }

    fn append_functions(
        &self,
        entries: &mut FunctionEntries,
    ) -> Result<(), String> {
        entries.push(
            FunctionBuilder::new(
                "claviron_laravel_broadcast",
                broadcast_handler,
            )
            .arg(Arg::new("key", DataType::String))
            .arg(Arg::new("channels", DataType::Array))
            .arg(Arg::new("event", DataType::String))
            .arg(Arg::new("data", DataType::String))
            .not_required()
            .arg(
                Arg::new("socket_id", DataType::String)
                    .allow_null()
                    .default("null"),
            )
            .returns(DataType::Void, false, false)
            .build()
            .map_err(|error| format!("claviron_laravel_broadcast: {error}"))?,
        );
        Ok(())
    }
}

extern "C" fn broadcast_handler(
    ex: &mut ExecuteData,
    _retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        if let Err(error) = broadcast(ex) {
            PhpException::from_message(format!(
                "Claviron Laravel broadcast failed: {error}"
            ))
            .throw();
        }
    }));
}

fn broadcast(ex: &mut ExecuteData) -> Result<(), String> {
    let mut key = Arg::new("key", DataType::String);
    let mut channels = Arg::new("channels", DataType::Array);
    let mut event = Arg::new("event", DataType::String);
    let mut data = Arg::new("data", DataType::String);
    let mut socket_id = Arg::new("socket_id", DataType::String).allow_null();
    ex.parser()
        .arg(&mut key)
        .arg(&mut channels)
        .arg(&mut event)
        .arg(&mut data)
        .not_required()
        .arg(&mut socket_id)
        .parse()
        .map_err(|error| error.to_string())?;

    let key = key
        .consume::<String>()
        .map_err(|_| "key must be a string".to_owned())?;
    let channels = channels
        .zval()
        .and_then(|value| value.array())
        .ok_or_else(|| "channels must be an array".to_owned())?
        .values()
        .map(|channel| {
            channel
                .binary_slice::<u8>()
                .and_then(|value| std::str::from_utf8(value).ok())
                .map(str::to_owned)
                .ok_or_else(|| {
                    "every channel must be a UTF-8 string".to_owned()
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let event = event
        .consume::<String>()
        .map_err(|_| "event must be a string".to_owned())?;
    let data = data
        .consume::<String>()
        .map_err(|_| "data must be a string".to_owned())?;
    let socket_id =
        match socket_id.zval() {
            None => None,
            Some(value) if value.is_null() => None,
            Some(_) => Some(socket_id.consume::<String>().map_err(|_| {
                "socket_id must be a string or null".to_owned()
            })?),
        };
    let publication = TriggerEvent {
        name: event,
        data: serde_json::Value::String(data),
        channels,
        channel: None,
        socket_id,
        info: None,
    }
    .validate()
    .map_err(|error| error.to_string())?;
    let broadcast = context().load_full().ok_or_else(|| {
        "Claviron Laravel broadcast extension is not configured".to_owned()
    })?;
    broadcast
        .publish_by_key(&key, &publication)
        .map(|_| ())
        .map_err(|error| error.to_string())
}
