//! `claviron_log_channel()` and `claviron_log()`: PHP writing records to a
//! log channel `logging.yaml` declares.
//!
//! The record goes into this process's lane of the log ring and the call
//! returns; the fleet's writer formats and writes it. Nothing here opens,
//! locks or stats a file, which is what a Monolog file handler does on every
//! request that logs in per-request mode.
//!
//! ```php
//! $channel = claviron_log_channel('laravel');           // null if undeclared
//! claviron_log($channel, 'warning', 'order failed', ['order' => 7]);
//! ```
//!
//! The record's target is where it came from: `$target` when given, otherwise
//! the first PHP frame above the call that is not logging machinery —
//! `Namespace\Class::method`, read from the executor's frames without building
//! a backtrace array.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use arc_swap::ArcSwapOption;
use claviron_logging::pipeline::{
    ChannelId, Level, LogChannels, Record, Value,
};
use ext_php_rs::args::Arg;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::{ArrayKey, ZendHashTable, Zval};
use ext_php_rs::zend::ExecuteData;

use crate::extension::FunctionEntries;

/// Frames that are the logging machinery itself, never a record's source.
const MACHINERY: [&str; 4] =
    ["Monolog\\", "Illuminate\\Log\\", "Psr\\Log\\", "Claviron\\Monolog\\"];

static CHANNELS: OnceLock<ArcSwapOption<LogChannels>> = OnceLock::new();

/// Extra prefixes a channel's registration asked to skip, by channel.
static SKIP: OnceLock<Mutex<HashMap<ChannelId, Vec<String>>>> = OnceLock::new();

fn channels_slot() -> &'static ArcSwapOption<LogChannels> {
    CHANNELS.get_or_init(ArcSwapOption::const_empty)
}

pub(crate) fn configure(channels: Option<Arc<LogChannels>>) {
    channels_slot().store(channels);
}

pub(crate) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    entries.push(
        FunctionBuilder::new("claviron_log_channel", channel_handler)
            .arg(Arg::new("name", DataType::String))
            .not_required()
            .arg(Arg::new("skip", DataType::Array))
            .returns(DataType::Long, false, true)
            .build()
            .map_err(|error| format!("claviron_log_channel: {error}"))?,
    );
    entries.push(
        FunctionBuilder::new("claviron_log", log_handler)
            .arg(Arg::new("channel", DataType::Long))
            .arg(Arg::new("level", DataType::String))
            .arg(Arg::new("message", DataType::String))
            .not_required()
            .arg(Arg::new("context", DataType::Array))
            .arg(Arg::new("target", DataType::String))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("claviron_log: {error}"))?,
    );
    Ok(())
}

/// The id of a declared channel, or null: undeclared, or no pipeline.
extern "C" fn channel_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let mut name = Arg::new("name", DataType::String);
        let mut skip = Arg::new("skip", DataType::Array);
        if ex
            .parser()
            .arg(&mut name)
            .not_required()
            .arg(&mut skip)
            .parse()
            .is_err()
        {
            retval.set_null();
            return;
        }
        let Ok(name) = name.consume::<String>() else {
            retval.set_null();
            return;
        };
        let Some(channel) = channels_slot()
            .load_full()
            .and_then(|channels| channels.channel(&name))
        else {
            retval.set_null();
            return;
        };
        if let Some(prefixes) = skip.zval().and_then(|zval| zval.array()) {
            let prefixes: Vec<String> =
                prefixes.values().filter_map(|value| value.string()).collect();
            SKIP.get_or_init(Default::default)
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert(channel, prefixes);
        }
        retval.set_long(i64::from(channel.raw()));
    }));
}

/// Publish one record; false when it could not be (bad arguments, no
/// pipeline, a ring write failure).
extern "C" fn log_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        retval.set_bool(log(ex).is_some());
    }));
}

fn log(ex: &mut ExecuteData) -> Option<()> {
    // Taken before the argument parser borrows this frame; the frames above
    // live for the whole call.
    let above = ex.prev_execute_data.cast_const();
    let mut channel = Arg::new("channel", DataType::Long);
    let mut level = Arg::new("level", DataType::String);
    let mut message = Arg::new("message", DataType::String);
    let mut context = Arg::new("context", DataType::Array);
    let mut target = Arg::new("target", DataType::String);
    ex.parser()
        .arg(&mut channel)
        .arg(&mut level)
        .arg(&mut message)
        .not_required()
        .arg(&mut context)
        .arg(&mut target)
        .parse()
        .ok()?;
    let channel = ChannelId::from_raw(
        u16::try_from(channel.consume::<i64>().ok()?).ok()?,
    );
    let channels = channels_slot().load_full()?;
    let level_name = level.consume::<String>().ok()?;
    let message = message.consume::<String>().ok()?;
    let target = match target.zval().and_then(|zval| zval.string()) {
        Some(target) => target,
        // SAFETY: see `above`.
        None => caller(unsafe { above.as_ref() }, channel),
    };

    // Owned first, then borrowed into the record: the values live on this
    // stack until the ring has a copy.
    let mut owned: Vec<(String, OwnedField)> = Vec::new();
    if let Some(context) = context.zval().and_then(|zval| zval.array()) {
        flatten(context, &mut owned);
    }
    let (level, lossy) = map_level(&level_name);
    if lossy {
        owned.push((
            "psr_level".to_owned(),
            OwnedField::Str(level_name.clone()),
        ));
    }
    let fields: Vec<(&str, Value<'_>)> = owned
        .iter()
        .map(|(key, value)| (key.as_str(), value.borrow()))
        .collect();
    let record =
        Record { level, target: &target, message: &message, fields: &fields };
    channels.emit(channel, &record).ok()
}

/// PSR-3's eight levels onto the pipeline's five. The ones that merge keep
/// their name in `psr_level`.
fn map_level(name: &str) -> (Level, bool) {
    match name.to_ascii_lowercase().as_str() {
        "debug" => (Level::Debug, false),
        "info" => (Level::Info, false),
        "notice" => (Level::Info, true),
        "warning" => (Level::Warn, false),
        "error" => (Level::Error, false),
        "critical" | "alert" | "emergency" => (Level::Error, true),
        _ => (Level::Info, true),
    }
}

enum OwnedField {
    Str(String),
    I64(i64),
    F64(f64),
    Bool(bool),
}

impl OwnedField {
    fn borrow(&self) -> Value<'_> {
        match self {
            Self::Str(text) => Value::Str(text),
            Self::I64(number) => Value::I64(*number),
            Self::F64(number) => Value::F64(*number),
            Self::Bool(flag) => Value::Bool(*flag),
        }
    }
}

/// One field per context key: scalars typed, anything else as text. A nested
/// array keeps its keys with a dot (`order.id`).
fn flatten(
    table: &ZendHashTable,
    out: &mut Vec<(String, OwnedField)>,
) {
    flatten_into(table, "", out);
}

fn flatten_into(
    table: &ZendHashTable,
    prefix: &str,
    out: &mut Vec<(String, OwnedField)>,
) {
    for (key, value) in table.iter() {
        let key = match key {
            ArrayKey::String(key) => key,
            other => other.to_string(),
        };
        let key =
            if prefix.is_empty() { key } else { format!("{prefix}.{key}") };
        if let Some(nested) = value.array() {
            flatten_into(nested, &key, out);
            continue;
        }
        let field = if let Some(flag) = value.bool() {
            OwnedField::Bool(flag)
        } else if let Some(number) = value.long() {
            OwnedField::I64(number)
        } else if let Some(number) = value.double() {
            OwnedField::F64(number)
        } else if let Some(text) = value.string() {
            OwnedField::Str(text)
        } else if value.is_null() {
            OwnedField::Str(String::new())
        } else if let Some(object) = value.object() {
            OwnedField::Str(format!(
                "[object {}]",
                object.get_class_name().unwrap_or_else(|_| "?".to_owned())
            ))
        } else {
            continue;
        };
        out.push((key, field));
    }
}

/// `Namespace\Class::method` (or `function`) of the first frame above this
/// call that is not logging machinery or a prefix the channel asked to skip.
fn caller(
    above: Option<&ExecuteData>,
    channel: ChannelId,
) -> String {
    let skip = SKIP
        .get()
        .and_then(|skip| {
            skip.lock().ok().and_then(|skip| skip.get(&channel).cloned())
        })
        .unwrap_or_default();
    let mut frame = above;
    while let Some(current) = frame {
        if let Some((class, function)) = frame_name(current) {
            let machinery = class.as_deref().is_some_and(|class| {
                MACHINERY.iter().any(|prefix| class.starts_with(prefix))
                    || skip
                        .iter()
                        .any(|prefix| class.starts_with(prefix.as_str()))
            });
            if !machinery {
                return match class {
                    Some(class) => format!("{class}::{function}"),
                    None => function,
                };
            }
        }
        frame = current.previous();
    }
    String::new()
}

/// The class and function a frame runs, when it runs a named function.
fn frame_name(frame: &ExecuteData) -> Option<(Option<String>, String)> {
    let function = frame.function()?;
    // SAFETY: `common` is the prefix every member of the zend_function union
    // shares; its name and scope are valid for as long as the frame is.
    let (name, scope) = unsafe {
        (function.common.function_name.as_ref(), function.common.scope.as_ref())
    };
    let name = name?.as_str().ok()?.to_owned();
    let class = scope.and_then(|scope| scope.name()).map(ToOwned::to_owned);
    Some((class, name))
}
