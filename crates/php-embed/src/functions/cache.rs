//! PHP-facing Claviron cache primitives.
//!
//! `claviron_cache_store()` resolves the configured default or named
//! [`CacheRepository`] once and returns an opaque native handle. The operation
//! functions accept that handle, so PHP framework integrations can keep their
//! selected repository without passing a store name on every call.

use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use claviron_cache::{
    ArchivedCacheJson, CacheJson, CacheProvider, CacheRepository, CacheValue,
    CacheValueRef,
};
use ext_php_rs::args::Arg;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::class::RegisteredClass;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::{ClassFlags, DataType};
use ext_php_rs::php_class;
use ext_php_rs::types::{ArrayKey, ZendClassObject, ZendHashTable, Zval};
use ext_php_rs::zend::ExecuteData;
use tokio::runtime::Handle;
use tracing::debug;

use crate::extension::FunctionEntries;

struct PhpCacheBridge {
    provider: Arc<CacheProvider>,
    runtime: Handle,
}

/// Opaque PHP object holding one already-resolved cache repository.
///
/// PHP cannot construct, extend, clone, or serialize this object. The only
/// creation path is [`store_handler`], analogous to PHP's native handle types.
#[php_class]
#[php(name = "Claviron\\Runtime\\CacheHandle")]
#[php(flags = ClassFlags::from_bits_retain(
    ClassFlags::Final.bits() | ClassFlags::NoDynamicProperties.bits()
))]
pub(crate) struct PhpCacheHandle {
    repository: Arc<CacheRepository>,
}

impl PhpCacheBridge {
    fn repository(
        &self,
        store: Option<&str>,
    ) -> claviron_cache::Result<Arc<CacheRepository>> {
        match store {
            Some("") => Err(claviron_cache::Error::Rejected(
                "cache store name must not be empty".to_string(),
            )),
            Some(name) => self.provider.store(name),
            None => self.provider.default_store(),
        }
    }

    fn block_on<F, T>(
        &self,
        future: F,
    ) -> Result<T, String>
    where
        F: Future<Output = claviron_cache::Result<T>>,
    {
        if Handle::try_current().is_ok() {
            return Err(
                "synchronous PHP cache access must run outside a Tokio runtime thread".to_string()
            );
        }
        self.runtime.block_on(future).map_err(|error| error.to_string())
    }
}

static PHP_CACHE: OnceLock<ArcSwapOption<PhpCacheBridge>> = OnceLock::new();

fn bridge_slot() -> &'static ArcSwapOption<PhpCacheBridge> {
    PHP_CACHE.get_or_init(ArcSwapOption::const_empty)
}

pub(crate) fn configure(
    provider: Option<Arc<CacheProvider>>
) -> claviron_runtime::registry::Result<()> {
    let bridge = provider
        .map(|provider| {
            Handle::try_current()
                .map(|runtime| Arc::new(PhpCacheBridge { provider, runtime }))
                .map_err(|error| {
                    claviron_runtime::registry::Error::msg(format!(
                        "PHP cache runtime handle is unavailable: {error}"
                    ))
                })
        })
        .transpose()?;

    let enabled = bridge.is_some();
    bridge_slot().store(bridge);
    debug!(enabled, "PHP Claviron cache host functions configured");
    Ok(())
}

fn bridge() -> Result<Arc<PhpCacheBridge>, String> {
    bridge_slot()
        .load_full()
        .ok_or_else(|| "Claviron cache is not configured".to_string())
}

pub(crate) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    let functions = [
        FunctionBuilder::new("claviron_cache_store", store_handler)
            .not_required()
            .arg(
                Arg::new("name", DataType::String).allow_null().default("null"),
            )
            .returns(DataType::object(PhpCacheHandle::CLASS_NAME), false, false)
            .build()
            .map_err(|error| format!("claviron_cache_store: {error}"))?,
        FunctionBuilder::new("claviron_cache_get", get_handler)
            .arg(Arg::new(
                "cache",
                DataType::object(PhpCacheHandle::CLASS_NAME),
            ))
            .arg(Arg::new("key", DataType::String))
            .returns(DataType::Mixed, false, true)
            .build()
            .map_err(|error| format!("claviron_cache_get: {error}"))?,
        FunctionBuilder::new("claviron_cache_put", put_handler)
            .arg(Arg::new(
                "cache",
                DataType::object(PhpCacheHandle::CLASS_NAME),
            ))
            .arg(Arg::new("key", DataType::String))
            .arg(Arg::new("value", DataType::Mixed))
            .not_required()
            .arg(
                Arg::new("seconds", DataType::Long)
                    .allow_null()
                    .default("null"),
            )
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("claviron_cache_put: {error}"))?,
        FunctionBuilder::new("claviron_cache_forget", forget_handler)
            .arg(Arg::new(
                "cache",
                DataType::object(PhpCacheHandle::CLASS_NAME),
            ))
            .arg(Arg::new("key", DataType::String))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("claviron_cache_forget: {error}"))?,
        FunctionBuilder::new("claviron_cache_clear", clear_handler)
            .arg(Arg::new(
                "cache",
                DataType::object(PhpCacheHandle::CLASS_NAME),
            ))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("claviron_cache_clear: {error}"))?,
    ];

    entries.extend(functions);
    debug!(
        "prepared PHP functions for cache: claviron_cache_store/claviron_cache_get/\
         claviron_cache_put/claviron_cache_forget/claviron_cache_clear"
    );
    Ok(())
}

pub(crate) extern "C" fn store_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let Some(name) = parse_store_name(ex) else {
            retval.set_null();
            return;
        };
        let Ok(bridge) =
            bridge().inspect_err(|error| throw_cache_exception(error))
        else {
            retval.set_null();
            return;
        };
        let Ok(repository) = bridge
            .repository(name.as_deref())
            .inspect_err(|error| throw_cache_exception(error))
        else {
            retval.set_null();
            return;
        };

        if ZendClassObject::new(PhpCacheHandle { repository })
            .set_zval(retval, false)
            .is_err()
        {
            retval.set_null();
            throw_cache_exception(
                "failed to create Claviron\\Runtime\\CacheHandle",
            );
        }
    }));
}

pub(crate) extern "C" fn get_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let Some((repository, key)) = parse_handle_key(ex) else {
            retval.set_null();
            return;
        };
        let Ok(bridge) =
            bridge().inspect_err(|error| throw_cache_exception(error))
        else {
            return;
        };

        match bridge.block_on(repository.get(&key)) {
            Ok(Some(bytes)) => match CacheValue::view(&bytes) {
                Ok(value) => set_retval_from_cache_value(retval, value),
                Err(error) => throw_cache_exception(error),
            },
            Ok(None) => retval.set_null(),
            Err(error) => throw_cache_exception(error),
        }
    }));
}

pub(crate) extern "C" fn put_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let Ok((repository, key, value, seconds)) =
            parse_put(ex).inspect_err(|error| throw_cache_exception(error))
        else {
            retval.set_bool(false);
            return;
        };
        let Ok(bridge) =
            bridge().inspect_err(|error| throw_cache_exception(error))
        else {
            retval.set_bool(false);
            return;
        };
        let result = if let Some(seconds) = seconds {
            if seconds <= 0 {
                bridge.block_on(repository.forget(&key))
            } else {
                bridge.block_on(repository.put_value(
                    &key,
                    value,
                    Some(Duration::from_secs(seconds as u64)),
                ))
            }
        } else {
            bridge.block_on(repository.put_value(&key, value, None))
        };

        set_bool_result(retval, result);
    }));
}

pub(crate) extern "C" fn forget_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let Some((repository, key)) = parse_handle_key(ex) else {
            retval.set_bool(false);
            return;
        };
        let Ok(bridge) =
            bridge().inspect_err(|error| throw_cache_exception(error))
        else {
            retval.set_bool(false);
            return;
        };
        set_bool_result(retval, bridge.block_on(repository.forget(&key)));
    }));
}

pub(crate) extern "C" fn clear_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let Some(repository) = parse_handle(ex) else {
            retval.set_bool(false);
            return;
        };
        let Ok(bridge) =
            bridge().inspect_err(|error| throw_cache_exception(error))
        else {
            retval.set_bool(false);
            return;
        };
        set_bool_result(retval, bridge.block_on(repository.clear()));
    }));
}

fn set_bool_result(
    retval: &mut Zval,
    result: Result<(), String>,
) {
    match result {
        Ok(()) => retval.set_bool(true),
        Err(error) => {
            retval.set_bool(false);
            throw_cache_exception(error);
        }
    }
}

fn throw_cache_exception(error: impl std::fmt::Display) {
    PhpException::from_message(format!(
        "Claviron cache operation failed: {error}"
    ))
    .throw();
}

fn parse_handle_key(
    ex: &mut ExecuteData
) -> Option<(Arc<CacheRepository>, String)> {
    let mut cache =
        Arg::new("cache", DataType::object(PhpCacheHandle::CLASS_NAME));
    let mut key = Arg::new("key", DataType::String);
    ex.parser().arg(&mut cache).arg(&mut key).parse().ok()?;
    Some((repository_from_arg(&mut cache)?, key.consume::<String>().ok()?))
}

fn parse_handle(ex: &mut ExecuteData) -> Option<Arc<CacheRepository>> {
    let mut cache =
        Arg::new("cache", DataType::object(PhpCacheHandle::CLASS_NAME));
    ex.parser().arg(&mut cache).parse().ok()?;
    repository_from_arg(&mut cache)
}

fn repository_from_arg(arg: &mut Arg<'_>) -> Option<Arc<CacheRepository>> {
    let object = arg.zval()?.object()?;
    let handle = ZendClassObject::<PhpCacheHandle>::from_zend_obj(object)?;
    Some(handle.repository.clone())
}

fn parse_store_name(ex: &mut ExecuteData) -> Option<Option<String>> {
    let mut name = Arg::new("name", DataType::String).allow_null();
    ex.parser().not_required().arg(&mut name).parse().ok()?;
    optional_string(name)
}

fn parse_put(
    ex: &mut ExecuteData
) -> Result<(Arc<CacheRepository>, String, CacheValue, Option<i64>), String> {
    let mut cache =
        Arg::new("cache", DataType::object(PhpCacheHandle::CLASS_NAME));
    let mut key = Arg::new("key", DataType::String);
    let mut value = Arg::new("value", DataType::Mixed);
    let mut seconds = Arg::new("seconds", DataType::Long).allow_null();
    ex.parser()
        .arg(&mut cache)
        .arg(&mut key)
        .arg(&mut value)
        .not_required()
        .arg(&mut seconds)
        .parse()
        .map_err(|error| error.to_string())?;

    let repository = repository_from_arg(&mut cache).ok_or_else(|| {
        "cache handle must be a Claviron\\Runtime\\CacheHandle".to_string()
    })?;
    let key = key
        .consume::<String>()
        .map_err(|_| "cache key must be a string".to_string())?;
    let value = value.zval().and_then(|value| cache_value_from_zval(value)).ok_or_else(|| {
        "unsupported cache value; expected null, bool, int, float, string, bytes, or array"
            .to_string()
    })?;
    let seconds = optional_i64(seconds)
        .ok_or_else(|| "cache TTL must be an integer".to_string())?;
    Ok((repository, key, value, seconds))
}

fn optional_string(mut arg: Arg<'_>) -> Option<Option<String>> {
    match arg.zval() {
        None => Some(None),
        Some(value) if value.is_null() => Some(None),
        Some(_) => arg.consume::<String>().ok().map(Some),
    }
}

fn optional_i64(mut arg: Arg<'_>) -> Option<Option<i64>> {
    match arg.zval() {
        None => Some(None),
        Some(value) if value.is_null() => Some(None),
        Some(_) => arg.consume::<i64>().ok().map(Some),
    }
}

fn cache_value_from_zval(zval: &Zval) -> Option<CacheValue> {
    if zval.is_null() {
        return Some(CacheValue::Null);
    }
    if let Some(value) = zval.bool() {
        return Some(CacheValue::Bool(value));
    }
    if let Some(value) = zval.long() {
        return if value >= 0 {
            Some(CacheValue::U64(value as u64))
        } else {
            Some(CacheValue::I64(value))
        };
    }
    if let Some(value) = zval.double() {
        return Some(CacheValue::F64(value));
    }
    if let Some(value) = zval.string() {
        return Some(CacheValue::String(value));
    }
    if zval.is_array() {
        return cache_json_from_zval(zval).map(CacheValue::Json);
    }
    zval.binary::<u8>().map(CacheValue::Bytes)
}

fn set_retval_from_cache_value(
    retval: &mut Zval,
    value: CacheValueRef<'_>,
) {
    match value {
        CacheValueRef::Null => retval.set_null(),
        CacheValueRef::Bool(value) => retval.set_bool(value),
        CacheValueRef::I64(value) => retval.set_long(value),
        CacheValueRef::U64(value) => set_u64(retval, value),
        CacheValueRef::F64(value) => retval.set_double(value),
        CacheValueRef::String(value) => {
            if retval.set_string(value, false).is_err() {
                retval.set_null();
            }
        }
        CacheValueRef::Bytes(value) => {
            retval
                .set_zend_string(ext_php_rs::types::ZendStr::new(value, false));
        }
        CacheValueRef::Json(value) => {
            if set_zval_from_archived_cache_json(retval, value).is_err() {
                retval.set_null();
            }
        }
        CacheValueRef::LegacyJson(value) => {
            if set_zval_from_cache_json(retval, &value).is_err() {
                retval.set_null();
            }
        }
    }
}

fn set_zval_from_archived_cache_json(
    retval: &mut Zval,
    value: &ArchivedCacheJson,
) -> Result<(), ext_php_rs::error::Error> {
    match value {
        ArchivedCacheJson::Null => retval.set_null(),
        ArchivedCacheJson::Bool(value) => retval.set_bool(*value),
        ArchivedCacheJson::I64(value) => retval.set_long(value.to_native()),
        ArchivedCacheJson::U64(value) => {
            let value = value.to_native();
            if let Ok(value) = i64::try_from(value) {
                retval.set_long(value);
            } else {
                retval.set_double(value as f64);
            }
        }
        ArchivedCacheJson::F64(value) => retval.set_double(value.to_native()),
        ArchivedCacheJson::String(value) => {
            retval.set_string(value.as_str(), false)?;
        }
        ArchivedCacheJson::Array(values) => {
            let mut array = ZendHashTable::with_capacity(
                values.len().try_into().unwrap_or(u32::MAX),
            );
            for value in values.iter() {
                let mut zval = Zval::new();
                set_zval_from_archived_cache_json(&mut zval, value)?;
                array.push(zval)?;
            }
            retval.set_hashtable(array);
        }
        ArchivedCacheJson::Object(values) => {
            let mut array = ZendHashTable::with_capacity(
                values.len().try_into().unwrap_or(u32::MAX),
            );
            for pair in values.iter() {
                let mut zval = Zval::new();
                set_zval_from_archived_cache_json(&mut zval, &pair.1)?;
                array.insert(ArrayKey::Str(pair.0.as_str()), zval)?;
            }
            retval.set_hashtable(array);
        }
    }
    Ok(())
}

fn cache_json_from_zval(zval: &Zval) -> Option<CacheJson> {
    if zval.is_null() {
        return Some(CacheJson::Null);
    }
    if let Some(value) = zval.bool() {
        return Some(CacheJson::Bool(value));
    }
    if let Some(value) = zval.long() {
        return if value >= 0 {
            Some(CacheJson::U64(value as u64))
        } else {
            Some(CacheJson::I64(value))
        };
    }
    if let Some(value) = zval.double() {
        return value.is_finite().then_some(CacheJson::F64(value));
    }
    if let Some(value) = zval.string() {
        return Some(CacheJson::String(value));
    }

    let array = zval.array()?;
    if array.has_sequential_keys() {
        let values = array
            .values()
            .map(cache_json_from_zval)
            .collect::<Option<Vec<_>>>()?;
        return Some(CacheJson::Array(values));
    }

    let mut object = Vec::with_capacity(array.len());
    for (key, value) in array {
        object.push((key.to_string(), cache_json_from_zval(value)?));
    }
    Some(CacheJson::Object(object))
}

fn set_zval_from_cache_json(
    retval: &mut Zval,
    value: &CacheJson,
) -> Result<(), ext_php_rs::error::Error> {
    match value {
        CacheJson::Null => {
            retval.set_null();
            Ok(())
        }
        CacheJson::Bool(value) => {
            retval.set_bool(*value);
            Ok(())
        }
        CacheJson::I64(value) => {
            retval.set_long(*value);
            Ok(())
        }
        CacheJson::U64(value) => {
            if let Ok(value) = i64::try_from(*value) {
                retval.set_long(value);
            } else {
                retval.set_double(*value as f64);
            }
            Ok(())
        }
        CacheJson::F64(value) => {
            retval.set_double(*value);
            Ok(())
        }
        CacheJson::String(value) => retval.set_string(value, false).map(|_| ()),
        CacheJson::Array(values) => {
            let mut array = ZendHashTable::with_capacity(
                values.len().try_into().unwrap_or(u32::MAX),
            );
            for value in values {
                let mut zval = Zval::new();
                set_zval_from_cache_json(&mut zval, value)?;
                array.push(zval)?;
            }
            retval.set_hashtable(array);
            Ok(())
        }
        CacheJson::Object(values) => {
            let mut array = ZendHashTable::with_capacity(
                values.len().try_into().unwrap_or(u32::MAX),
            );
            for (key, value) in values {
                let mut zval = Zval::new();
                set_zval_from_cache_json(&mut zval, value)?;
                array.insert(ArrayKey::String(key.clone()), zval)?;
            }
            retval.set_hashtable(array);
            Ok(())
        }
    }
}

fn set_u64(
    retval: &mut Zval,
    value: u64,
) {
    if let Ok(value) = i64::try_from(value) {
        retval.set_long(value);
    } else {
        let value = value.to_string();
        if retval.set_string(&value, false).is_err() {
            retval.set_null();
        }
    }
}
