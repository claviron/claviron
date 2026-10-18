//! Read-only, framework-neutral access to explicitly exported Congif values.

use std::sync::OnceLock;

use arc_swap::ArcSwapOption;
use claviron_runtime::AppState;
use congif::{ConfigReader, ConfigValue};
use ext_php_rs::args::Arg;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::{ArrayKey, ZendHashTable, Zval};
use ext_php_rs::zend::ExecuteData;

use crate::extension::FunctionEntries;

fn reader_slot() -> &'static ArcSwapOption<ConfigReader> {
    static READER: OnceLock<ArcSwapOption<ConfigReader>> = OnceLock::new();
    READER.get_or_init(ArcSwapOption::const_empty)
}

pub(crate) fn configure_from_state(state: &AppState) {
    reader_slot().store(Some(state.config_reader_handle()));
}

pub(crate) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    entries.push(
        FunctionBuilder::new("claviron_config", handler)
            .arg(Arg::new("key", DataType::String))
            .not_required()
            .arg(
                Arg::new("default", DataType::Mixed)
                    .allow_null()
                    .default("null"),
            )
            .returns(DataType::Mixed, false, true)
            .build()
            .map_err(|error| format!("claviron_config: {error}"))?,
    );
    Ok(())
}

fn lookup(
    reader: &ConfigReader,
    key: &str,
) -> Result<Option<ConfigValue>, String> {
    reader.value(key).map_err(|error| error.to_string())
}

extern "C" fn handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| -> Result<(), String> {
            let mut key = Arg::new("key", DataType::String);
            let mut default = Arg::new("default", DataType::Mixed).allow_null();
            ex.parser()
                .arg(&mut key)
                .not_required()
                .arg(&mut default)
                .parse()
                .map_err(|error| error.to_string())?;
            let key = key
                .consume::<String>()
                .map_err(|_| "config key must be a UTF-8 string".to_owned())?;
            let reader = reader_slot()
                .load_full()
                .ok_or("Claviron config reader is not configured")?;
            match lookup(&reader, &key)? {
                Some(value) => write_value(retval, value)?,
                None => match default.zval() {
                    Some(value) => *retval = value.shallow_clone(),
                    None => retval.set_null(),
                },
            }
            Ok(())
        })();
        if let Err(error) = result {
            retval.set_null();
            PhpException::from_message(format!(
                "Claviron config lookup failed: {error}"
            ))
            .throw();
        }
    }));
}

fn php_integer(value: u64) -> Result<i64, String> {
    i64::try_from(value)
        .map_err(|_| "config integer exceeds PHP's signed integer range".into())
}

fn array_key(value: ConfigValue) -> Result<ArrayKey<'static>, String> {
    Ok(match value {
        ConfigValue::String(value) => ArrayKey::String(value),
        ConfigValue::Char(value) => ArrayKey::String(value.to_string()),
        ConfigValue::I8(value) => ArrayKey::Long(value.into()),
        ConfigValue::I16(value) => ArrayKey::Long(value.into()),
        ConfigValue::I32(value) => ArrayKey::Long(value.into()),
        ConfigValue::I64(value) => ArrayKey::Long(value),
        ConfigValue::U8(value) => ArrayKey::Long(value.into()),
        ConfigValue::U16(value) => ArrayKey::Long(value.into()),
        ConfigValue::U32(value) => ArrayKey::Long(value.into()),
        ConfigValue::U64(value) => ArrayKey::Long(php_integer(value)?),
        ConfigValue::Newtype(value) => return array_key(*value),
        _ => {
            return Err(
                "config map keys must be strings or integers for PHP".into()
            );
        }
    })
}

fn write_value(
    retval: &mut Zval,
    value: ConfigValue,
) -> Result<(), String> {
    match value {
        ConfigValue::Unit | ConfigValue::Option(None) => retval.set_null(),
        ConfigValue::Bool(value) => retval.set_bool(value),
        ConfigValue::U8(value) => retval.set_long(value),
        ConfigValue::U16(value) => retval.set_long(value),
        ConfigValue::U32(value) => retval.set_long(value),
        ConfigValue::U64(value) => retval.set_long(php_integer(value)?),
        ConfigValue::I8(value) => retval.set_long(value),
        ConfigValue::I16(value) => retval.set_long(value),
        ConfigValue::I32(value) => retval.set_long(value),
        ConfigValue::I64(value) => retval.set_long(value),
        ConfigValue::F32(value) => retval.set_double(value),
        ConfigValue::F64(value) => retval.set_double(value),
        ConfigValue::Char(value) => retval
            .set_string(&value.to_string(), false)
            .map_err(|error| error.to_string())?,
        ConfigValue::String(value) => retval
            .set_string(&value, false)
            .map_err(|error| error.to_string())?,
        ConfigValue::Bytes(value) => retval.set_binary(value),
        ConfigValue::Option(Some(value)) | ConfigValue::Newtype(value) => {
            write_value(retval, *value)?
        }
        ConfigValue::Seq(values) => {
            let mut array = ZendHashTable::new();
            for value in values {
                let mut element = Zval::new();
                write_value(&mut element, value)?;
                array.push(element).map_err(|error| error.to_string())?;
            }
            retval.set_hashtable(array);
        }
        ConfigValue::Map(values) => {
            let mut array = ZendHashTable::new();
            for (key, value) in values {
                let key = array_key(key)?;
                let mut element = Zval::new();
                write_value(&mut element, value)?;
                array
                    .insert(key, element)
                    .map_err(|error| error.to_string())?;
            }
            retval.set_hashtable(array);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/functions/config.rs"]
mod tests;
