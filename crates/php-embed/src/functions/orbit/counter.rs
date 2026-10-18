//! PHP-facing Orbit counter primitives.
//!
//! Counters are signed 64-bit integers. PHP callers choose any decimal scale
//! they need and pass the resulting integer units.

use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwapOption;
use claviron_orbit::{Counter, CounterProvider};
use ext_php_rs::args::Arg;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::Zval;
use ext_php_rs::zend::ExecuteData;
use tracing::debug;

use crate::extension::FunctionEntries;

static ORBIT_COUNTER: OnceLock<ArcSwapOption<Counter>> = OnceLock::new();

fn counter_slot() -> &'static ArcSwapOption<Counter> {
    ORBIT_COUNTER.get_or_init(ArcSwapOption::const_empty)
}

pub(crate) fn configure(provider: Arc<CounterProvider>) {
    let counter = provider.handle();
    let enabled = counter.is_some();
    counter_slot().store(counter);
    debug!(enabled, "PHP Orbit counter host functions configured");
}

fn counter() -> Option<Arc<Counter>> {
    counter_slot().load_full()
}

pub(crate) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    let functions = [
        FunctionBuilder::new("orbit_counter_get", get_handler)
            .arg(Arg::new("key", DataType::String))
            .returns(DataType::Long, false, true)
            .build()
            .map_err(|error| format!("orbit_counter_get: {error}"))?,
        FunctionBuilder::new("orbit_counter_inc", increment_handler)
            .arg(Arg::new("key", DataType::String))
            .not_required()
            .arg(Arg::new("by", DataType::Long))
            .returns(DataType::Mixed, false, false)
            .build()
            .map_err(|error| format!("orbit_counter_inc: {error}"))?,
        FunctionBuilder::new("orbit_counter_dec", decrement_handler)
            .arg(Arg::new("key", DataType::String))
            .not_required()
            .arg(Arg::new("by", DataType::Long))
            .returns(DataType::Mixed, false, false)
            .build()
            .map_err(|error| format!("orbit_counter_dec: {error}"))?,
        FunctionBuilder::new("orbit_counter_reset", reset_handler)
            .arg(Arg::new("key", DataType::String))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("orbit_counter_reset: {error}"))?,
    ];

    entries.extend(functions);
    Ok(())
}

pub(crate) extern "C" fn get_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let Some(key) = parse_key(ex) else {
            retval.set_null();
            return;
        };
        let Some(counter) = counter() else {
            retval.set_null();
            return;
        };

        match counter.get(&key) {
            Ok(Some(value)) => retval.set_long(value),
            Ok(None) | Err(_) => retval.set_null(),
        }
    }));
}

pub(crate) extern "C" fn increment_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        update(ex, retval, Counter::increment);
    }));
}

pub(crate) extern "C" fn decrement_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        update(ex, retval, Counter::decrement);
    }));
}

pub(crate) extern "C" fn reset_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let Some(key) = parse_key(ex) else {
            retval.set_bool(false);
            return;
        };
        let Some(counter) = counter() else {
            retval.set_bool(false);
            return;
        };

        retval.set_bool(counter.reset(&key).is_ok());
    }));
}

fn update(
    ex: &mut ExecuteData,
    retval: &mut Zval,
    operation: fn(&Counter, &str, i64) -> claviron_orbit::counter::Result<i64>,
) {
    let Some((key, by)) = parse_key_amount(ex) else {
        retval.set_bool(false);
        return;
    };
    let Some(counter) = counter() else {
        retval.set_bool(false);
        return;
    };

    match operation(&counter, &key, by) {
        Ok(value) => retval.set_long(value),
        Err(_) => retval.set_bool(false),
    }
}

fn parse_key(ex: &mut ExecuteData) -> Option<String> {
    let mut key = Arg::new("key", DataType::String);
    ex.parser().arg(&mut key).parse().ok()?;
    key.consume::<String>().ok()
}

fn parse_key_amount(ex: &mut ExecuteData) -> Option<(String, i64)> {
    let mut key = Arg::new("key", DataType::String);
    let mut by = Arg::new("by", DataType::Long);
    ex.parser().arg(&mut key).not_required().arg(&mut by).parse().ok()?;
    let key = key.consume::<String>().ok()?;
    let by = by.consume::<i64>().unwrap_or(1);
    (by >= 0).then_some((key, by))
}
