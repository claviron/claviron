//! PHP-facing Orbit lock primitives.
//!
//! These functions intentionally mirror a lock driver's atomic mechanics.
//! PHP owns lock objects, owner generation, blocking/retry policy, callbacks,
//! and `finally` release.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use claviron_orbit::FleetProvider;
use claviron_orbit::lock::{
    LockAcquire, LockKey, LockOwner, LockType, OrbitLock,
};
use ext_php_rs::args::Arg;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::Zval;
use ext_php_rs::zend::ExecuteData;
use tracing::debug;

use crate::extension::FunctionEntries;

static ORBIT_LOCK: OnceLock<ArcSwapOption<OrbitLock>> = OnceLock::new();

struct PhpLock;

impl LockType for PhpLock {
    const NAMESPACE: &'static str = "claviron.php";
}

fn lock_slot() -> &'static ArcSwapOption<OrbitLock> {
    ORBIT_LOCK.get_or_init(ArcSwapOption::const_empty)
}

pub(crate) fn configure(provider: Arc<FleetProvider>) {
    let lock = provider
        .handle()
        .and_then(|fleet| OrbitLock::new(fleet).ok())
        .map(Arc::new);
    let enabled = lock.is_some();
    lock_slot().store(lock);
    debug!(enabled, "PHP Orbit lock host functions configured");
}

fn locks() -> Option<Arc<OrbitLock>> {
    lock_slot().load_full()
}

pub(crate) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    let functions = [
        FunctionBuilder::new("orbit_lock_acquire", acquire_handler)
            .arg(Arg::new("key", DataType::String))
            .arg(Arg::new("owner", DataType::String))
            .arg(Arg::new("ttl_ms", DataType::Long))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("orbit_lock_acquire: {error}"))?,
        FunctionBuilder::new("orbit_lock_release", release_handler)
            .arg(Arg::new("key", DataType::String))
            .arg(Arg::new("owner", DataType::String))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("orbit_lock_release: {error}"))?,
        FunctionBuilder::new("orbit_lock_owner", owner_handler)
            .arg(Arg::new("key", DataType::String))
            .returns(DataType::String, false, true)
            .build()
            .map_err(|error| format!("orbit_lock_owner: {error}"))?,
        FunctionBuilder::new("orbit_lock_force_release", force_release_handler)
            .arg(Arg::new("key", DataType::String))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("orbit_lock_force_release: {error}"))?,
    ];

    entries.extend(functions);
    Ok(())
}

pub(crate) extern "C" fn acquire_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let Some((key, owner, ttl)) = parse_key_owner_ttl(ex) else {
            retval.set_bool(false);
            return;
        };
        let Some(locks) = locks() else {
            retval.set_bool(false);
            return;
        };

        let acquired = matches!(
            locks.try_acquire(&key, &owner, ttl),
            Ok(LockAcquire::Acquired(_))
        );
        retval.set_bool(acquired);
    }));
}

pub(crate) extern "C" fn release_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let Some((key, owner)) = parse_key_owner(ex) else {
            retval.set_bool(false);
            return;
        };
        let Some(locks) = locks() else {
            retval.set_bool(false);
            return;
        };

        retval.set_bool(locks.release_owned(&key, &owner).unwrap_or(false));
    }));
}

pub(crate) extern "C" fn owner_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let Some(key) = parse_key(ex) else {
            retval.set_null();
            return;
        };
        let Some(locks) = locks() else {
            retval.set_null();
            return;
        };

        let Ok(Some(lease)) = locks.current(&key) else {
            retval.set_null();
            return;
        };
        let Ok(owner) = std::str::from_utf8(lease.owner.as_bytes()) else {
            retval.set_null();
            return;
        };
        if retval.set_string(owner, false).is_err() {
            retval.set_null();
        }
    }));
}

pub(crate) extern "C" fn force_release_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let Some(key) = parse_key(ex) else {
            retval.set_bool(false);
            return;
        };
        let Some(locks) = locks() else {
            retval.set_bool(false);
            return;
        };

        retval.set_bool(locks.force_release(&key).unwrap_or(false));
    }));
}

fn parse_key(ex: &mut ExecuteData) -> Option<LockKey> {
    let mut key = Arg::new("key", DataType::String);
    ex.parser().arg(&mut key).parse().ok()?;
    Some(LockKey::new::<PhpLock>(key.consume::<String>().ok()?))
}

fn parse_key_owner(ex: &mut ExecuteData) -> Option<(LockKey, LockOwner)> {
    let mut key = Arg::new("key", DataType::String);
    let mut owner = Arg::new("owner", DataType::String);
    ex.parser().arg(&mut key).arg(&mut owner).parse().ok()?;
    Some((
        LockKey::new::<PhpLock>(key.consume::<String>().ok()?),
        LockOwner::from(owner.consume::<String>().ok()?),
    ))
}

fn parse_key_owner_ttl(
    ex: &mut ExecuteData
) -> Option<(LockKey, LockOwner, Duration)> {
    let mut key = Arg::new("key", DataType::String);
    let mut owner = Arg::new("owner", DataType::String);
    let mut ttl_ms = Arg::new("ttl_ms", DataType::Long);
    ex.parser().arg(&mut key).arg(&mut owner).arg(&mut ttl_ms).parse().ok()?;
    let ttl_ms = ttl_ms.consume::<i64>().ok()?;
    if ttl_ms <= 0 {
        return None;
    }
    Some((
        LockKey::new::<PhpLock>(key.consume::<String>().ok()?),
        LockOwner::from(owner.consume::<String>().ok()?),
        Duration::from_millis(ttl_ms as u64),
    ))
}
