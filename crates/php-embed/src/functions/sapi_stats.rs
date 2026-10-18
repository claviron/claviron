//! `claviron_sapi_stats(): array` — exposes embed SAPI pool counters
//! to PHP.
//!
//! The PHP function returns both the local worker snapshot and a fleet
//! aggregate. Fleet stats use a dedicated SAPI SHM table: each worker
//! writes only its own slot, and readers fold the slots into totals.

#[cfg(test)]
use std::ffi::CString;
use std::io;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_orbit::shm::{
    ExistingBootPolicy, FinalizePolicy, ManagedShmProvider, ManagedShmResource,
    ManagedShmResources, PhysicalShmState, ShmCreateOutcome,
    ShmResourceDescriptor, ShmShape,
};
use ext_php_rs::boxed::ZBox;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::{ArrayKey, ZendHashTable, Zval};
use ext_php_rs::zend::ExecuteData;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::engine::{
    PhpSapiEngine, SapiThreadPoolSnapshot, SapiThreadSlotSnapshot,
};
use crate::extension::FunctionEntries;

/// Orbit kind of the SAPI stats table: filed under the fleet's name so
/// `orbit list` and `orbit clear` handle it with the rings.
const SAPI_STATS_KIND: u8 = 243;
const MAGIC: u32 = 0x0c1a_575a; // "claviron stats"
const VERSION: u32 = 5;
const HEADER_READY_RETRIES: usize = 100;
const HEADER_READY_SLEEP: Duration = Duration::from_millis(5);
const PUBLISH_INTERVAL: Duration = Duration::from_millis(250);
const STALE_AFTER_MS: u64 = 2_000;

static SAPI_ENGINE: OnceLock<ArcSwapOption<PhpSapiEngine>> = OnceLock::new();
static SAPI_STATS_TABLE: OnceLock<ArcSwapOption<SapiStatsTable>> =
    OnceLock::new();
static SAPI_STATS_ERROR: OnceLock<RwLock<Option<String>>> = OnceLock::new();

const SHM_PROVIDER_NAME: &str = "PhpSapiStatsShm";
const SHM_RESOURCE_NAME: &str = "php-sapi-stats";

pub(crate) type SapiStatsShm = ManagedShmProvider<SapiStatsShmResources>;

pub(crate) struct SapiStatsShmResources;

impl ManagedShmResources for SapiStatsShmResources {
    fn name(&self) -> &'static str {
        SHM_PROVIDER_NAME
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![self]
    }
}

#[async_trait]
impl ManagedShmResource for SapiStatsShmResources {
    fn descriptor(&self) -> ShmResourceDescriptor {
        ShmResourceDescriptor {
            provider: SHM_PROVIDER_NAME,
            resource: SHM_RESOURCE_NAME,
            shape: ShmShape::PosixRegion,
            orbit_kind: None,
            existing_boot: ExistingBootPolicy::Reuse,
            finalize: FinalizePolicy::Preserve,
        }
    }

    fn validate_physical(
        &self,
        state: &claviron_runtime::AppState,
    ) -> claviron_runtime::registry::Result<PhysicalShmState> {
        let name = shm_name_for(&crate::invalidation::segment_fleet(state));
        Ok(validate_existing_when_ready(
            &name,
            shm_size(sapi_worker_capacity() as usize),
        )?
        .into())
    }

    fn create_or_reuse(
        &self,
        state: &claviron_runtime::AppState,
    ) -> claviron_runtime::registry::Result<ShmCreateOutcome> {
        if state.is_worker() {
            return Err(claviron_runtime::registry::Error::msg(
                "PHP SAPI stats SHM is missing; the master must initialize it before worker boot",
            ));
        }
        open_for_lifecycle(state)
    }

    fn reuse(
        &self,
        state: &claviron_runtime::AppState,
    ) -> claviron_runtime::registry::Result<()> {
        open_for_lifecycle(state).map(drop)
    }
}

pub(crate) fn register_shm(
    state: &claviron_runtime::AppState
) -> claviron_runtime::registry::Result<()> {
    claviron_orbit::shm::register(state, SapiStatsShmResources)
}

#[cfg(test)]
fn reset_shared_memory(instance_name: &str) {
    let Ok(name) = CString::new(shm_name_for(instance_name)) else {
        return;
    };
    let rc = unsafe { libc::shm_unlink(name.as_ptr()) };
    if rc < 0 {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::NotFound {
            warn!(instance = instance_name, %error, "failed to unlink test SAPI stats SHM");
        }
    }
}

#[repr(C, align(64))]
struct Header {
    magic: u32,
    version: u32,
    worker_capacity: u32,
    _pad: [u8; 52],
}

#[repr(C, align(64))]
struct Slot {
    seq: AtomicU64,
    updated_at_unix_ms: u64,
    pid: u32,
    worker_id: u8,
    present: u8,
    accepting: u8,
    elastic: u8,
    threads_current: u32,
    max_threads_current: u32,
    busy_threads: u32,
    max_busy_threads: u32,
    idle_threads: u32,
    in_flight: u32,
    max_in_flight: u32,
    threads_min: u32,
    threads_max: u32,
    threads_max_idle: u32,
    thread_idle_timeout_ms: u64,
    request_count_total: u64,
    threads_created_total: u64,
}

pub(crate) struct SapiStatsTable {
    shm: orbitive::shm::ShmRegion,
    self_idx: u8,
    worker_capacity: u8,
    state: claviron_runtime::AppState,
}

#[derive(Clone, Copy)]
struct FleetWorkerStats {
    worker_id: u8,
    updated_at_unix_ms: u64,
    threads_current: u64,
    max_threads_current: u64,
    threads_max: u64,
    busy_threads: u64,
    max_busy_threads: u64,
    idle_threads: u64,
    in_flight: u64,
    max_in_flight: u64,
    request_count_total: u64,
    threads_created_total: u64,
}

struct FleetStatsSummary {
    ok: bool,
    source: &'static str,
    captured_at_unix_ms: u64,
    fleet_size: u8,
    fleet_seen: usize,
    missing_worker_ids: Vec<u8>,
    stale_worker_ids: Vec<u8>,
    threads_current_total: u64,
    threads_capacity_total: u64,
    max_threads_current_total: u64,
    busy_threads_total: u64,
    max_busy_threads_total: u64,
    idle_threads_total: u64,
    in_flight_total: u64,
    max_in_flight_total: u64,
    request_count_total: u64,
    threads_created_total: u64,
}

fn engine_slot() -> &'static ArcSwapOption<PhpSapiEngine> {
    SAPI_ENGINE.get_or_init(ArcSwapOption::const_empty)
}

fn table_slot() -> &'static ArcSwapOption<SapiStatsTable> {
    SAPI_STATS_TABLE.get_or_init(ArcSwapOption::const_empty)
}

fn error_slot() -> &'static RwLock<Option<String>> {
    SAPI_STATS_ERROR.get_or_init(|| RwLock::new(None))
}

pub(crate) fn configure_engine(engine: Arc<PhpSapiEngine>) {
    engine_slot().store(Some(engine));
}

pub(crate) fn start_publisher(
    state: claviron_runtime::AppState,
    engine: Arc<PhpSapiEngine>,
    cancellation: CancellationToken,
) -> Option<JoinHandle<()>> {
    let worker_id =
        state.worker_id().or_else(|| state.is_standalone().then_some(1))?;
    let worker_capacity = sapi_worker_capacity();
    let table = match SapiStatsTable::open(
        state.clone(),
        worker_id,
        worker_capacity,
    ) {
        Ok(table) => Arc::new(table),
        Err(err) => {
            set_error(format!("{err}"));
            warn!(error = %err, "SAPI stats table open failed; fleet stats disabled");
            return None;
        }
    };
    set_error(String::new());
    table_slot().store(Some(Arc::clone(&table)));

    Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(PUBLISH_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = ticker.tick() => {
                    if let Some(snapshot) = engine.pool_snapshot() {
                        table.publish(&snapshot);
                    }
                }
            }
        }
    }))
}

const fn sapi_worker_capacity() -> u8 {
    let capacity = claviron_runtime::FleetCapacity::BUILT.worker_capacity();
    if capacity == 0 { 1 } else { capacity }
}

fn open_for_lifecycle(
    state: &claviron_runtime::AppState
) -> claviron_runtime::registry::Result<ShmCreateOutcome> {
    // The master maps no writable lane; id 1 is used only to exercise the
    // complete table/header validation before workers are spawned.
    let validation_slot = state.worker_id().unwrap_or(1);
    SapiStatsTable::open(state.clone(), validation_slot, sapi_worker_capacity())
        .map(|table| {
            if table.shm.created() {
                ShmCreateOutcome::Created
            } else {
                ShmCreateOutcome::Opened
            }
        })
        .map_err(|error| {
            claviron_runtime::registry::Error::msg(format!(
                "PHP SAPI stats SHM validation failed: {error}"
            ))
        })
}

fn engine() -> Option<Arc<PhpSapiEngine>> {
    engine_slot().load_full()
}

fn table() -> Option<Arc<SapiStatsTable>> {
    table_slot().load_full()
}

fn set_error(error: String) {
    if let Ok(mut guard) = error_slot().write() {
        *guard = (!error.is_empty()).then_some(error);
    }
}

fn error_detail() -> Option<String> {
    error_slot().read().ok().and_then(|guard| guard.clone())
}

pub(crate) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    let stats = FunctionBuilder::new(
        "claviron_sapi_stats",
        claviron_sapi_stats_handler,
    )
    .returns(DataType::Array, false, true)
    .build()
    .map_err(|err| format!("claviron_sapi_stats: {err}"))?;
    entries.push(stats);
    Ok(())
}

pub(crate) extern "C" fn claviron_sapi_stats_handler(
    _ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let Some(snapshot) = engine().and_then(|engine| engine.pool_snapshot())
        else {
            retval.set_null();
            return;
        };

        if let Some(table) = table() {
            table.publish(&snapshot);
        }

        retval.set_hashtable(stats_to_array(
            snapshot,
            table().as_deref().and_then(SapiStatsTable::summary),
        ));
    }));
}

impl SapiStatsTable {
    fn open(
        state: claviron_runtime::AppState,
        worker_id: u8,
        worker_capacity: u8,
    ) -> io::Result<Self> {
        let instance_name = state.name();
        let self_idx = worker_id.checked_sub(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker_id is 0; IDs are 1-based",
            )
        })?;
        if self_idx >= worker_capacity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "worker_id {worker_id} > worker_capacity {worker_capacity}"
                ),
            ));
        }

        let shm_name =
            shm_name_for(&crate::invalidation::segment_fleet(&state));
        let total_size = shm_size(worker_capacity as usize);
        // Worker processes boot concurrently. Hold Orbitive's per-name process
        // lock from create/open through header initialization so a peer cannot
        // mmap the object between `shm_open` and `ftruncate` on macOS.
        let (shm, initialization_lock) =
            orbitive::shm::ShmRegion::open_or_create_locked(
                &shm_name, total_size,
            )?;
        let is_creator = shm.created();
        let shm_ptr =
            NonNull::new(shm.as_ptr()).expect("SHM mapping is non-null");

        if is_creator {
            unsafe {
                std::ptr::write(
                    shm_ptr.as_ptr().cast::<Header>(),
                    Header {
                        magic: MAGIC,
                        version: VERSION,
                        worker_capacity: u32::from(worker_capacity),
                        _pad: [0; 52],
                    },
                );
            }
            info!(
                instance = instance_name,
                worker_capacity,
                size = total_size,
                "📊 SAPI stats shm table created"
            );
        } else if let Err(err) = wait_for_ready_header(shm_ptr, worker_capacity)
        {
            return Err(err);
        } else {
            debug!(instance = instance_name, "📊 SAPI stats shm table opened");
        }
        drop(initialization_lock);

        Ok(Self { shm, self_idx, worker_capacity, state })
    }

    fn slot_ptr(
        &self,
        idx: u8,
    ) -> *mut Slot {
        debug_assert!(idx < self.worker_capacity);
        unsafe {
            self.shm
                .as_ptr()
                .add(std::mem::size_of::<Header>())
                .add(idx as usize * std::mem::size_of::<Slot>())
                .cast::<Slot>()
        }
    }

    fn publish(
        &self,
        snapshot: &SapiThreadPoolSnapshot,
    ) {
        let slot = unsafe { &mut *self.slot_ptr(self.self_idx) };
        let seq = slot.seq.load(Ordering::Relaxed);
        let odd = if seq % 2 == 0 { seq + 1 } else { seq + 2 };
        slot.seq.store(odd, Ordering::Release);

        slot.updated_at_unix_ms = now_unix_ms();
        slot.pid = std::process::id();
        slot.worker_id = self.self_idx + 1;
        slot.present = 1;
        slot.accepting = u8::from(snapshot.accepting);
        slot.elastic = u8::from(snapshot.elastic);
        slot.threads_current =
            snapshot.threads_current.min(u32::MAX as usize) as u32;
        slot.max_threads_current =
            snapshot.max_threads_current.min(u32::MAX as usize) as u32;
        slot.busy_threads = snapshot.busy_threads.min(u32::MAX as usize) as u32;
        slot.max_busy_threads =
            snapshot.max_busy_threads.min(u32::MAX as usize) as u32;
        slot.idle_threads = snapshot.idle_threads.min(u32::MAX as usize) as u32;
        slot.in_flight = snapshot.in_flight.min(u32::MAX as usize) as u32;
        slot.max_in_flight =
            snapshot.max_in_flight.min(u32::MAX as usize) as u32;
        slot.threads_min = snapshot.threads_min.min(u32::MAX as usize) as u32;
        slot.threads_max = snapshot.threads_max.min(u32::MAX as usize) as u32;
        slot.threads_max_idle =
            snapshot.threads_max_idle.min(u32::MAX as usize) as u32;
        slot.thread_idle_timeout_ms = snapshot.thread_idle_timeout_ms;
        slot.request_count_total = snapshot.request_count_total;
        slot.threads_created_total = snapshot.threads_created_total;

        slot.seq.store(odd + 1, Ordering::Release);
    }

    fn summary(&self) -> Option<FleetStatsSummary> {
        let members =
            match claviron_orbit::service::current_members(&self.state) {
                Ok(members) => members,
                Err(error) => {
                    set_error(format!("fleet membership unavailable: {error}"));
                    return None;
                }
            };
        let worker_ids = members
            .iter()
            .filter_map(|member| match member.role {
                claviron_orbit::service::FleetMemberRole::Master => None,
                claviron_orbit::service::FleetMemberRole::Standalone => Some(1),
                claviron_orbit::service::FleetMemberRole::Worker => {
                    Some(member.node_id as u8)
                }
            })
            .collect::<Vec<_>>();
        set_error(String::new());
        self.summary_for(&worker_ids)
    }

    fn summary_for(
        &self,
        worker_ids: &[u8],
    ) -> Option<FleetStatsSummary> {
        let captured_at = now_unix_ms();
        let mut workers = Vec::new();
        for &worker_id in worker_ids {
            if let Some(worker) = self.read_slot(worker_id - 1) {
                workers.push(worker);
            }
        }

        let mut missing_worker_ids = Vec::new();
        let mut stale_worker_ids = Vec::new();
        for &worker_id in worker_ids {
            match workers.iter().find(|worker| worker.worker_id == worker_id) {
                Some(worker)
                    if captured_at
                        .saturating_sub(worker.updated_at_unix_ms)
                        > STALE_AFTER_MS =>
                {
                    stale_worker_ids.push(worker_id)
                }
                Some(_) => {}
                None => missing_worker_ids.push(worker_id),
            }
        }

        let fresh_workers = workers.iter().filter(|worker| {
            captured_at.saturating_sub(worker.updated_at_unix_ms)
                <= STALE_AFTER_MS
        });
        let mut summary = FleetStatsSummary {
            ok: missing_worker_ids.is_empty() && stale_worker_ids.is_empty(),
            source: "embed-sapi-fleet",
            captured_at_unix_ms: captured_at,
            fleet_size: worker_ids.len() as u8,
            fleet_seen: workers.len(),
            missing_worker_ids,
            stale_worker_ids,
            threads_current_total: 0,
            threads_capacity_total: 0,
            max_threads_current_total: 0,
            busy_threads_total: 0,
            max_busy_threads_total: 0,
            idle_threads_total: 0,
            in_flight_total: 0,
            max_in_flight_total: 0,
            request_count_total: 0,
            threads_created_total: 0,
        };
        for worker in fresh_workers {
            summary.threads_current_total = summary
                .threads_current_total
                .saturating_add(worker.threads_current);
            summary.threads_capacity_total = summary
                .threads_capacity_total
                .saturating_add(worker.threads_max);
            summary.max_threads_current_total = summary
                .max_threads_current_total
                .saturating_add(worker.max_threads_current);
            summary.busy_threads_total =
                summary.busy_threads_total.saturating_add(worker.busy_threads);
            summary.max_busy_threads_total = summary
                .max_busy_threads_total
                .saturating_add(worker.max_busy_threads);
            summary.idle_threads_total =
                summary.idle_threads_total.saturating_add(worker.idle_threads);
            summary.in_flight_total =
                summary.in_flight_total.saturating_add(worker.in_flight);
            summary.max_in_flight_total = summary
                .max_in_flight_total
                .saturating_add(worker.max_in_flight);
            summary.request_count_total = summary
                .request_count_total
                .saturating_add(worker.request_count_total);
            summary.threads_created_total = summary
                .threads_created_total
                .saturating_add(worker.threads_created_total);
        }
        Some(summary)
    }

    fn read_slot(
        &self,
        idx: u8,
    ) -> Option<FleetWorkerStats> {
        let slot = unsafe { &*self.slot_ptr(idx) };
        let expected_worker_id = idx + 1;
        for _ in 0..3 {
            let seq_before = slot.seq.load(Ordering::Acquire);
            if seq_before == 0 || seq_before % 2 != 0 || slot.present == 0 {
                return None;
            }
            if slot.worker_id != expected_worker_id {
                return None;
            }
            let stats = FleetWorkerStats {
                worker_id: slot.worker_id,
                updated_at_unix_ms: slot.updated_at_unix_ms,
                threads_current: u64::from(slot.threads_current),
                max_threads_current: u64::from(slot.max_threads_current),
                threads_max: u64::from(slot.threads_max),
                busy_threads: u64::from(slot.busy_threads),
                max_busy_threads: u64::from(slot.max_busy_threads),
                idle_threads: u64::from(slot.idle_threads),
                in_flight: u64::from(slot.in_flight),
                max_in_flight: u64::from(slot.max_in_flight),
                request_count_total: slot.request_count_total,
                threads_created_total: slot.threads_created_total,
            };
            let seq_after = slot.seq.load(Ordering::Acquire);
            if seq_before == seq_after && seq_after % 2 == 0 {
                return Some(stats);
            }
        }
        None
    }
}

fn stats_to_array(
    local: SapiThreadPoolSnapshot,
    fleet: Option<FleetStatsSummary>,
) -> ZBox<ZendHashTable> {
    let mut array = ZendHashTable::with_capacity(4);
    insert_bool(&mut array, "ok", true);
    insert_string(&mut array, "source", "embed-sapi");
    insert_u64(&mut array, "captured_at_unix_ms", now_unix_ms());
    insert_array(&mut array, "fleet", fleet_to_array(fleet));
    insert_array(&mut array, "local", local_to_array(local));
    array
}

fn local_to_array(snapshot: SapiThreadPoolSnapshot) -> ZBox<ZendHashTable> {
    let mut array = ZendHashTable::with_capacity(18);
    insert_bool(&mut array, "ok", true);
    insert_string(&mut array, "source", "embed-sapi-local");
    insert_bool(&mut array, "accepting", snapshot.accepting);
    insert_bool(&mut array, "elastic", snapshot.elastic);
    insert_usize(&mut array, "threads_min", snapshot.threads_min);
    insert_usize(&mut array, "threads_max", snapshot.threads_max);
    insert_usize(&mut array, "threads_max_idle", snapshot.threads_max_idle);
    insert_u64(
        &mut array,
        "thread_idle_timeout_ms",
        snapshot.thread_idle_timeout_ms,
    );
    if let Some(ms) = snapshot.thread_max_life_ms {
        insert_u64(&mut array, "thread_max_life_ms", ms);
    }
    insert_usize(&mut array, "in_flight", snapshot.in_flight);
    insert_usize(&mut array, "max_in_flight", snapshot.max_in_flight);
    insert_usize(&mut array, "threads_current", snapshot.threads_current);
    insert_usize(
        &mut array,
        "max_threads_current",
        snapshot.max_threads_current,
    );
    insert_usize(
        &mut array,
        "max_threads_concurrent",
        snapshot.max_threads_current,
    );
    insert_u64(
        &mut array,
        "threads_created_total",
        snapshot.threads_created_total,
    );
    insert_usize(&mut array, "busy_threads", snapshot.busy_threads);
    insert_usize(&mut array, "max_busy_threads", snapshot.max_busy_threads);
    insert_usize(&mut array, "idle_threads", snapshot.idle_threads);
    insert_u64(&mut array, "request_count_total", snapshot.request_count_total);

    let mut slots = ZendHashTable::with_capacity(
        snapshot.slots.len().try_into().unwrap_or(0),
    );
    for slot in snapshot.slots {
        let mut zval = Zval::new();
        zval.set_hashtable(slot_to_array(slot));
        let _ = slots.push(zval);
    }
    let mut slots_zval = Zval::new();
    slots_zval.set_hashtable(slots);
    let _ = array.insert(ArrayKey::String("slots".to_string()), slots_zval);

    array
}

fn fleet_to_array(summary: Option<FleetStatsSummary>) -> ZBox<ZendHashTable> {
    let mut array = ZendHashTable::with_capacity(14);
    let Some(summary) = summary else {
        insert_bool(&mut array, "ok", false);
        insert_string(&mut array, "source", "embed-sapi-fleet");
        insert_string(&mut array, "error", "stats_table_unavailable");
        if let Some(detail) = error_detail() {
            insert_string(&mut array, "error_detail", &detail);
        }
        insert_u64(&mut array, "captured_at_unix_ms", now_unix_ms());
        insert_u64(&mut array, "fleet_size", 0);
        insert_u64(&mut array, "fleet_seen", 0);
        insert_u64(&mut array, "threads_current_total", 0);
        insert_u64(&mut array, "threads_capacity_total", 0);
        insert_u64(&mut array, "max_threads_current_total", 0);
        insert_u64(&mut array, "max_threads_concurrent_total", 0);
        insert_u64(&mut array, "threads_created_total", 0);
        insert_u64(&mut array, "busy_threads_total", 0);
        insert_u64(&mut array, "max_busy_threads_total", 0);
        insert_u64(&mut array, "idle_threads_total", 0);
        insert_u64(&mut array, "in_flight_total", 0);
        insert_u64(&mut array, "max_in_flight_total", 0);
        insert_u64(&mut array, "request_count_total", 0);
        insert_u8_list(&mut array, "missing_worker_ids", Vec::new());
        insert_u8_list(&mut array, "stale_worker_ids", Vec::new());
        return array;
    };

    insert_bool(&mut array, "ok", summary.ok);
    insert_string(&mut array, "source", summary.source);
    insert_u64(&mut array, "captured_at_unix_ms", summary.captured_at_unix_ms);
    insert_u64(&mut array, "fleet_size", u64::from(summary.fleet_size));
    insert_usize(&mut array, "fleet_seen", summary.fleet_seen);
    insert_u64(
        &mut array,
        "threads_current_total",
        summary.threads_current_total,
    );
    insert_u64(
        &mut array,
        "threads_capacity_total",
        summary.threads_capacity_total,
    );
    insert_u64(
        &mut array,
        "max_threads_current_total",
        summary.max_threads_current_total,
    );
    insert_u64(
        &mut array,
        "max_threads_concurrent_total",
        summary.max_threads_current_total,
    );
    insert_u64(
        &mut array,
        "threads_created_total",
        summary.threads_created_total,
    );
    insert_u64(&mut array, "busy_threads_total", summary.busy_threads_total);
    insert_u64(
        &mut array,
        "max_busy_threads_total",
        summary.max_busy_threads_total,
    );
    insert_u64(&mut array, "idle_threads_total", summary.idle_threads_total);
    insert_u64(&mut array, "in_flight_total", summary.in_flight_total);
    insert_u64(&mut array, "max_in_flight_total", summary.max_in_flight_total);
    insert_u64(&mut array, "request_count_total", summary.request_count_total);
    insert_u8_list(
        &mut array,
        "missing_worker_ids",
        summary.missing_worker_ids,
    );
    insert_u8_list(&mut array, "stale_worker_ids", summary.stale_worker_ids);
    array
}

fn slot_to_array(slot: SapiThreadSlotSnapshot) -> ZBox<ZendHashTable> {
    let mut array = ZendHashTable::with_capacity(5);
    insert_u64(&mut array, "id", u64::from(slot.id));
    insert_bool(&mut array, "busy", slot.busy);
    insert_u64(&mut array, "request_count", slot.request_count);
    insert_u64(&mut array, "idle_for_ms", slot.idle_for_ms);
    insert_u64(&mut array, "age_ms", slot.age_ms);
    array
}

fn insert_array(
    array: &mut ZendHashTable,
    key: &str,
    value: ZBox<ZendHashTable>,
) {
    let mut zval = Zval::new();
    zval.set_hashtable(value);
    let _ = array.insert(ArrayKey::String(key.to_string()), zval);
}

fn insert_bool(
    array: &mut ZendHashTable,
    key: &str,
    value: bool,
) {
    let mut zval = Zval::new();
    zval.set_bool(value);
    let _ = array.insert(ArrayKey::String(key.to_string()), zval);
}

fn insert_string(
    array: &mut ZendHashTable,
    key: &str,
    value: &str,
) {
    let mut zval = Zval::new();
    if zval.set_string(value, false).is_err() {
        zval.set_null();
    }
    let _ = array.insert(ArrayKey::String(key.to_string()), zval);
}

fn insert_usize(
    array: &mut ZendHashTable,
    key: &str,
    value: usize,
) {
    insert_u64(array, key, value as u64);
}

fn insert_u64(
    array: &mut ZendHashTable,
    key: &str,
    value: u64,
) {
    let mut zval = Zval::new();
    if let Ok(value) = i64::try_from(value) {
        zval.set_long(value);
    } else {
        let value = value.to_string();
        if zval.set_string(&value, false).is_err() {
            zval.set_null();
        }
    }
    let _ = array.insert(ArrayKey::String(key.to_string()), zval);
}

fn insert_u8_list(
    array: &mut ZendHashTable,
    key: &str,
    values: Vec<u8>,
) {
    let mut list =
        ZendHashTable::with_capacity(values.len().try_into().unwrap_or(0));
    for value in values {
        let mut zval = Zval::new();
        zval.set_long(i64::from(value));
        let _ = list.push(zval);
    }
    let mut zval = Zval::new();
    zval.set_hashtable(list);
    let _ = array.insert(ArrayKey::String(key.to_string()), zval);
}

/// `fleet_name` is the segment's fleet component: the Orbit fleet in a
/// deployment, a test's own short name when it never joined one.
fn shm_name_for(fleet_name: &str) -> String {
    orbitive::shm::ring_segment_name(fleet_name, SAPI_STATS_KIND)
}

const fn shm_size(worker_capacity: usize) -> usize {
    std::mem::size_of::<Header>()
        + worker_capacity * std::mem::size_of::<Slot>()
}

fn validate_existing_when_ready(
    name: &str,
    minimum_size: usize,
) -> io::Result<orbitive::shm::ShmValidation> {
    let mut last_error = None;
    for attempt in 0..=HEADER_READY_RETRIES {
        match orbitive::shm::ShmRegion::validate_existing(name, minimum_size) {
            Ok(validation) => return Ok(validation),
            Err(error)
                if attempt < HEADER_READY_RETRIES
                    && (error.kind() == io::ErrorKind::InvalidData
                        || error.raw_os_error() == Some(libc::EINVAL)) =>
            {
                last_error = Some(error);
                std::thread::sleep(HEADER_READY_SLEEP);
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.expect("validation retry records its error"))
}

fn wait_for_ready_header(
    shm: NonNull<u8>,
    worker_capacity: u8,
) -> io::Result<()> {
    let mut last_error = None;
    for attempt in 0..=HEADER_READY_RETRIES {
        let hdr = unsafe { &*shm.as_ptr().cast::<Header>() };
        match validate_header(hdr, worker_capacity) {
            Ok(()) => return Ok(()),
            Err(err) => {
                if hdr.magic != 0 || attempt == HEADER_READY_RETRIES {
                    last_error = Some(err);
                    break;
                }
                last_error = Some(err);
                std::thread::sleep(HEADER_READY_SLEEP);
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "SAPI stats shm header not ready",
        )
    }))
}

fn validate_header(
    hdr: &Header,
    worker_capacity: u8,
) -> io::Result<()> {
    if hdr.magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("SAPI stats shm magic mismatch: 0x{:x}", hdr.magic),
        ));
    }
    if hdr.version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("SAPI stats shm version {} != {}", hdr.version, VERSION),
        ));
    }
    if hdr.worker_capacity != u32::from(worker_capacity) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "SAPI stats shm worker_capacity {} != {}",
                hdr.worker_capacity, worker_capacity
            ),
        ));
    }
    Ok(())
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
#[path = "../../tests/functions/sapi_stats.rs"]
mod resize_tests;
