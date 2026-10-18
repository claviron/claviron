//! What the pool *did*, as opposed to what it currently holds.
//!
//! `live` / `idle` / `replenishing` are gauges, and a gauge cannot tell a
//! healthy saturation from a purge storm: both read as a full pool at every
//! sample, and everything between two samples is gone. Every behaviour the
//! pool work of 2026-09 touched — a caller queueing for a slot, that wait
//! expiring into a 504, a connection dialled or failing its ping, a stale
//! eviction and which of the three limits caused it, a replayed request, or
//! a queue suspected and then condemned — happens between samples and left no trace
//! at all.
//!
//! These are counters, so the ones that matter are monotonic and a
//! dashboard reads them with `rate()`. They remain local to the process
//! that owns the pool.
//!
//! Counting is one relaxed `fetch_add` on paths that already dial a socket,
//! park a thread or drop a queue, so it is not gated behind the `metrics`
//! feature: a pool that behaves differently depending on whether anyone is
//! watching is a worse thing to own than twelve atomics.

use std::sync::atomic::{AtomicU64, Ordering};

/// The counters themselves, owned by a pool's `Inner`.
#[derive(Debug, Default)]
pub struct PoolEvents {
    waited: AtomicU64,
    wait_expired: AtomicU64,
    dialled: AtomicU64,
    dial_failed: AtomicU64,
    ping_failed: AtomicU64,
    expired_requests: AtomicU64,
    expired_lifetime: AtomicU64,
    expired_idle: AtomicU64,
    purged: AtomicU64,
    replayed: AtomicU64,
    queue_suspected: AtomicU64,
    queue_condemned: AtomicU64,
}

impl PoolEvents {
    /// A caller reached the wait: the pool was dry and at a ceiling, so it
    /// queued rather than being served or dialling.
    pub(crate) fn waited(&self) {
        self.waited.fetch_add(1, Ordering::Relaxed);
    }

    /// A wait ran out of `acquire_timeout`. This is the 504 the operator
    /// sees, counted where it is decided rather than where it is rendered.
    pub(crate) fn wait_expired(&self) {
        self.wait_expired.fetch_add(1, Ordering::Relaxed);
    }

    /// A connection was opened, by the replenisher or by an acquire that
    /// found nothing warm.
    pub(crate) fn dialled(&self) {
        self.dialled.fetch_add(1, Ordering::Relaxed);
    }

    /// A dial did not produce a connection.
    pub(crate) fn dial_failed(&self) {
        self.dial_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// A dialled connection failed its `ping.path` and never joined the
    /// queue. Distinct from `dial_failed`: the socket came up and FPM did
    /// not answer on it, which is the failure `ping_path` exists to catch.
    pub(crate) fn ping_failed(&self) {
        self.ping_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// A connection was retired for reaching one of its three limits, by
    /// the limit that retired it.
    pub(crate) fn expired(
        &self,
        reason: Expiry,
    ) {
        match reason {
            Expiry::Requests => &self.expired_requests,
            Expiry::Lifetime => &self.expired_lifetime,
            Expiry::Idle => &self.expired_idle,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    /// Connections dropped by a confirmed purge. Counted by connection and
    /// not by purge, because the question the counter answers is how much a
    /// purge cost, and `queue_condemned` beside it already says how often.
    pub(crate) fn purged(
        &self,
        connections: usize,
    ) {
        self.purged.fetch_add(connections as u64, Ordering::Relaxed);
    }

    /// A buffered request was sent a second time, on a connection the pool
    /// had never handed out.
    pub(crate) fn replayed(&self) {
        self.replayed.fetch_add(1, Ordering::Relaxed);
    }

    /// A reused connection broke and the queue was left standing.
    pub(crate) fn queue_suspected(&self) {
        self.queue_suspected.fetch_add(1, Ordering::Relaxed);
    }

    /// A second reused connection agreed, and the queue was dropped. The
    /// ratio of this to `queue_suspected` is the one number that says
    /// whether the corroboration rule is earning its retry.
    pub(crate) fn queue_condemned(&self) {
        self.queue_condemned.fetch_add(1, Ordering::Relaxed);
    }

    /// Read every counter. Not an atomic view of all twelve — they are
    /// counters, so a sample that catches one increment and misses its
    /// neighbour is a sample taken a microsecond earlier, not a wrong one.
    #[cfg_attr(not(feature = "metrics"), allow(dead_code))]
    pub(crate) fn snapshot(&self) -> PoolEventCounts {
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        PoolEventCounts {
            waited: read(&self.waited),
            wait_expired: read(&self.wait_expired),
            dialled: read(&self.dialled),
            dial_failed: read(&self.dial_failed),
            ping_failed: read(&self.ping_failed),
            expired_requests: read(&self.expired_requests),
            expired_lifetime: read(&self.expired_lifetime),
            expired_idle: read(&self.expired_idle),
            purged: read(&self.purged),
            replayed: read(&self.replayed),
            queue_suspected: read(&self.queue_suspected),
            queue_condemned: read(&self.queue_condemned),
        }
    }
}

/// Which of a connection's three limits retired it.
///
/// The three are separate because they say different things about a
/// deployment: `Requests` is the rotation working as configured, `Lifetime`
/// is a connection outliving its welcome, and `Idle` is a pool wider than
/// the traffic it was sized for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expiry {
    /// Reached `max_requests` — the configured rotation.
    Requests,
    /// Reached `max_lifetime`.
    Lifetime,
    /// Sat unused for `idle_timeout`.
    Idle,
}

/// One reading of a pool's counters.
///
/// Adds across pools and across workers, which is the whole reason these
/// can be scalars on the wire: a total is a total wherever it was counted,
/// unlike a ceiling.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "metrics", derive(serde::Serialize, serde::Deserialize))]
pub struct PoolEventCounts {
    pub waited: u64,
    pub wait_expired: u64,
    pub dialled: u64,
    pub dial_failed: u64,
    pub ping_failed: u64,
    pub expired_requests: u64,
    pub expired_lifetime: u64,
    pub expired_idle: u64,
    pub purged: u64,
    pub replayed: u64,
    pub queue_suspected: u64,
    pub queue_condemned: u64,
}

impl PoolEventCounts {
    /// Every counter, with the series name and help it renders under.
    ///
    /// One list used by the per-pool metrics rows, so a counter's value,
    /// name and help text stay together.
    #[cfg_attr(not(feature = "metrics"), allow(dead_code))]
    pub(crate) const SERIES: [PoolEventSeries; 12] = [
        ("acquire_waited_total", "Acquires that queued for a slot", |c| {
            c.waited
        }),
        (
            "acquire_timeout_total",
            "Acquires that ran out of acquire_timeout and answered 504",
            |c| c.wait_expired,
        ),
        ("dial_total", "Connections opened to FPM", |c| c.dialled),
        ("dial_failed_total", "Dials that did not produce a connection", |c| {
            c.dial_failed
        }),
        (
            "ping_failed_total",
            "Connections dropped because FPM did not answer ping.path",
            |c| c.ping_failed,
        ),
        (
            "expired_requests_total",
            "Connections retired at max_requests (the configured rotation)",
            |c| c.expired_requests,
        ),
        (
            "expired_lifetime_total",
            "Connections retired at max_lifetime",
            |c| c.expired_lifetime,
        ),
        ("expired_idle_total", "Connections retired at idle_timeout", |c| {
            c.expired_idle
        }),
        (
            "purged_total",
            "Connections dropped by a condemned idle queue",
            |c| c.purged,
        ),
        (
            "replayed_total",
            "Buffered requests sent a second time on a fresh connection",
            |c| c.replayed,
        ),
        (
            "queue_suspected_total",
            "Reused connections that broke without condemning the queue",
            |c| c.queue_suspected,
        ),
        (
            "queue_condemned_total",
            "Times a second break confirmed the queue and purged it",
            |c| c.queue_condemned,
        ),
    ];
}

type PoolEventSeries =
    (&'static str, &'static str, fn(&PoolEventCounts) -> u64);
