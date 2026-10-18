//! What this process is doing right now.
//!
//! Not metrics. A metric is a number someone else reads later, and it may be
//! absent without changing how the process behaves; this is state the process
//! acts on — the watchdog decides from it whether requests are still moving.
//! That is why it has no feature gate: a build that cannot count its own
//! in-flight work cannot tell whether it is wedged.
//!
//! The metrics layer reports these numbers when it is enabled. It does not own
//! them.
//!
//! It lives in the runtime rather than in the HTTP layer because the question
//! is the runtime's: an edge worker counts HTTP exchanges, a Python runner
//! counts ASGI exchanges, a task host counts tasks, and the answer to "has
//! this process stopped serving" has the same shape for all three.
//!
//! ## Two counts, because two things are being asked
//!
//! An **exchange** is expected to end: a request dispatched to a host
//! application, whichever shape that application has. It is counted while the
//! application is producing a response — not while the body is being
//! transferred, so a client slowly draining a large file does not look like a
//! request that is stuck.
//!
//! A **session** is not expected to end: a WebSocket, an H2 CONNECT tunnel, a
//! WebTransport session. It legitimately stays open for hours with nothing
//! happening, so putting it in the first count would make a healthy process
//! look stalled. It is still worth knowing how many are open, which is the
//! only question its count answers. The watchdog reads the first and ignores
//! the second.

use std::sync::atomic::{AtomicU64, Ordering};

/// This process's live work.
pub static ACTIVITY: Activity = Activity::new();

pub struct Activity {
    in_flight: AtomicU64,
    total: AtomicU64,
    sessions: AtomicU64,
    sessions_total: AtomicU64,
}

impl Activity {
    pub const fn new() -> Self {
        Self {
            in_flight: AtomicU64::new(0),
            total: AtomicU64::new(0),
            sessions: AtomicU64::new(0),
            sessions_total: AtomicU64::new(0),
        }
    }

    /// Mark one exchange as begun; the guard marks it finished when dropped.
    ///
    /// A guard rather than a pair of calls, because the paths this wraps end
    /// in ways a matching call would miss: a cancelled future when the client
    /// disconnects, a panic in a handler, a response dropped mid-flight. A
    /// count that leaks upward on those is a watchdog that cries stall.
    #[inline]
    pub fn begin(&'static self) -> Exchange {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
        Exchange { activity: self }
    }

    #[inline]
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    /// Mark one long-lived session as open; the guard closes it when dropped.
    ///
    /// Deliberately not an exchange: nothing here expects it to end, so the
    /// watchdog never reads it.
    #[inline]
    pub fn open_session(&'static self) -> Session {
        self.sessions.fetch_add(1, Ordering::Relaxed);
        self.sessions_total.fetch_add(1, Ordering::Relaxed);
        Session { activity: self }
    }

    /// Sessions open right now — WebSockets, CONNECT tunnels, WebTransport.
    #[inline]
    pub fn sessions(&self) -> u64 {
        self.sessions.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn sessions_total(&self) -> u64 {
        self.sessions_total.load(Ordering::Relaxed)
    }
}

impl Default for Activity {
    fn default() -> Self {
        Self::new()
    }
}

/// One exchange in flight, counted for as long as this lives.
pub struct Exchange {
    activity: &'static Activity,
}

impl Drop for Exchange {
    fn drop(&mut self) {
        self.activity.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// One long-lived session, counted for as long as this lives.
pub struct Session {
    activity: &'static Activity,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.activity.sessions.fetch_sub(1, Ordering::Relaxed);
    }
}
