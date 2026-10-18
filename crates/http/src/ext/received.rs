//! When a request reached the server.

use std::time::SystemTime;

/// The wall-clock time the edge took the request, before it waited for a
/// place among the host's requests. What an application reports as the
/// request's start — PHP's `$_SERVER['REQUEST_TIME_FLOAT']` — so that the
/// time it spent queued is part of the request, not hidden before it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceivedAt(pub SystemTime);
