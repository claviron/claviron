use std::time::Duration;

/// What bounds an application's instances: how many callers it admits and
/// how long one unit of work may run on an instance.
///
/// Running, not request handling. How long a request waits for an instance,
/// and everything else about how a request is handled, is its route's
/// (`requests:` / `responses:`); these belong to the instances, which serve
/// work that arrives with no route too. Engines retain ownership of how they
/// acquire an execution slot and how an over-budget guest is interrupted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationLimits {
    max_waiting: usize,
    execution_timeout: Option<Duration>,
}

impl ApplicationLimits {
    pub const fn new(
        max_waiting: usize,
        execution_timeout: Option<Duration>,
    ) -> Self {
        Self { max_waiting, execution_timeout }
    }

    /// Maximum concurrent callers admitted to application dispatch, including
    /// active executions and callers waiting for an execution slot.
    pub const fn max_waiting(self) -> usize {
        self.max_waiting
    }

    /// Maximum time owned by one accepted application execution.
    /// None intentionally leaves execution unbounded.
    pub const fn execution_timeout(self) -> Option<Duration> {
        self.execution_timeout
    }
}

/// Runtime-independent replacement policy for one application instance.
///
/// Engines keep ownership of what an instance is — a PHP worker thread, a V8
/// isolate owner, whatever a later embed brings — and of how they notice one
/// has gone. This value only says how long to wait before the next attempt
/// and when to stop attempting, so the two runtimes cannot drift into two
/// different curves.
///
/// Retry replaces a failed instance. It NEVER replays an accepted request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationRetryPolicy {
    backoff: Duration,
    backoff_max: Duration,
    max_attempts: Option<u32>,
}

impl ApplicationRetryPolicy {
    pub const fn new(
        backoff: Duration,
        backoff_max: Duration,
        max_attempts: Option<u32>,
    ) -> Self {
        Self { backoff, backoff_max, max_attempts }
    }

    /// First delay after a failure, doubled on each consecutive failure.
    pub const fn backoff(self) -> Duration {
        self.backoff
    }

    /// Ceiling the doubling stops at.
    pub const fn backoff_max(self) -> Duration {
        self.backoff_max
    }

    /// Consecutive failures allowed before an instance is left down.
    /// None retries for as long as the application is enabled.
    pub const fn max_attempts(self) -> Option<u32> {
        self.max_attempts
    }

    /// How long to wait before the attempt that follows `failures`
    /// consecutive failures: `backoff * 2^(failures - 1)`, capped at
    /// `backoff_max`. The shift is clamped so a long-running crash loop
    /// cannot overflow it.
    pub fn delay_for(
        self,
        failures: u32,
    ) -> Duration {
        self.backoff
            .saturating_mul(1 << failures.saturating_sub(1).min(30))
            .min(self.backoff_max)
    }

    /// Whether `failures` consecutive failures have used up the budget.
    /// A failure counter is expected to reset once the instance proves
    /// itself by completing work.
    pub fn exhausted(
        self,
        failures: u32,
    ) -> bool {
        self.max_attempts.is_some_and(|limit| failures >= limit)
    }

    pub fn validate(
        self,
        label: &str,
    ) -> Result<(), String> {
        if self.backoff.is_zero() {
            return Err(format!("{label}.backoff must be greater than zero"));
        }
        if self.backoff > self.backoff_max {
            return Err(format!(
                "{label}.backoff must not exceed {label}.backoff_max"
            ));
        }
        if self.max_attempts == Some(0) {
            return Err(format!(
                "{label}.max_attempts must be greater than zero or null"
            ));
        }
        Ok(())
    }
}

impl Default for ApplicationRetryPolicy {
    fn default() -> Self {
        Self {
            backoff: Duration::from_secs(1),
            backoff_max: Duration::from_secs(10),
            max_attempts: None,
        }
    }
}
