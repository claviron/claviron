//! Prepared queue/head timing around the selected application's capacity.

use std::future::Future;
use std::time::Duration;

use claviron_http::admission::{Admission, Place};
use claviron_http::policy::RequestPolicy;

/// Immutable timing policy prepared after listener/host narrowing.
///
/// Capacity still belongs to the current application. This stage borrows that
/// authority rather than retaining it in a connection's metadata cache.
/// Waiting and reply-head timing use HTTP's existing futures and RAII permits.
/// They do not become synchronous HostLayer callbacks or erased layer futures.
#[derive(Clone, Copy, Debug)]
pub struct AdmissionPolicy {
    queue_timeout: Option<Duration>,
    head_timeout: Option<Duration>,
}

impl AdmissionPolicy {
    pub fn new(policy: RequestPolicy) -> Self {
        Self {
            queue_timeout: policy.queue_timeout,
            head_timeout: policy.head_timeout,
        }
    }

    /// Return the existing admission future without an async wrapper or task.
    /// The future borrows only the supplied capacity; timing values are copied.
    pub fn enter<'a>(
        &self,
        admission: &'a Admission,
    ) -> impl Future<Output = Option<Place>> + Send + 'a + use<'a> {
        admission.enter(self.queue_timeout)
    }

    /// Ask the backend about timing ownership only when a limit is declared.
    /// A backend enforcing that limit itself must not be timed a second time.
    pub fn head_timeout_for(
        &self,
        owns_timeout: impl FnOnce() -> bool,
    ) -> Option<Duration> {
        self.head_timeout.filter(|_| !owns_timeout())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/admission.rs"]
mod tests;
