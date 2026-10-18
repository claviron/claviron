//! One typed PHP cache invalidation event on the existing fleet event ring.
//! No private mmap, per-worker ring, or polling transport lives here.

use claviron_orbit::FleetEventCodec;
use claviron_runtime::events::Event;

/// Borrowed producer-side operation; no cache value is replicated.
#[derive(Clone, Copy, Debug)]
#[cfg(any(feature = "apcu-intercept", feature = "opcache-intercept"))]
pub enum InvalEvent<'a> {
    #[cfg(feature = "apcu-intercept")]
    Delete(&'a str),
    #[cfg(feature = "apcu-intercept")]
    DeletePattern(&'a str),
    #[cfg(feature = "apcu-intercept")]
    ApcuClearCache,
    #[cfg(feature = "opcache-intercept")]
    OpcacheReset,
    #[cfg(feature = "opcache-intercept")]
    OpcacheInvalidate(&'a str, bool),
}

/// Pending peer operation, applied only inside a PHP request context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhpCacheInvalidationKind {
    Delete(String),
    DeletePattern(String),
    ApcuClearCache,
    OpcacheReset,
    OpcacheInvalidate(String, bool),
}

pub(crate) use PhpCacheInvalidationKind as OwnedInvalEvent;

/// A successful local cache operation observed by a worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhpCacheInvalidationObserved {
    pub source_worker: u8,
    pub operation: PhpCacheInvalidationKind,
}

impl Event for PhpCacheInvalidationObserved {}

impl FleetEventCodec for PhpCacheInvalidationObserved {
    // Versioned wire identity; changing the operation encoding needs a new topic.
    const TOPIC: &'static str = "claviron.php.cache-invalidation.observed.v1";

    fn encode_orbit(&self) -> Vec<u8> {
        // Wire v1: source worker, operation tag, then UTF-8 key/file.
        // Tags 5/6 preserve OPcache's force flag without ambiguous key encoding.
        let (tag, payload) = match &self.operation {
            OwnedInvalEvent::Delete(key) => (1, key.as_str()),
            OwnedInvalEvent::DeletePattern(pattern) => (2, pattern.as_str()),
            OwnedInvalEvent::ApcuClearCache => (3, ""),
            OwnedInvalEvent::OpcacheReset => (4, ""),
            OwnedInvalEvent::OpcacheInvalidate(file, false) => {
                (5, file.as_str())
            }
            OwnedInvalEvent::OpcacheInvalidate(file, true) => {
                (6, file.as_str())
            }
        };
        let mut bytes = Vec::with_capacity(2 + payload.len());
        bytes.extend_from_slice(&[self.source_worker, tag]);
        bytes.extend_from_slice(payload.as_bytes());
        bytes
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        let [source_worker, tag, payload @ ..] = bytes else {
            return Err("PHP cache invalidation frame is truncated".into());
        };
        if *source_worker == 0 {
            return Err("PHP cache invalidation source must be a worker".into());
        }
        let payload = std::str::from_utf8(payload).map_err(|_| {
            "PHP cache invalidation payload is not UTF-8".to_string()
        })?;
        let operation =
            match tag {
                1 => OwnedInvalEvent::Delete(payload.into()),
                2 => OwnedInvalEvent::DeletePattern(payload.into()),
                3 if payload.is_empty() => OwnedInvalEvent::ApcuClearCache,
                4 if payload.is_empty() => OwnedInvalEvent::OpcacheReset,
                5 => OwnedInvalEvent::OpcacheInvalidate(payload.into(), false),
                6 => OwnedInvalEvent::OpcacheInvalidate(payload.into(), true),
                _ => return Err(
                    "PHP cache invalidation operation or payload is invalid"
                        .into(),
                ),
            };
        Ok(Self { source_worker: *source_worker, operation })
    }
}

#[cfg(test)]
#[path = "../../tests/invalidation/ipc.rs"]
mod tests;
