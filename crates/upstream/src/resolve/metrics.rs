//! Only elected owners count completed lookups. Gauges read canonical truth.
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use claviron_orbit::counter::Counter;

use super::{AddressAnswer, PublicationError};

// These are the five exhaustive query outcome classes, not runtime capacity.
const RESULTS: [&str; 5] = ["ok", "nxdomain", "empty", "timeout", "error"];
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct QueryCounts {
    pub ok: u64,
    pub nxdomain: u64,
    pub empty: u64,
    pub timeout: u64,
    pub error: u64,
}
pub(super) enum Counters {
    Local([AtomicU64; 5]),
    Fleet { table: Arc<Counter>, keys: [String; 5] },
}
#[derive(Default)]
pub(crate) struct Store(Mutex<BTreeMap<(String, String), Arc<Counters>>>);
impl Store {
    pub(super) fn prepare(
        &self,
        profile: &str,
        name: &str,
        table: Option<Arc<Counter>>,
    ) -> Result<Arc<Counters>, PublicationError> {
        let key = (profile.to_owned(), name.to_owned());
        let mut store = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(counts) = store.get(&key) {
            return Ok(counts.clone());
        }
        let counters = match table {
            None => Counters::Local(std::array::from_fn(|_| AtomicU64::new(0))),
            Some(table) => {
                let identity = serde_json::to_vec(&key)?;
                let digest = blake3::derive_key(
                    "claviron.upstream.resolve.metrics.v1",
                    &identity,
                );
                let identity = claviron_helpers::base64::base64_encode(&digest);
                let keys = RESULTS.map(|result| {
                    format!("clv:upstream:dns:{identity}:{result}")
                });
                for key in &keys {
                    table.increment(key, 0).map_err(failed)?;
                }
                Counters::Fleet { table, keys }
            }
        };
        let counters = Arc::new(counters);
        store.insert(key, counters.clone());
        Ok(counters)
    }
}
fn failed(error: impl std::fmt::Display) -> PublicationError {
    PublicationError::Io(std::io::Error::other(format!(
        "upstream DNS counter: {error}"
    )))
}
impl Counters {
    pub(super) fn record(
        &self,
        answer: &Result<AddressAnswer, claviron_resolver::ResolveError>,
    ) {
        use claviron_resolver::ResolveErrorKind;
        let index = match answer {
            Ok(_) => 0,
            Err(error) => match error.kind {
                ResolveErrorKind::NxDomain => 1,
                ResolveErrorKind::NoRecords => 2,
                ResolveErrorKind::Timeout => 3,
                _ => 4,
            },
        };
        match self {
            Self::Local(counters) => {
                let _ = counters[index].try_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |v| v.checked_add(1),
                );
            }
            Self::Fleet { table, keys } => {
                if let Err(error) = table.increment(&keys[index], 1) {
                    tracing::error!(%error, "upstream DNS query counter could not increment");
                }
            }
        }
    }
    pub(super) fn snapshot(&self) -> Result<QueryCounts, PublicationError> {
        let mut values = [0; 5];
        match self {
            Self::Local(counters) => {
                for (value, counter) in values.iter_mut().zip(counters) {
                    *value = counter.load(Ordering::Relaxed);
                }
            }
            Self::Fleet { table, keys } => {
                for (value, key) in values.iter_mut().zip(keys) {
                    *value = u64::try_from(
                        table.get(key).map_err(failed)?.unwrap_or(0),
                    )
                    .map_err(failed)?;
                }
            }
        }
        let [ok, nxdomain, empty, timeout, error] = values;
        Ok(QueryCounts { ok, nxdomain, empty, timeout, error })
    }
}
fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}
impl crate::UpstreamProvider {
    pub(crate) fn write_resolution_metrics(
        &self,
        output: &mut String,
    ) -> Result<(), String> {
        for (suffix, kind, help) in [
            (
                "resolution_total",
                "counter",
                "Completed elected DNS lookups by outcome.",
            ),
            (
                "resolved_addresses",
                "gauge",
                "Active authoritative resolved addresses.",
            ),
            ("resolution_stale", "gauge", "Authoritative resolution is stale."),
            (
                "resolution_truncated",
                "gauge",
                "Authoritative answer exceeded its address bound.",
            ),
        ] {
            let _ = writeln!(
                output,
                "# HELP claviron_upstream_{suffix} {help}\n# TYPE claviron_upstream_{suffix} {kind}"
            );
        }
        for profile in self.resolution_status().map_err(|e| e.to_string())? {
            for name in profile.names {
                let labels = format!(
                    "profile=\"{}\",name=\"{}\"",
                    escape(&profile.profile),
                    escape(&name.label)
                );
                let QueryCounts { ok, nxdomain, empty, timeout, error } =
                    name.queries;
                for (result, value) in RESULTS
                    .into_iter()
                    .zip([ok, nxdomain, empty, timeout, error])
                {
                    let _ = writeln!(
                        output,
                        "claviron_upstream_resolution_total{{{labels},result=\"{result}\"}} {value}"
                    );
                }
                for (suffix, value) in [
                    ("resolved_addresses", name.active.len()),
                    ("resolution_stale", usize::from(name.stale)),
                    ("resolution_truncated", usize::from(name.truncated)),
                ] {
                    let _ = writeln!(
                        output,
                        "claviron_upstream_{suffix}{{{labels}}} {value}"
                    );
                }
            }
        }
        Ok(())
    }
}
