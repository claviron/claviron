use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use hickory_resolver::TokioResolver;
use hickory_resolver::config::{
    ConnectionConfig, NameServerConfig, ResolverConfig as DriverConfig,
};
use hickory_resolver::lookup::Lookup;
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::net::{DnsError, NetError};
use hickory_resolver::proto::op::ResponseCode;
use hickory_resolver::proto::rr::{Name, RData, RecordType as DriverType};

use crate::{
    Answer, Query, Record, RecordType, ResolveError, ResolveErrorKind,
    ResolveFuture, Resolver, ResolverConfig, ResolverFactory,
};

/// Opens a [`HickoryResolver`] for each consumer configuration. An
/// application binds this as `dyn ResolverFactory` when it chooses the
/// built-in driver; nothing binds it implicitly.
#[derive(Clone, Copy, Debug, Default)]
pub struct HickoryFactory;

impl ResolverFactory for HickoryFactory {
    fn open(
        &self,
        config: &ResolverConfig,
    ) -> Result<Arc<dyn Resolver>, ResolveError> {
        Ok(Arc::new(HickoryResolver::new(config)?))
    }
}

/// Built-in DNS adapter. Clones share the same Hickory resolver/cache.
/// Construct once in the owning consumer; use lookup on a live Tokio runtime.
#[derive(Clone, Debug)]
pub struct HickoryResolver {
    inner: TokioResolver,
    boundary: Arc<CacheBoundary>,
}
#[derive(Debug)]
struct CacheBoundary {
    // Serialize cache lookup/insertion across clones. No synchronous lock is
    // held across await; canceled callers release this async admission guard.
    lookup: tokio::sync::Mutex<()>,
    state: Mutex<Deadlines>,
    capacity: u64,
    negative_ttl: (std::time::Duration, std::time::Duration),
}
#[derive(Debug, Default)]
struct Deadlines {
    epoch: u64,
    entries: BTreeMap<(String, DriverType), Instant>,
}
impl Deadlines {
    fn remember(
        &mut self,
        key: (String, DriverType),
        returned: Instant,
        previous: Option<Instant>,
        capacity: u64,
    ) -> Instant {
        let deadline = previous.map_or(returned, |old| old.min(returned));
        if capacity > 0 {
            if self.entries.len() as u64 >= capacity
                && !self.entries.contains_key(&key)
                && let Some(oldest) = self
                    .entries
                    .iter()
                    .min_by_key(|(_, expiry)| **expiry)
                    .map(|(key, _)| key.clone())
            {
                self.entries.remove(&oldest);
            }
            self.entries.insert(key, deadline);
        }
        deadline
    }
}
impl HickoryResolver {
    pub fn new(config: &ResolverConfig) -> Result<Self, ResolveError> {
        config.validate()?;
        let mut builder = if config.nameservers.is_empty() {
            TokioResolver::builder_tokio().map_err(backend_error)?
        } else {
            TokioResolver::builder_with_config(
                driver_config(config)?,
                TokioRuntimeProvider::default(),
            )
        };
        let options = builder.options_mut();
        options.timeout = config.timeout;
        options.attempts = config.attempts;
        options.cache_size = config.cache_size;
        options.positive_min_ttl = Some(config.min_ttl);
        options.negative_min_ttl = Some(config.min_ttl);
        options.positive_max_ttl = Some(config.max_ttl);
        options.negative_max_ttl = Some(config.max_ttl);
        Ok(Self {
            inner: builder.build().map_err(backend_error)?,
            boundary: Arc::new(CacheBoundary {
                lookup: tokio::sync::Mutex::new(()),
                state: Mutex::new(Deadlines::default()),
                capacity: config.cache_size,
                negative_ttl: (config.min_ttl, config.max_ttl),
            }),
        })
    }
    pub fn clear_cache(&self) {
        let mut state =
            self.boundary.state.lock().unwrap_or_else(|p| p.into_inner());
        state.epoch += 1;
        state.entries.clear();
        self.inner.clear_cache();
    }
}
impl Resolver for HickoryResolver {
    fn lookup<'a>(
        &'a self,
        query: Query<'a>,
    ) -> ResolveFuture<'a> {
        Box::pin(async move {
            let name = Name::from_utf8(query.name).map_err(|err| {
                ResolveError::with_source(ResolveErrorKind::InvalidName, err)
            })?;
            let record_type = driver_type(query.record_type);
            let key = (name.to_lowercase().to_utf8(), record_type);
            let _admission = self.boundary.lookup.lock().await;
            let (epoch, previous) = {
                let mut state = self
                    .boundary
                    .state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                let now = Instant::now();
                state.entries.retain(|_, expiry| *expiry > now);
                let previous = state.entries.get(&key).copied();
                // Hickory 0.26 reconstructs cached Lookup expiry from integer
                // remaining TTL, which may grant a fractional fresh second.
                // Unknown/expired provenance must fetch again, not adopt that
                // reconstructed deadline as an original cache insertion.
                if previous.is_none() {
                    self.inner.clear_lookup_cache(name.clone(), record_type);
                }
                (state.epoch, previous)
            };
            let started = Instant::now();
            let lookup = match self.inner.lookup(name, record_type).await {
                Ok(lookup) => lookup,
                Err(error) => {
                    if let NetError::Dns(DnsError::NoRecordsFound(records)) =
                        &error
                    {
                        let (min, max) = self.boundary.negative_ttl;
                        let ttl = records
                            .negative_ttl
                            .map(|ttl| {
                                std::time::Duration::from_secs(ttl.into())
                            })
                            .unwrap_or(min)
                            .clamp(min, max);
                        let mut state = self
                            .boundary
                            .state
                            .lock()
                            .unwrap_or_else(|p| p.into_inner());
                        if state.epoch == epoch {
                            state.remember(
                                key,
                                started + ttl,
                                previous,
                                self.boundary.capacity,
                            );
                        }
                    }
                    return Err(backend_error(error));
                }
            };
            let mut answer = answer(&lookup, query.record_type);
            answer.valid_until = previous
                .map_or(answer.valid_until, |old| old.min(answer.valid_until));
            let mut state =
                self.boundary.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.epoch == epoch {
                answer.valid_until = state.remember(
                    key,
                    answer.valid_until,
                    previous,
                    self.boundary.capacity,
                );
            }
            Ok(answer)
        })
    }
}
fn driver_config(
    config: &ResolverConfig
) -> Result<DriverConfig, ResolveError> {
    let mut driver = DriverConfig::default();
    for addr in &config.nameservers {
        if let std::net::SocketAddr::V6(v6) = addr
            && (v6.scope_id() != 0 || v6.flowinfo() != 0)
        {
            return Err(ResolveError::config(
                "Hickory nameservers do not support IPv6 scope_id or flowinfo",
            ));
        }
        let mut connections =
            vec![ConnectionConfig::udp(), ConnectionConfig::tcp()];
        for connection in &mut connections {
            connection.port = addr.port();
        }
        driver.name_servers.push(NameServerConfig::new(
            addr.ip(),
            true,
            connections,
        ));
    }
    Ok(driver)
}
fn driver_type(record_type: RecordType) -> DriverType {
    match record_type {
        RecordType::A => DriverType::A,
        RecordType::Aaaa => DriverType::AAAA,
        RecordType::Srv => DriverType::SRV,
        RecordType::Txt => DriverType::TXT,
    }
}
fn answer(
    lookup: &Lookup,
    requested: RecordType,
) -> Answer {
    let records = lookup
        .answers()
        .iter()
        .filter_map(|record| match (&record.data, requested) {
            (RData::A(addr), RecordType::A) => Some(Record::A(addr.0)),
            (RData::AAAA(addr), RecordType::Aaaa) => Some(Record::Aaaa(addr.0)),
            (RData::SRV(srv), RecordType::Srv) => Some(Record::Srv {
                priority: srv.priority,
                weight: srv.weight,
                port: srv.port,
                target: srv.target.to_utf8(),
            }),
            (RData::TXT(txt), RecordType::Txt) => Some(Record::Txt(
                txt.txt_data.iter().map(|part| part.to_vec()).collect(),
            )),
            _ => None,
        })
        .collect();
    Answer { records, valid_until: lookup.valid_until() }
}
fn backend_error(error: NetError) -> ResolveError {
    let kind = match &error {
        NetError::Dns(DnsError::NoRecordsFound(records)) => {
            match records.response_code {
                ResponseCode::NXDomain => ResolveErrorKind::NxDomain,
                ResponseCode::NoError => ResolveErrorKind::NoRecords,
                _ => ResolveErrorKind::Backend,
            }
        }
        NetError::Timeout => ResolveErrorKind::Timeout,
        _ => ResolveErrorKind::Backend,
    };
    ResolveError::with_source(kind, error)
}

#[cfg(test)]
#[path = "../tests/unit/hickory.rs"]
mod tests;
