//! Cold DNS answer preparation. Fleet publication and service adoption are
//! separate owners: this module never queries during request selection/acquire.
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use claviron_resolver::{
    Query, Record, RecordType, ResolveError, ResolveErrorKind, Resolver,
};

pub use crate::config::profile::resolve::{
    Boot, Family, OnFailure, ResolveConfig,
};
pub use crate::state::assignment::{
    LocalMembershipState, MembershipStateError,
};
pub(crate) mod control;
mod desired;
mod event;
#[cfg(feature = "metrics")]
mod metrics;
pub(crate) mod runnable;
mod status;
pub use desired::materialize::MaterializedProfile;
pub use desired::{DesiredError, DesiredProfile, DesiredUpstreams};
#[cfg(feature = "metrics")]
pub use metrics::QueryCounts;
pub use status::{
    NameResolutionStatus, ProfileResolutionStatus, ServingOriginStatus,
};
mod owner;
mod set;
mod snapshot;
pub use event::ResolutionChanged;
pub use owner::{
    ProfileResolver, ResolutionAuthority, ResolutionName, ResolverError,
    ResolverPlan,
};
pub(crate) mod table;
pub use snapshot::ResolutionSnapshot;
pub use table::{
    PublicationError, PublishedResolution, ResolutionEntry, ResolutionTable,
    ResolutionWriter, ResolverDuty,
};
mod srv;
pub use set::{Assignment, Change, ResolvedSet, Retirement};

/// Compatibility name for the cold resolution API. This is a reserved slot,
/// a different type from the native service's dense origin index.
pub use crate::origin::ReservedOriginId as OriginId;
pub use crate::origin::{ReservedOriginId, ResolutionGeneration};

/// Membership facts retained through SRV target expansion and normalization.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct Address {
    pub socket: SocketAddr,
    pub priority: u16,
    /// Zero is a valid SRV weight and must not be rewritten at resolution time.
    pub weight: u32,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddressAnswer {
    pub addresses: Vec<Address>,
    pub valid_until: Instant,
    pub truncated: bool,
}

/// A name remains separate from a concrete transport address until adoption.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum Name {
    Host { name: String, port: u16, weight: u32 },
    Srv { name: String },
}
impl Name {
    pub fn name(&self) -> &str {
        match self {
            Self::Host { name, .. } | Self::Srv { name } => name,
        }
    }
}

/// Apply one total budget across every family and SRV target query. Dropping
/// this future fences its result; it does not join Hickory's shared driver.
pub async fn lookup(
    resolver: &dyn Resolver,
    name: &Name,
    config: &ResolveConfig,
    budget: Duration,
) -> Result<AddressAnswer, ResolveError> {
    config.validate()?;
    if budget.is_zero() || Instant::now().checked_add(budget).is_none() {
        return Err(ResolveError::new(
            ResolveErrorKind::Config,
            "total DNS budget must be positive and fit the monotonic clock",
        ));
    }
    let operation = async {
        let (addresses, valid_until) = match name {
            Name::Host { name, port, weight } => {
                if *port == 0 || *weight == 0 {
                    return Err(ResolveError::new(
                        ResolveErrorKind::Config,
                        "host port and weight must be positive",
                    ));
                }
                let (ips, expiry) = host(resolver, name, config.family).await?;
                (
                    ips.into_iter()
                        .map(|ip| Address {
                            socket: SocketAddr::new(ip, *port),
                            priority: 0,
                            weight: *weight,
                        })
                        .collect(),
                    expiry,
                )
            }
            Name::Srv { name } => {
                srv::lookup(resolver, name, config.family).await?
            }
        };
        normalize(addresses, valid_until, config.max_addresses)
    };
    tokio::time::timeout(budget, operation).await.map_err(|_| {
        ResolveError::new(
            ResolveErrorKind::Timeout,
            format!("DNS lookup `{}` exceeded the total budget", name.name()),
        )
    })?
}

async fn host(
    resolver: &dyn Resolver,
    name: &str,
    family: Family,
) -> Result<(Vec<std::net::IpAddr>, Instant), ResolveError> {
    let types: &[RecordType] = match family {
        Family::Any => &[RecordType::A, RecordType::Aaaa],
        Family::Ipv4 => &[RecordType::A],
        Family::Ipv6 => &[RecordType::Aaaa],
    };
    let mut ips = Vec::new();
    let mut expiry = None;
    for &record_type in types {
        let answer = match resolver.lookup(Query::new(name, record_type)).await
        {
            Ok(answer) => answer,
            // A missing family is normal; NXDOMAIN and transport failures are
            // failures of this name, never a partially successful shrink.
            Err(error) if error.kind == ResolveErrorKind::NoRecords => continue,
            Err(error) => return Err(error),
        };
        expiry = Some(expiry.map_or(answer.valid_until, |old: Instant| {
            old.min(answer.valid_until)
        }));
        for record in answer.records {
            match (record_type, record) {
                (RecordType::A, Record::A(ip)) => ips.push(ip.into()),
                (RecordType::Aaaa, Record::Aaaa(ip)) => ips.push(ip.into()),
                _ => {}
            }
        }
    }
    if ips.is_empty() {
        return Err(ResolveError::new(
            ResolveErrorKind::NoRecords,
            format!("DNS name `{name}` has no requested addresses"),
        ));
    }
    ips.sort_unstable();
    ips.dedup();
    Ok((ips, expiry.expect("an address requires a positive answer")))
}

fn normalize(
    mut addresses: Vec<Address>,
    valid_until: Instant,
    limit: usize,
) -> Result<AddressAnswer, ResolveError> {
    addresses.sort_unstable();
    let mut normalized: Vec<Address> = Vec::with_capacity(addresses.len());
    for address in addresses {
        if let Some(previous) =
            normalized.last_mut().filter(|old| old.socket == address.socket)
        {
            // Lowest priority wins for a duplicate endpoint. At the same
            // priority, duplicate SRV service weights contribute together.
            if previous.priority == address.priority {
                previous.weight = previous
                    .weight
                    .checked_add(address.weight)
                    .ok_or_else(|| {
                    ResolveError::new(
                        ResolveErrorKind::Backend,
                        "combined SRV weight exceeds u32",
                    )
                })?;
            }
        } else {
            normalized.push(address);
        }
    }
    if normalized.is_empty() {
        return Err(ResolveError::new(
            ResolveErrorKind::NoRecords,
            "DNS returned no service addresses",
        ));
    }
    let truncated = normalized.len() > limit;
    normalized.truncate(limit);
    // Keep remaining cached lifetime exactly: no fresh minimum TTL at read.
    Ok(AddressAnswer { addresses: normalized, valid_until, truncated })
}
