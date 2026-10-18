//! SRV targets expand through the same family policy and total lookup budget.
use std::net::SocketAddr;
use std::time::Instant;

use claviron_resolver::{
    Query, Record, RecordType, ResolveError, ResolveErrorKind, Resolver,
};

use super::{Address, Family, host};

pub(super) async fn lookup(
    resolver: &dyn Resolver,
    name: &str,
    family: Family,
) -> Result<(Vec<Address>, Instant), ResolveError> {
    let answer = resolver.lookup(Query::new(name, RecordType::Srv)).await?;
    let mut expiry = answer.valid_until;
    let mut addresses = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for record in answer.records {
        let Record::Srv { priority, weight, port, target } = record else {
            continue;
        };
        if target == "." {
            return Err(ResolveError::new(
                ResolveErrorKind::NoRecords,
                format!("SRV service `{name}` is unavailable"),
            ));
        }
        if port == 0 {
            return Err(ResolveError::new(
                ResolveErrorKind::Backend,
                "SRV service port must be positive",
            ));
        }
        // DNS is an RRset: repeating the same record (including target case)
        // must not multiply its weight or issue an extra target lookup.
        let target = target.to_ascii_lowercase();
        if !seen.insert((priority, weight, port, target.clone())) {
            continue;
        }
        // Do not publish a partial set when one target fails: that would
        // retire healthy addresses because DNS temporarily could not expand it.
        let (ips, target_expiry) = host(resolver, &target, family).await?;
        expiry = expiry.min(target_expiry);
        addresses.extend(ips.into_iter().map(|ip| Address {
            socket: SocketAddr::new(ip, port),
            priority,
            weight: u32::from(weight),
        }));
    }
    Ok((addresses, expiry))
}
