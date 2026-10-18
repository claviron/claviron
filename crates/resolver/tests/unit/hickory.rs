use std::error::Error;
use std::time::{Duration, Instant};

use hickory_resolver::net::NoRecords;
use hickory_resolver::proto::op::{Query as DriverQuery, ResponseCode};
use hickory_resolver::proto::rr::Record as DriverRecord;
use hickory_resolver::proto::rr::rdata::{A, AAAA, SRV, TXT};

use super::*;

#[test]
fn cached_deadline_rounding_cannot_extend_first_expiry_and_tracking_is_bounded()
{
    let now = Instant::now();
    let first = now + Duration::from_secs(10);
    let key = ("api.internal.".into(), DriverType::A);
    let mut deadlines = Deadlines::default();
    assert_eq!(deadlines.remember(key.clone(), first, None, 1), first);
    // Hickory's cached response TTL subtracts whole elapsed seconds, then
    // reconstructs now + TTL. Clamp that fractional extension to first expiry.
    assert_eq!(
        deadlines.remember(
            key.clone(),
            first + Duration::from_millis(900),
            Some(first),
            1
        ),
        first
    );
    deadlines.remember(
        ("other.internal.".into(), DriverType::A),
        first,
        None,
        1,
    );
    assert_eq!(deadlines.entries.len(), 1);
    assert!(!deadlines.entries.contains_key(&key));
    deadlines.remember(key, first, None, 0);
    assert_eq!(
        deadlines.entries.len(),
        1,
        "cache_size=0 installs no tracking entry"
    );
}

#[test]
fn answer_conversion_filters_type_and_preserves_remaining_cache_lifetime() {
    let name = Name::from_ascii("api.internal.").unwrap();
    let expiry = Instant::now() + Duration::from_secs(1);
    let records = vec![
        DriverRecord::from_rdata(
            name.clone(),
            30,
            RData::A(A::new(127, 0, 0, 1)),
        ),
        DriverRecord::from_rdata(
            name.clone(),
            30,
            RData::AAAA(AAAA("::1".parse().unwrap())),
        ),
        DriverRecord::from_rdata(
            name.clone(),
            30,
            RData::SRV(SRV::new(10, 7, 3000, name.clone())),
        ),
        DriverRecord::from_rdata(
            name.clone(),
            30,
            RData::TXT(TXT::from_bytes(vec![b"one", &[0xff, 0x00]])),
        ),
        DriverRecord::from_rdata(
            name.clone(),
            30,
            RData::CNAME(hickory_resolver::proto::rr::rdata::CNAME(
                name.clone(),
            )),
        ),
    ];
    let lookup = Lookup::new_with_deadline(
        DriverQuery::query(name, DriverType::A),
        records,
        expiry,
    );
    for (ty, expected) in [
        (RecordType::A, Record::A("127.0.0.1".parse().unwrap())),
        (RecordType::Aaaa, Record::Aaaa("::1".parse().unwrap())),
        (
            RecordType::Srv,
            Record::Srv {
                priority: 10,
                weight: 7,
                port: 3000,
                target: "api.internal.".into(),
            },
        ),
        (RecordType::Txt, Record::Txt(vec![b"one".to_vec(), vec![0xff, 0x00]])),
    ] {
        let result = answer(&lookup, ty);
        assert_eq!(result.records, [expected]);
        assert_eq!(
            result.valid_until, expiry,
            "do not extend cached TTL on read"
        );
    }
}

#[test]
fn errors_distinguish_negative_answers_timeout_and_backend_with_source() {
    let query = DriverQuery::query(
        Name::from_ascii("api.internal.").unwrap(),
        DriverType::A,
    );
    for (error, expected) in [
        (
            NetError::from(NoRecords::new(
                query.clone(),
                ResponseCode::NXDomain,
            )),
            ResolveErrorKind::NxDomain,
        ),
        (
            NetError::from(NoRecords::new(query, ResponseCode::NoError)),
            ResolveErrorKind::NoRecords,
        ),
        (
            NetError::from(NoRecords::new(
                DriverQuery::query(
                    Name::from_ascii("api.internal.").unwrap(),
                    DriverType::A,
                ),
                ResponseCode::ServFail,
            )),
            ResolveErrorKind::Backend,
        ),
        (NetError::Timeout, ResolveErrorKind::Timeout),
        (NetError::NoConnections, ResolveErrorKind::Backend),
    ] {
        let mapped = backend_error(error);
        assert_eq!(mapped.kind, expected);
        assert!(mapped.source().is_some());
    }
}

#[tokio::test]
async fn explicit_nameservers_preserve_ports_and_operational_options_without_queries()
 {
    let config = ResolverConfig {
        nameservers: vec![
            "127.0.0.1:5353".parse().unwrap(),
            "[::1]:5354".parse().unwrap(),
        ],
        timeout: Duration::from_millis(200),
        attempts: 1,
        cache_size: 0,
        min_ttl: Duration::ZERO,
        max_ttl: Duration::from_secs(20),
    };
    let driver = driver_config(&config).unwrap();
    for (index, server) in driver.name_servers.iter().enumerate() {
        assert_eq!(server.ip, config.nameservers[index].ip());
        assert_eq!(server.connections.len(), 2);
        assert!(
            server
                .connections
                .iter()
                .all(|c| c.port == config.nameservers[index].port())
        );
    }
    let resolver = HickoryResolver::new(&config).unwrap();
    let options = resolver.inner.options();
    assert_eq!(options.timeout, config.timeout);
    assert_eq!(options.attempts, config.attempts);
    assert_eq!(options.cache_size, config.cache_size);
    assert_eq!(options.positive_min_ttl, Some(config.min_ttl));
    assert_eq!(options.negative_max_ttl, Some(config.max_ttl));
    let invalid = resolver
        .lookup(Query::new(&"a".repeat(64), RecordType::A))
        .await
        .unwrap_err();
    assert_eq!(invalid.kind, ResolveErrorKind::InvalidName);
    let scoped = ResolverConfig {
        nameservers: vec!["[fe80::1%3]:53".parse().unwrap()],
        ..config
    };
    assert_eq!(
        HickoryResolver::new(&scoped).unwrap_err().kind,
        ResolveErrorKind::Config
    );
}
