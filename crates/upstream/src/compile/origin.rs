//! Owns target parsing and prepared origin/header/lock construction.
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use claviron_orbit::lock::LockKey;
use http::HeaderValue;
use http::uri::Authority;
use url::Url;

use crate::config::error::ConfigError;
use crate::config::origin::OriginConfig;
use crate::health::lock::{
    UpstreamActiveProbe, UpstreamDownProbe, UpstreamL7Probe, UpstreamOutlier,
    UpstreamRecoveryProbe,
};
use crate::origin::{ConnectTarget, Origin, OriginId};
pub(crate) fn compile_origin(
    profile: &str,
    origin: &OriginConfig,
    profile_authority: Option<&str>,
    id: OriginId,
) -> Result<Origin, ConfigError> {
    let source: Arc<str> = origin.target().into();
    let invalid = |detail: String| ConfigError::InvalidOrigin {
        profile: profile.into(),
        origin: source.to_string(),
        detail,
    };
    let url = Url::parse(&source).map_err(|err| invalid(err.to_string()))?;
    let (connect, default_authority) = match url.scheme() {
        "tcp" | "tls" => {
            let host =
                url.host_str().ok_or_else(|| invalid("missing host".into()))?;
            // URL renders IPv6 hosts with brackets; SocketAddr and the HTTP authority remain distinct.
            let ip = host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<IpAddr>()
                .map_err(|_| ConfigError::DnsOriginRejected {
                    profile: profile.into(),
                    origin: source.to_string(),
                })?;
            let port = url.port().ok_or_else(|| ConfigError::MissingPort {
                profile: profile.into(),
                origin: source.to_string(),
            })?;
            let addr = SocketAddr::new(ip, port);
            let connect = ConnectTarget::Tcp(addr);
            let connect = if url.scheme() == "tls" {
                ConnectTarget::Tls(addr)
            } else {
                connect
            };
            (connect, Some(addr.to_string()))
        }
        "unix" => {
            if url.path().is_empty() {
                return Err(invalid("missing unix socket path".into()));
            }
            (ConnectTarget::Unix(PathBuf::from(url.path())), None)
        }
        scheme => {
            return Err(ConfigError::UnsupportedScheme {
                profile: profile.into(),
                scheme: scheme.into(),
            });
        }
    };
    let authority = origin
        .authority()
        .or(profile_authority)
        .or(default_authority.as_deref())
        .ok_or_else(|| ConfigError::MissingAuthority {
            profile: profile.into(),
            origin: source.to_string(),
        })?;
    let authority = authority
        .parse::<Authority>()
        .map_err(|err| invalid(err.to_string()))?;
    if origin.weight() == 0 {
        return Err(invalid("weight must be greater than zero".into()));
    }
    if origin.max_live() == Some(0) {
        return Err(invalid("max_live must be greater than zero".into()));
    }
    let label: Arc<str> =
        origin.name().map(Arc::from).unwrap_or_else(|| source.clone());
    let state_key = crate::state::origin_key(profile, &label, &source)
        .map_err(|error| invalid(error.to_string()))?;
    let mut prepared = prepare_origin(
        source.clone(),
        label,
        authority,
        state_key,
        connect,
        origin.weight(),
        id,
    )
    .map_err(|err| invalid(err.to_string()))?;
    prepared.max_live = origin.max_live();
    Ok(prepared)
}

/// Shared concrete construction. Named callers supply address-derived bounded
/// keys only after receiving a real socket; literal key identity stays unchanged.
pub(crate) fn prepare_origin(
    source: Arc<str>,
    label: Arc<str>,
    authority: Authority,
    state_key: Arc<str>,
    connect: ConnectTarget,
    weight: u32,
    id: OriginId,
) -> Result<Origin, http::header::InvalidHeaderValue> {
    let host = HeaderValue::from_str(authority.as_str())?;
    let lock_value = bytes::Bytes::copy_from_slice(state_key.as_bytes());
    Ok(Origin {
        id,
        label,
        authority,
        host,
        source,
        state_key,
        slot: crate::state::StateSlot::default(),
        tls_name: None,
        connect,
        weight,
        max_live: None,
        priority: 0,
        lock: [
            LockKey::new::<UpstreamDownProbe>(lock_value.clone()),
            LockKey::new::<UpstreamRecoveryProbe>(lock_value.clone()),
            LockKey::new::<UpstreamActiveProbe>(lock_value.clone()),
            LockKey::new::<UpstreamL7Probe>(lock_value.clone()),
            LockKey::new::<UpstreamOutlier>(lock_value.clone()),
        ],
        metrics: crate::metrics::OriginMetrics::new(id.0),
    })
}
