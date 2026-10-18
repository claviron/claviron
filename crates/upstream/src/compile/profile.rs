//! Owns the compiled profile and its capacity. Every other compiled value
//! lives beside the code that produces it —
//! `tls::Prepared`, `l7::Prepared`, `forward::Prepared`, `retry::Prepared` —
//! and `Profile`, which carries all of them, is no exception.

use super::origin::compile_origin;
use crate::compile::Validation;
use crate::config::UpstreamYaml;
use crate::config::error::ConfigError;
use crate::config::profile::UpstreamProtocol;
use crate::config::profile::capacity::CapacityConfig;
use crate::config::profile::connection::ConnectionConfig;
use crate::config::profile::health::{
    DropProbeConfig, HealthConfig, HealthOverride,
};
use crate::origin::Origin;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Profile {
    pub name: String,
    pub protocol: UpstreamProtocol,
    pub origins: std::sync::Arc<[Origin]>,
    pub capacity: Capacity,
    pub connection: ConnectionConfig,
    pub health: HealthConfig,
    pub(crate) l7_probe: Option<std::sync::Arc<crate::compile::l7::Prepared>>,
    pub(crate) connector: Option<std::sync::Arc<crate::compile::tls::Prepared>>,
    pub tls: crate::config::profile::tls::TlsConfig,
    pub(crate) balancer: std::sync::Arc<crate::proxy::balance::Prepared>,
    pub balance: crate::config::profile::balance::BalanceConfig,
    pub(crate) forwarding: crate::compile::forward::Prepared,
    pub forward: crate::config::profile::forward::ForwardConfig,
    pub(crate) retrying: Option<crate::compile::retry::Prepared>,
    pub retry: crate::config::profile::retry::RetryConfig,
    pub requests: claviron_http::policy::RequestPolicyOverride,
    pub responses: claviron_http::policy::ResponsePolicyOverride,
    pub outlier: crate::config::profile::outlier::OutlierConfig,
    pub breaker: crate::config::profile::breaker::BreakerConfig,
    pub via: http::HeaderValue,
    pub lifecycle: crate::config::profile::lifecycle::LifecycleConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Capacity {
    H1 {
        max_live: usize,
        min_idle: usize,
        max_idle: usize,
        replenish: crate::config::profile::capacity::ReplenishConfig,
    },
    H2 {
        max_streams: usize,
    },
}
impl Capacity {
    pub fn min_idle(&self) -> usize {
        match self {
            Self::H1 { min_idle, .. } => *min_idle,
            Self::H2 { .. } => 0,
        }
    }

    pub fn max_live(&self) -> usize {
        match *self {
            Self::H1 { max_live, .. } => max_live,
            Self::H2 { max_streams } => max_streams,
        }
    }
    pub fn max_streams(&self) -> usize {
        match *self {
            Self::H1 { .. } => 1,
            Self::H2 { max_streams } => max_streams,
        }
    }
    pub fn max_idle(&self) -> usize {
        match *self {
            Self::H1 { max_idle, .. } => max_idle,
            Self::H2 { .. } => 0,
        }
    }
}

impl CapacityConfig {
    pub fn compile(
        &self,
        protocol: UpstreamProtocol,
        _worker_count: Option<usize>,
    ) -> Result<Capacity, ConfigError> {
        if self.max_live == 0 {
            return Err(ConfigError::ZeroMaxLive);
        }

        let max_idle = self.max_idle.unwrap_or(self.max_live);
        let invalid =
            |field, detail| ConfigError::InvalidCapacity { field, detail };
        if protocol == UpstreamProtocol::H1 {
            if self.max_streams.is_some() {
                return Err(invalid(
                    "max_streams",
                    "h1 carries one request per connection; `max_live` is its whole capacity",
                ));
            }
            if self.min_idle > max_idle || max_idle > self.max_live {
                return Err(invalid(
                    "min_idle/max_idle/max_live",
                    "must satisfy min_idle <= max_idle <= max_live",
                ));
            }
            if self.min_idle > 0
                && (self.replenish.initial_backoff.is_zero()
                    || self.replenish.max_backoff.is_zero()
                    || self.replenish.initial_backoff
                        > self.replenish.max_backoff)
            {
                return Err(invalid(
                    "replenish",
                    "enabled warm floor requires 0 < initial_backoff <= max_backoff",
                ));
            }
        } else {
            if self.min_idle > 0 {
                return Err(invalid(
                    "min_idle",
                    "warm connection floor is only supported by h1",
                ));
            }
            if self.max_streams == Some(0) {
                return Err(invalid(
                    "max_streams",
                    "must be greater than zero",
                ));
            }
        }

        Ok(match protocol {
            UpstreamProtocol::H1 => Capacity::H1 {
                max_live: self.max_live,
                min_idle: self.min_idle,
                max_idle,
                replenish: self.replenish.clone(),
            },
            UpstreamProtocol::H2 => Capacity::H2 {
                max_streams: self.max_streams.unwrap_or(self.max_live),
            },
        })
    }
}

impl UpstreamYaml {
    /// Desired DNS preparation supplies concrete literals only. Names remain
    /// typed declarations until materialization; no placeholder socket exists.
    pub(crate) fn compile_profile_with_origins(
        &self,
        name: &str,
        profile: &ProfileConfig,
        validation: &Validation,
        prepared: Option<Vec<crate::origin::Origin>>,
        material: &super::tls::TlsMaterial,
    ) -> Result<Profile, ConfigError> {
        if profile.origins.is_empty() {
            return Err(ConfigError::NoOrigins { profile: name.to_owned() });
        }

        profile.resolve.validate().map_err(|err| {
            ConfigError::InvalidResolve {
                profile: name.into(),
                detail: err.to_string(),
            }
        })?;

        let capacity = profile
            .capacity
            .compile(profile.protocol, validation.healthy_worker_count)?;
        if let Some(limit) = profile.breaker.max_connections
            && capacity.min_idle().saturating_mul(profile.origins.len()) > limit
        {
            return Err(ConfigError::InvalidCapacity {
                field: "breaker.max_connections",
                detail: "must accommodate the configured H1 warm floors across origins",
            });
        }
        if profile.outlier.enabled()
            && (profile.outlier.interval.is_zero()
                || profile.outlier.base_ejection_time
                    < std::time::Duration::from_millis(1)
                || profile.outlier.max_ejection_percent > 100)
        {
            return Err(ConfigError::InvalidCapacity {
                field: "outlier",
                detail: "enabled detection requires interval > 0, base_ejection_time >= 1ms and max_ejection_percent <= 100",
            });
        }
        if profile.timeouts.is_some() {
            return Err(ConfigError::InvalidCapacity {
                field: "timeouts",
                detail: "timeouts is gone: acquire is requests.queue_timeout, connect is connection.connect_timeout, response_header is requests.head_timeout, request_body_read_idle is requests.body_read_timeout, response_body_idle is responses.body_idle_timeout",
            });
        }
        // Decided by every profile; the library does not pick a number.
        if matches!(
            profile.connection.connect_timeout,
            crate::config::overrides::Override::Inherit
        ) {
            return Err(ConfigError::UndeclaredConnectTimeout {
                profile: name.to_owned(),
            });
        }
        let connection = profile.connection.apply(&ConnectionConfig::default());
        let health = compile_health(profile.protocol, &profile.health);
        validate_health(name, &health)?;
        let origins = if let Some(origins) = prepared {
            origins
        } else {
            let mut origins = Vec::with_capacity(profile.origins.len());
            for (index, origin) in profile.origins.iter().enumerate() {
                origins.push(compile_origin(
                    name,
                    origin,
                    profile.authority.as_deref(),
                    crate::origin::OriginId(u16::try_from(index).map_err(
                        |_| ConfigError::InvalidOrigin {
                            profile: name.into(),
                            origin: origin.target().into(),
                            detail: "too many origins for OriginId".into(),
                        },
                    )?),
                )?);
            }
            origins
        };

        let mut origins = origins;
        let connector = super::tls::compile(
            name,
            &profile.tls,
            profile.protocol,
            &mut origins,
            material,
        )?;
        let l7_probe = super::l7::Prepared::compile(
            name,
            &health.l7,
            profile.protocol,
            &origins,
        )?;
        let balancer = crate::proxy::balance::Prepared::compile(
            name,
            &profile.balance,
            &origins,
        )?;
        Ok(Profile {
            name: name.to_owned(),
            protocol: profile.protocol,
            origins: origins.into(),
            capacity,
            connection,
            health,
            l7_probe,
            tls: profile.tls.clone(),
            connector,
            balancer,
            balance: profile.balance.clone(),
            forwarding: super::forward::Prepared::compile(
                name,
                &profile.forward,
            )?,
            forward: profile.forward.clone(),
            retrying: super::retry::Prepared::compile(name, &profile.retry)?,
            retry: profile.retry.clone(),
            requests: profile.requests,
            responses: profile.responses,
            outlier: profile.outlier.clone(),
            breaker: profile.breaker.clone(),
            lifecycle: profile.lifecycle.clone(),
            via: http::HeaderValue::from_static("1.1 claviron"),
        })
    }
}
/// Build the final `HealthConfig` for a profile from two layers:
///
/// 1. Protocol-aware built-in base. The only protocol-sensitive field is
///    `drop_probe.enabled` — true for H2 (single-session model: a drop is
///    a real reachability signal) and false for H1 (multi-conn pool: a
///    single conn dying is noise, request-path failure counting catches
///    real outages with less probe traffic).
/// 2. Per-profile `health` overlay (sparse — unset fields retain the
///    protocol-aware built-in policy).
fn compile_health(
    protocol: UpstreamProtocol,
    overrides: &HealthOverride,
) -> HealthConfig {
    let base = HealthConfig {
        drop_probe: match protocol {
            UpstreamProtocol::H1 => DropProbeConfig::h1_default(),
            UpstreamProtocol::H2 => DropProbeConfig::h2_default(),
        },
        ..Default::default()
    };
    overrides.apply(&base)
}

use crate::config::profile::ProfileConfig;

fn validate_health(
    name: &str,
    health: &HealthConfig,
) -> Result<(), ConfigError> {
    if health.l7.enabled
        && (health.l7.failure_threshold == 0
            || health.l7.success_threshold == 0
            || health.l7.expect.status[0] < 100
            || health.l7.expect.status[1] > 999
            || health.l7.expect.status[0] > health.l7.expect.status[1])
    {
        return Err(ConfigError::InvalidHealth {
            profile: name.into(),
            field: "l7.thresholds/status",
            detail: "thresholds must be positive and status an ordered valid HTTP range",
        });
    }
    let recovery = &health.recovery_probe;
    let active = &health.active;
    for (field, value, enabled) in [
        (
            "drop_probe.probe_timeout",
            health.drop_probe.probe_timeout,
            health.drop_probe.enabled,
        ),
        (
            "recovery_probe.initial_backoff",
            recovery.initial_backoff,
            recovery.enabled,
        ),
        (
            "recovery_probe.probe_timeout",
            recovery.probe_timeout,
            recovery.enabled,
        ),
        ("recovery_probe.max_ttl", recovery.max_ttl, recovery.enabled),
        ("l7.interval", health.l7.interval, health.l7.enabled),
        ("l7.timeout", health.l7.timeout, health.l7.enabled),
        ("active.interval", active.interval, active.enabled),
        ("active.probe_timeout", active.probe_timeout, active.enabled),
    ] {
        if enabled && value.is_zero() {
            return Err(ConfigError::InvalidHealth {
                profile: name.into(),
                field,
                detail: "must be greater than zero",
            });
        }
    }
    // Orbit leases have millisecond deadlines. A sub-millisecond TTL rounds
    // upward in Orbit, so rejecting it here keeps configured TTL and renewal
    // cadence describing the same lifetime; this is a wire-resolution bound.
    if recovery.enabled
        && recovery.max_ttl < std::time::Duration::from_millis(1)
    {
        return Err(ConfigError::InvalidHealth {
            profile: name.into(),
            field: "recovery_probe.max_ttl",
            detail: "must be at least 1ms (Orbit lease resolution)",
        });
    }
    if let Some(max_backoff) = recovery.max_backoff
        && recovery.enabled
    {
        if max_backoff.is_zero() {
            return Err(ConfigError::InvalidHealth {
                profile: name.into(),
                field: "recovery_probe.max_backoff",
                detail: "must be greater than zero, or null for no ceiling",
            });
        }
        if recovery.initial_backoff > max_backoff {
            return Err(ConfigError::InvalidHealth {
                profile: name.into(),
                field: "recovery_probe.initial_backoff",
                detail: "must not exceed max_backoff",
            });
        }
    }
    Ok(())
}
