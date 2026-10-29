use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::num::{NonZeroU16, NonZeroUsize};
use std::time::Duration;

use claviron_helpers::de::one_or_many;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::proxy::ProxyOverrideExt;
use super::{
    ConfigError, H1Config, H1Override, H2Config, H2Override, HttpPolicy,
    Override, ProxyConfig, ProxyOverride, QuicConfig, QuicOverride,
    RequestPolicy, RequestPolicyOverride, ResponsePolicy,
    ResponsePolicyOverride, TcpConfig, TcpOverride,
};
use crate::ListenerId;
use crate::listener::{HttpProtocol, ListenerTransport};

/// Sparse defaults shared by listener declarations. Nested blocks merge by
/// field; lists replace. Identity, bind and protocol choices never inherit.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenerOverrides {
    #[serde(default)]
    pub max_connections: Override<Option<NonZeroUsize>>,
    #[serde(default, deserialize_with = "super::helpers::optional_duration")]
    pub graceful_timeout: Override<Option<Duration>>,
    #[serde(default, deserialize_with = "super::helpers::duration")]
    pub acquire_timeout: Override<Duration>,
    #[serde(default, deserialize_with = "super::helpers::duration")]
    pub handshake_timeout: Override<Duration>,
    #[serde(default)]
    pub proxy: Override<Option<ProxyOverride>>,
    #[serde(default)]
    pub redirect_port: Override<NonZeroU16>,
    #[serde(default)]
    pub tcp: TcpOverride,
    #[serde(default)]
    pub h1: H1Override,
    #[serde(default)]
    pub h2: H2Override,
    #[serde(default)]
    pub quic: QuicOverride,
    #[serde(default)]
    pub requests: RequestPolicyOverride,
    #[serde(default)]
    pub responses: ResponsePolicyOverride,
}

/// One named listener's declaration. Fields are explicit (not serde flatten)
/// so unknown options are rejected reliably at every nesting level.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenerDefinition {
    pub transport: ListenerTransport,
    #[serde(deserialize_with = "one_or_many")]
    pub listen: Vec<SocketAddr>,
    #[serde(deserialize_with = "one_or_many")]
    pub protocols: Vec<HttpProtocol>,
    #[serde(default)]
    pub tls: Override<bool>,
    #[serde(default)]
    pub acme: bool,
    #[serde(default)]
    pub max_connections: Override<Option<NonZeroUsize>>,
    #[serde(default, deserialize_with = "super::helpers::optional_duration")]
    pub graceful_timeout: Override<Option<Duration>>,
    #[serde(default, deserialize_with = "super::helpers::duration")]
    pub acquire_timeout: Override<Duration>,
    #[serde(default, deserialize_with = "super::helpers::duration")]
    pub handshake_timeout: Override<Duration>,
    #[serde(default)]
    pub proxy: Override<Option<ProxyOverride>>,
    #[serde(default)]
    pub redirect_port: Override<NonZeroU16>,
    #[serde(default)]
    pub tcp: Override<TcpOverride>,
    #[serde(default)]
    pub h1: Override<H1Override>,
    #[serde(default)]
    pub h2: Override<H2Override>,
    #[serde(default)]
    pub quic: Override<QuicOverride>,
    #[serde(default)]
    pub requests: RequestPolicyOverride,
    #[serde(default)]
    pub responses: ResponsePolicyOverride,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionLimits {
    /// Only an explicitly declared or inherited count limits admission.
    /// None is unlimited; no operating-system limit is converted into policy.
    pub max_connections: Option<NonZeroUsize>,
    /// None is indefinite, not a default deadline.
    pub graceful_timeout: Option<Duration>,
    pub acquire_timeout: Duration,
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        Self {
            max_connections: None,
            graceful_timeout: None,
            acquire_timeout: default_acquire_timeout(),
        }
    }
}

/// Socket addresses in this group share one worker-local gate. Other groups
/// never share that gate, even if both use the same driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListenerConfig {
    pub id: ListenerId,
    pub listen: Vec<SocketAddr>,
    pub limits: ConnectionLimits,
    pub proxy: Option<ProxyConfig>,
    /// What this listener accepts of a request body. Outside `settings`
    /// because it is the same question for TCP and QUIC, and answering it per
    /// transport is what left HTTP/1.1 and HTTP/2 with no answer at all.
    pub requests: RequestPolicy,
    /// How this listener hands a reply back, for every host on it.
    pub responses: ResponsePolicy,
    pub settings: ListenerSettings,
}

impl ListenerConfig {
    /// Both sides of this listener's policy: the guarantee every host and
    /// application on it narrows.
    pub fn policy(&self) -> HttpPolicy {
        HttpPolicy { requests: self.requests, responses: self.responses }
    }
}

impl ListenerConfig {
    pub fn transport(&self) -> ListenerTransport {
        match self.settings {
            ListenerSettings::Tcp(_) => ListenerTransport::Tcp,
            ListenerSettings::Quic(_) => ListenerTransport::Quic,
        }
    }

    /// Whether a running listener generation may adopt `other` without
    /// rebinding sockets or replacing its admission gate.
    pub fn same_binding(
        &self,
        other: &Self,
    ) -> bool {
        let same_driver = match (&self.settings, &other.settings) {
            (ListenerSettings::Tcp(a), ListenerSettings::Tcp(b)) => matches!(
                (&a.security, &b.security),
                (TcpSecurity::Cleartext { .. }, TcpSecurity::Cleartext { .. })
                    | (TcpSecurity::Tls { .. }, TcpSecurity::Tls { .. })
            ),
            (ListenerSettings::Quic(_), ListenerSettings::Quic(_)) => true,
            _ => false,
        };
        same_driver
            && self.id == other.id
            && self.listen == other.listen
            && self.limits == other.limits
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListenerSettings {
    Tcp(TcpListenerConfig),
    Quic(QuicListenerConfig),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TcpSecurity {
    Cleartext { redirect_port: NonZeroU16, acme: bool },
    Tls { handshake_timeout: Duration },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpListenerConfig {
    pub security: TcpSecurity,
    pub protocols: Vec<HttpProtocol>,
    pub tcp: TcpConfig,
    pub h1: H1Config,
    pub h2: H2Config,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuicListenerConfig {
    pub handshake_timeout: Duration,
    pub quic: QuicConfig,
}

impl ListenerOverrides {
    fn limits(
        &self,
        base: &ConnectionLimits,
    ) -> ConnectionLimits {
        ConnectionLimits {
            max_connections: self
                .max_connections
                .resolve(&base.max_connections),
            graceful_timeout: self
                .graceful_timeout
                .resolve(&base.graceful_timeout),
            acquire_timeout: self
                .acquire_timeout
                .resolve(&base.acquire_timeout),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        self.tcp.apply(&TcpConfig::default()).accept_retry.validate()?;
        super::tcp::validate(
            &self.h1.apply(&H1Config::default()),
            &self.h2.apply(&H2Config::default()),
        )?;
        super::quic::validate(&self.quic.apply(&QuicConfig::default()))?;
        if matches!(self.handshake_timeout, Override::Value(value) if value.is_zero())
        {
            return Err(ConfigError::global(
                "handshake_timeout must be positive",
            ));
        }
        if self
            .tcp
            .apply(&TcpConfig::default())
            .keepalive
            .is_some_and(|value| value.is_zero())
        {
            return Err(ConfigError::global(
                "tcp.keepalive must be positive or null",
            ));
        }
        Ok(())
    }
}

impl ListenerDefinition {
    fn overrides(&self) -> ListenerOverrides {
        ListenerOverrides {
            max_connections: self.max_connections,
            graceful_timeout: self.graceful_timeout,
            acquire_timeout: self.acquire_timeout,
            handshake_timeout: self.handshake_timeout,
            proxy: self.proxy.clone(),
            redirect_port: self.redirect_port,
            tcp: self.tcp.resolve(&TcpOverride::default()),
            h1: self.h1.resolve(&H1Override::default()),
            h2: self.h2.resolve(&H2Override::default()),
            quic: self.quic.resolve(&QuicOverride::default()),
            requests: self.requests,
            responses: self.responses,
        }
    }

    pub(crate) fn resolve(
        self,
        id: ListenerId,
        defaults: &ListenerOverrides,
    ) -> Result<ListenerConfig, ConfigError> {
        let fail = |message: &str| ConfigError::listener(&id, message);
        if self.listen.is_empty() {
            return Err(fail(
                "listen must contain at least one socket address",
            ));
        }
        for (i, addr) in self.listen.iter().enumerate() {
            if self.listen[..i].contains(addr) {
                return Err(fail("listen contains duplicate addresses"));
            }
        }
        if self.protocols.is_empty() {
            return Err(fail("protocols must not be empty"));
        }
        for (i, proto) in self.protocols.iter().enumerate() {
            if self.protocols[..i].contains(proto) {
                return Err(fail("protocols contains duplicate entries"));
            }
        }
        let options = self.overrides();
        let limits =
            options.limits(&defaults.limits(&ConnectionLimits::default()));
        let proxy =
            options.proxy.resolve_proxy(defaults.proxy.resolve_proxy(None));
        let settings = match self.transport {
            ListenerTransport::Tcp => {
                if !self.quic.is_inherit() {
                    return Err(fail(
                        "TCP listeners cannot contain quic options",
                    ));
                }
                if self.protocols.contains(&HttpProtocol::H3) {
                    return Err(fail("TCP listeners support only h1/h2"));
                }
                let tls = self.tls.resolve(&false);
                if !tls && self.protocols != [HttpProtocol::H1] {
                    return Err(fail(
                        "cleartext TCP currently supports only h1; prior-knowledge h2 is not implemented",
                    ));
                }
                if !self.protocols.contains(&HttpProtocol::H1)
                    && !self.h1.is_inherit()
                {
                    return Err(fail("h1 options require h1 in protocols"));
                }
                if !self.protocols.contains(&HttpProtocol::H2)
                    && !self.h2.is_inherit()
                {
                    return Err(fail("h2 options require h2 in protocols"));
                }
                let tcp = options
                    .tcp
                    .apply(&defaults.tcp.apply(&TcpConfig::default()));
                tcp.accept_retry
                    .validate()
                    .map_err(|error| fail(&error.message))?;
                let h1 =
                    options.h1.apply(&defaults.h1.apply(&H1Config::default()));
                let h2 =
                    options.h2.apply(&defaults.h2.apply(&H2Config::default()));
                super::tcp::validate(&h1, &h2).map_err(|e| fail(&e.message))?;
                if tcp.keepalive.is_some_and(|value| value.is_zero()) {
                    return Err(fail("tcp.keepalive must be positive or null"));
                }
                let security = if tls {
                    if self.acme || !self.redirect_port.is_inherit() {
                        return Err(fail(
                            "HTTP-01 and redirect_port belong to cleartext listeners",
                        ));
                    }
                    let handshake_timeout = options.handshake_timeout.resolve(
                        &defaults
                            .handshake_timeout
                            .resolve(&default_tcp_handshake_timeout()),
                    );
                    if handshake_timeout.is_zero() {
                        return Err(fail("handshake_timeout must be positive"));
                    }
                    TcpSecurity::Tls { handshake_timeout }
                } else {
                    if !self.handshake_timeout.is_inherit() {
                        return Err(fail(
                            "cleartext listeners have no handshake_timeout",
                        ));
                    }
                    if self.acme && !cfg!(feature = "acme") {
                        return Err(fail("HTTP-01 requires the acme feature"));
                    }
                    TcpSecurity::Cleartext {
                        redirect_port: options.redirect_port.resolve(
                            &defaults
                                .redirect_port
                                .resolve(&default_redirect_port()),
                        ),
                        acme: self.acme,
                    }
                };
                ListenerSettings::Tcp(TcpListenerConfig {
                    security,
                    protocols: self.protocols.clone(),
                    tcp,
                    h1,
                    h2,
                })
            }
            ListenerTransport::Quic => {
                if !self.tls.resolve(&true) {
                    return Err(fail("QUIC requires TLS"));
                }
                if self.protocols != [HttpProtocol::H3] {
                    return Err(fail("QUIC listeners support only h3"));
                }
                if !self.tcp.is_inherit()
                    || !self.h1.is_inherit()
                    || !self.h2.is_inherit()
                {
                    return Err(fail(
                        "QUIC listeners cannot contain tcp/h1/h2 options",
                    ));
                }
                if self.acme || !self.redirect_port.is_inherit() {
                    return Err(fail(
                        "HTTP-01 and redirect_port belong to cleartext TCP listeners",
                    ));
                }
                let quic = options
                    .quic
                    .apply(&defaults.quic.apply(&QuicConfig::default()));
                super::quic::validate(&quic).map_err(|e| fail(&e.message))?;
                let handshake_timeout = options.handshake_timeout.resolve(
                    &defaults
                        .handshake_timeout
                        .resolve(&default_quic_handshake_timeout()),
                );
                if handshake_timeout.is_zero() {
                    return Err(fail("handshake_timeout must be positive"));
                }
                ListenerSettings::Quic(QuicListenerConfig {
                    handshake_timeout,
                    quic,
                })
            }
        };
        let requests = RequestPolicy::for_listener(
            &defaults.requests.overlay(&options.requests),
        )
        .map_err(fail)?;

        let responses = ResponsePolicy::for_listener(
            &defaults.responses.overlay(&options.responses),
        );
        Ok(ListenerConfig {
            id,
            listen: self.listen,
            limits,
            proxy,
            requests,
            responses,
            settings,
        })
    }
}

pub(super) fn deserialize_definitions<'de, D: Deserializer<'de>>(
    de: D
) -> Result<BTreeMap<ListenerId, ListenerDefinition>, D::Error> {
    struct Definitions;
    impl<'de> Visitor<'de> for Definitions {
        type Value = BTreeMap<ListenerId, ListenerDefinition>;
        fn expecting(
            &self,
            f: &mut fmt::Formatter<'_>,
        ) -> fmt::Result {
            f.write_str("a map of unique listener identities")
        }
        fn visit_map<M: MapAccess<'de>>(
            self,
            mut map: M,
        ) -> Result<Self::Value, M::Error> {
            let mut definitions = BTreeMap::new();
            while let Some((id, definition)) =
                map.next_entry::<ListenerId, ListenerDefinition>()?
            {
                if definitions.contains_key(&id) {
                    return Err(serde::de::Error::custom(format!(
                        "duplicate listener '{id}'"
                    )));
                }
                definitions.insert(id, definition);
            }
            Ok(definitions)
        }
    }
    de.deserialize_map(Definitions)
}

fn default_acquire_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_tcp_handshake_timeout() -> Duration {
    Duration::from_secs(10)
}
fn default_quic_handshake_timeout() -> Duration {
    Duration::from_secs(5)
}
fn default_redirect_port() -> NonZeroU16 {
    NonZeroU16::new(443).expect("default HTTPS port is nonzero")
}
