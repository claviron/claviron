use std::net::IpAddr;

use http::HeaderName;
use ipnet::IpNet;
use serde::{Deserialize, Deserializer};

use super::Override;

/// Resolved listener trust policy. Header names are parsed once at boot.
#[derive(
    Clone, Debug, Default, Eq, PartialEq, claviron_macros::Overridable,
)]
#[overridable(helpers = "crate::config::helpers")]
pub struct ProxyConfig {
    #[overridable(deserialize_with = "headers")]
    pub headers: Vec<HeaderName>,
    pub trusted: Vec<IpNet>,
}

impl ProxyConfig {
    pub fn trusts(
        &self,
        peer: IpAddr,
    ) -> bool {
        self.trusted.iter().any(|net| net.contains(&peer))
    }
}

fn headers<'de, D: Deserializer<'de>>(
    de: D
) -> Result<Override<Vec<HeaderName>>, D::Error> {
    Vec::<String>::deserialize(de)?
        .into_iter()
        .map(|name| name.parse().map_err(serde::de::Error::custom))
        .collect::<Result<Vec<_>, _>>()
        .map(Override::Value)
}

pub(crate) trait ProxyOverrideExt {
    fn resolve_proxy(
        &self,
        inherited: Option<ProxyConfig>,
    ) -> Option<ProxyConfig>;
}

impl ProxyOverrideExt for Override<Option<ProxyOverride>> {
    fn resolve_proxy(
        &self,
        inherited: Option<ProxyConfig>,
    ) -> Option<ProxyConfig> {
        match self {
            Self::Inherit => inherited,
            Self::Value(None) => None,
            Self::Value(Some(value)) => {
                Some(value.apply(&inherited.unwrap_or_default()))
            }
        }
    }
}
