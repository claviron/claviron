//! Name/authority/TLS identity is prepared before addresses exist.
use std::net::IpAddr;
use std::sync::Arc;

use http::uri::Authority;
use url::Url;

use super::*;
use crate::config::origin::OriginConfig;
use crate::config::profile::ProfileConfig;
use crate::origin::ReservedOriginId;
use crate::resolve::Name;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Named {
    pub(super) name: ResolutionName,
    pub(super) source: Arc<str>,
    pub(super) authority: Authority,
    pub(super) tls: bool,
    pub(super) tls_name: Option<rustls::pki_types::ServerName<'static>>,
}
impl Named {
    pub(super) fn prepare(
        profile: &str,
        config: &ProfileConfig,
        origin: &OriginConfig,
        first: usize,
    ) -> Result<Option<Self>, ConfigError> {
        let invalid = |detail: String| ConfigError::InvalidOrigin {
            profile: profile.into(),
            origin: origin.target().into(),
            detail,
        };
        let url = Url::parse(origin.target())
            .map_err(|error| invalid(error.to_string()))?;
        if !matches!(url.scheme(), "tcp" | "tls" | "srv") {
            return Ok(None);
        }
        let host =
            url.host_str().ok_or_else(|| invalid("missing host".into()))?;
        if url.scheme() != "srv"
            && host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<IpAddr>()
                .is_ok()
        {
            return Ok(None);
        }
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !matches!(url.path(), "" | "/")
        {
            return Err(invalid(
                "named target cannot contain userinfo, path, query or fragment"
                    .into(),
            ));
        }
        let (source, default_authority) = if url.scheme() == "srv" {
            if url.port().is_some() || origin.weight() != 1 {
                return Err(invalid(
                    "SRV supplies its port and weight; declaration cannot override them".into()
                ));
            }
            (Name::Srv { name: host.to_owned() }, None)
        } else {
            let port =
                url.port().filter(|port| *port != 0).ok_or_else(|| {
                    ConfigError::MissingPort {
                        profile: profile.into(),
                        origin: origin.target().into(),
                    }
                })?;
            if origin.weight() == 0 {
                return Err(invalid("weight must be greater than zero".into()));
            }
            (
                Name::Host { name: host.into(), port, weight: origin.weight() },
                Some(format!("{host}:{port}")),
            )
        };
        let authority = origin
            .authority()
            .or(config.authority.as_deref())
            .or(default_authority.as_deref())
            .ok_or_else(|| {
                invalid("SRV requires explicit HTTP authority".into())
            })?
            .parse::<Authority>()
            .map_err(|error| invalid(error.to_string()))?;
        let tls = url.scheme() == "tls";
        let tls_name = tls
            .then(|| {
                crate::compile::tls::server_name(
                    profile,
                    &config.tls,
                    &authority,
                )
            })
            .transpose()?;
        let first = u16::try_from(first).map_err(|_| {
            invalid("named reservations exceed ReservedOriginId".into())
        })?;
        Ok(Some(Self {
            name: ResolutionName {
                label: origin.name().unwrap_or(origin.target()).into(),
                source,
                first: ReservedOriginId(first),
            },
            source: origin.target().into(),
            authority,
            tls,
            tls_name,
        }))
    }
}
