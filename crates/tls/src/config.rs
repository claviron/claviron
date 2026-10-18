use std::path::PathBuf;
use std::time::Duration;

use claviron_helpers::de::deserialize_duration;
use claviron_types::ssl::certstore::{
    CertEntry, CertStore, CertificateSource, default_certificate_source,
    default_certstore_path,
};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(untagged)]
#[allow(
    clippy::large_enum_variant,
    reason = "Cold boot/reload declaration; keep inline configuration without an extra box"
)]
enum CertStoreConfig {
    Path(PathBuf),
    Inline(CertStore),
}

impl Default for CertStoreConfig {
    fn default() -> Self {
        Self::Path(default_certstore_path())
    }
}

/// `tls.yaml` resolved: the certstore it names is loaded. Built from the
/// prepared [`TlsConfigSource`] by `TryFrom`, under a blocking boundary,
/// because loading the store is file IO the reader must not do.
pub struct TlsConfig {
    certstore: CertStore,
    default_cert: CertificateSource,
    sni_entries: Vec<CertEntry>,
    sessions: TlsSessionConfig,
    /// What this server will negotiate. Absent keeps rustls' safe defaults.
    pub policy: crate::policy::TlsPolicy,
}

/// `tls.yaml` as written: what Congif prepares, compares and publishes.
/// Resolving it — loading the certstore it names — may still fail, which is
/// why the declaration and [`TlsConfig`] are two types.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct TlsConfigSource {
    /// Omission and null both select the shared type crate's default YAML path.
    #[serde(default, alias = "cert_store")]
    certstore: Option<CertStoreConfig>,
    /// Omission and YAML null both select the conventional `default` identity.
    #[serde(default, alias = "default")]
    default_cert: Option<CertificateSource>,
    #[serde(
        default,
        alias = "sni_certs",
        alias = "certs",
        alias = "certificates",
        alias = "hosts",
        alias = "servers"
    )]
    sni_entries: Vec<CertEntry>,
    #[serde(default)]
    sessions: TlsSessionConfig,
    #[serde(default)]
    policy: crate::policy::TlsPolicy,
}

impl TryFrom<TlsConfigSource> for TlsConfig {
    type Error = String;

    fn try_from(source: TlsConfigSource) -> Result<Self, Self::Error> {
        let certstore = match source.certstore.unwrap_or_default() {
            CertStoreConfig::Path(path) => CertStore::from_path(&path)
                .map_err(|error| {
                    format!("certstore {}: {error}", path.display())
                })?,
            CertStoreConfig::Inline(certstore) => certstore,
        };
        Ok(Self {
            certstore,
            default_cert: source
                .default_cert
                .unwrap_or_else(default_certificate_source),
            sni_entries: source.sni_entries,
            sessions: source.sessions,
            policy: source.policy,
        })
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct TlsSessionConfig {
    #[serde(
        default = "default_session_ttl",
        deserialize_with = "deserialize_duration"
    )]
    ttl: Duration,
    #[serde(default = "default_session_cleanup_on_exit", alias = "cleanup")]
    cleanup_on_exit: bool,
}

impl Default for TlsSessionConfig {
    fn default() -> Self {
        Self {
            ttl: default_session_ttl(),
            cleanup_on_exit: default_session_cleanup_on_exit(),
        }
    }
}

fn default_session_ttl() -> Duration {
    Duration::from_secs(24 * 60 * 60)
}

fn default_session_cleanup_on_exit() -> bool {
    true
}

impl TlsConfig {
    pub fn default_cert_entry(&self) -> &CertificateSource {
        &self.default_cert
    }

    pub fn certstore(&self) -> &CertStore {
        &self.certstore
    }

    pub fn sni_entries(&self) -> &[CertEntry] {
        &self.sni_entries
    }

    pub fn sessions(&self) -> &TlsSessionConfig {
        &self.sessions
    }
}

impl TlsSessionConfig {
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Whether application exit invalidates previously issued sessions.
    /// This lifecycle policy is not hot-reloadable.
    pub fn cleanup_on_exit(&self) -> bool {
        self.cleanup_on_exit
    }
}
