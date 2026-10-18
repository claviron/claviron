//! Owns outbound TLS verification and client identity policy.
//!
//! Our own certificates are named, not pathed: `tls.yaml` already loads this
//! deployment's material and the authority it issues under, so a leg that
//! talks to one of our backends names what it wants and the certificate
//! manager answers. A third party's application owns its material instead, so
//! `ca` and `client` both still accept the paths where it keeps it.

use std::time::Duration;

use claviron_types::ssl::certstore::CertEntry;
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    #[serde(default = "default_verify")]
    pub verify: Verification,
    /// Trust store for `verify: ca`, cwd-relative like every operator path.
    #[serde(default = "default_ca")]
    pub ca: Option<std::path::PathBuf>,
    pub server_name: Option<String>,
    /// The identity this leg presents, spelled as `tls.yaml` spells one: a
    /// name the process resolver answers, read again at each handshake so a
    /// renewal needs no reload here, or `{cert_path, key_path}` for material
    /// that belongs to somebody else's application.
    #[serde(default = "default_client")]
    pub client: Option<CertEntry>,
    #[serde(default = "default_insecure_skip_verify")]
    pub insecure_skip_verify: bool,
    #[serde(default = "default_handshake_timeout", with = "humantime_serde")]
    pub handshake_timeout: Duration,
    #[serde(default = "default_alpn")]
    pub alpn: Option<Vec<String>>,
}
impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            verify: default_verify(),
            ca: default_ca(),
            server_name: default_server_name(),
            client: default_client(),
            insecure_skip_verify: default_insecure_skip_verify(),
            handshake_timeout: default_handshake_timeout(),
            alpn: default_alpn(),
        }
    }
}
fn default_verify() -> Verification {
    Verification::Native
}
fn default_ca() -> Option<std::path::PathBuf> {
    None
}
fn default_server_name() -> Option<String> {
    None
}
fn default_client() -> Option<CertEntry> {
    None
}
fn default_insecure_skip_verify() -> bool {
    false
}
fn default_handshake_timeout() -> Duration {
    Duration::from_secs(5)
}
fn default_alpn() -> Option<Vec<String>> {
    None
}
/// Who issued the certificate this leg expects to be shown.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verification {
    /// The operating system trust store: a backend this deployment does not
    /// issue for. The default, because an unstated upstream is somebody else's.
    Native,
    /// The authority `tls.yaml` issues this deployment's own certificates
    /// from. This is the mutual-TLS case: our edge, our backend, our CA.
    Authority,
    /// An explicit PEM trust store; `ca` is required and names it.
    Ca,
}
