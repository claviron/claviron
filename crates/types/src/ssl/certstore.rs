use std::path::{Path, PathBuf};

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

use serde::Deserialize;

use super::{Basename, CommonName, Organization};
use crate::{Host, KeyString};

pub const SERVER_CERTS_DIRNAME: &str = "servers";
/// Conventional CRL file at the certificate store root.
pub const CRL_FILENAME: &str = "revocations.crl";
/// Client identities occupy a separate subtree from signing authorities.
pub const CLIENT_CERTS_DIRNAME: &str = "clients";
/// Existing client stores and the certificate generator use this leaf filename.
pub const CLIENT_CERT_FILENAME: &str = "client.crt";
/// Existing client stores and the certificate generator use this key filename.
pub const CLIENT_KEY_FILENAME: &str = "client.key";
/// Human-readable client certificate inspection output, never signing material.
pub const CLIENT_TEXT_FILENAME: &str = "certificate.txt";
pub const CA_CERTS_DIR_NAME: &str = "ca";
pub const CA_CERT_FILENAME: &str = "ca.crt";
pub const CA_KEY_FILENAME: &str = "ca.key";
pub const PRIVKEY_FILENAME: &str = "privkey.pem";
pub const FULLCHAIN_CERT_FILENAME: &str = "fullchain.pem";

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum CertEntry {
    Name(Host),
    Paths(CertPaths),
}

/// CA and default identities are not DNS hostnames. A bare name that is a
/// [`Basename`] tries the file store first, then hosting; a bare name that is
/// not one is in hosting only. Explicit local, CN and path declarations select
/// one source.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash)]
#[serde(try_from = "CertificateSourceDeclaration")]
pub enum CertificateSource {
    Name(CommonName),
    Local(Basename),
    Cn(CommonName),
    Path(PathBuf),
    Paths(CertPaths),
}

impl CertificateSource {
    /// The file store folder this source selects, when it selects one.
    pub fn basename(&self) -> Option<Basename> {
        match self {
            Self::Name(name) => Basename::new(name.as_str()),
            Self::Local(name) => Some(*name),
            Self::Cn(_) | Self::Path(_) | Self::Paths(_) => None,
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum CertificateSourceDeclaration {
    Name(CommonName),
    Selection(SourceSelection),
    Paths(CertPaths),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceSelection {
    /// Kept as written so a name that is not a basename is reported as one.
    local: Option<String>,
    cn: Option<CommonName>,
    path: Option<PathBuf>,
}

impl TryFrom<CertificateSourceDeclaration> for CertificateSource {
    type Error = String;

    fn try_from(
        source: CertificateSourceDeclaration
    ) -> Result<Self, Self::Error> {
        let source = match source {
            CertificateSourceDeclaration::Name(name) => {
                return Ok(Self::Name(name));
            }
            CertificateSourceDeclaration::Paths(paths)
                if paths.cert_path.is_some() =>
            {
                return Ok(Self::Paths(paths));
            }
            CertificateSourceDeclaration::Paths(_) => {
                return Err("certificate source requires a cert_path".into());
            }
            CertificateSourceDeclaration::Selection(source) => source,
        };
        match (source.local, source.cn, source.path) {
            (Some(local), None, None) => {
                Basename::try_from(local).map(Self::Local)
            }
            (None, Some(cn), None) => Ok(Self::Cn(cn)),
            (None, None, Some(path)) if !path.as_os_str().is_empty() => {
                Ok(Self::Path(path))
            }
            _ => {
                Err("certificate source must select exactly one non-empty local, cn or path"
                    .into())
            }
        }
    }
}

pub type AuthorityEntry = CertificateSource;

pub fn default_certificate_source() -> CertificateSource {
    CertificateSource::Name(
        "default".parse().expect("valid default certificate name"),
    )
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct CertPaths {
    cert_path: Option<PathBuf>,
    key_path: Option<PathBuf>,
    ca_path: Option<PathBuf>,
}

impl CertPaths {
    pub fn from_pem_key_pair(
        cert_path: &Path,
        key_path: &Path,
    ) -> Self {
        Self {
            cert_path: Some(cert_path.to_path_buf()),
            key_path: Some(key_path.to_path_buf()),
            ca_path: None,
        }
    }

    pub fn ca_path(&self) -> Option<&PathBuf> {
        self.ca_path.as_ref()
    }

    pub fn cert_path(&self) -> Option<&PathBuf> {
        self.cert_path.as_ref()
    }

    pub fn key_path(&self) -> Option<&PathBuf> {
        self.key_path.as_ref()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CertStore {
    dir: PathBuf,
    /// authority can be root or intermediate
    authority: Option<AuthorityEntry>,
    #[serde(default)]
    organization: Organization,
    /// Explicit CRL path, cwd-relative. Omission selects dir/CRL_FILENAME.
    #[serde(default)]
    crl_path: Option<PathBuf>,
}

impl CertStore {
    pub fn crl_path(&self) -> PathBuf {
        self.crl_path.clone().unwrap_or_else(|| self.dir.join(CRL_FILENAME))
    }

    pub fn organization(&self) -> &Organization {
        &self.organization
    }

    pub fn authority_entry(&self) -> Option<&AuthorityEntry> {
        self.authority.as_ref()
    }

    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

    /// A client folder is named by a basename or by the certificate's
    /// common name.
    pub fn client_dir(
        &self,
        cname: &str,
    ) -> PathBuf {
        self.dir().join(CLIENT_CERTS_DIRNAME).join(cname)
    }

    /// A server folder is named by a basename or by the certificate's host.
    pub fn server_dir(
        &self,
        cname: &str,
    ) -> PathBuf {
        self.dir().join(SERVER_CERTS_DIRNAME).join(cname)
    }

    pub fn ca_dir(
        &self,
        name: Basename,
    ) -> PathBuf {
        self.dir().join(CA_CERTS_DIR_NAME).join(name)
    }

    pub fn server_cert_path(
        &self,
        name: impl KeyString,
    ) -> PathBuf {
        let name = name.key_str();
        self.server_dir(name.as_ref()).join(FULLCHAIN_CERT_FILENAME)
    }

    pub fn server_key_path(
        &self,
        name: impl KeyString,
    ) -> PathBuf {
        let name = name.key_str();
        self.server_dir(name.as_ref()).join(PRIVKEY_FILENAME)
    }

    pub fn client_cert_path(
        &self,
        name: &str,
    ) -> PathBuf {
        self.client_dir(name).join(FULLCHAIN_CERT_FILENAME)
    }

    pub fn client_key_path(
        &self,
        name: &str,
    ) -> PathBuf {
        self.client_dir(name).join(CLIENT_KEY_FILENAME)
    }

    pub fn ca_cert_path(
        &self,
        name: Basename,
    ) -> PathBuf {
        self.ca_dir(name).join(CA_CERT_FILENAME)
    }

    pub fn ca_key_path(
        &self,
        name: Basename,
    ) -> PathBuf {
        self.ca_dir(name).join(CA_KEY_FILENAME)
    }

    pub fn authority_cert_path(&self) -> Option<PathBuf> {
        match self.authority.as_ref()? {
            AuthorityEntry::Path(path) => Some(self.dir.join(path)),
            AuthorityEntry::Paths(paths) => paths.cert_path.clone(),
            source @ (AuthorityEntry::Name(_)
            | AuthorityEntry::Local(_)
            | AuthorityEntry::Cn(_)) => {
                source.basename().map(|name| self.ca_cert_path(name))
            }
        }
    }
}

/// Default function for the `path` field
pub fn default_certstore_path() -> PathBuf {
    dirs::config_dir()
        .map(|dir| dir.join("claviron").join("certstore.yaml"))
        .filter(|path| path.is_file())
        .or_else(|| {
            dirs::home_dir()
                .map(|home| {
                    home.join(".config").join("claviron").join("certstore.yaml")
                })
                .filter(|path| path.is_file())
        })
        .unwrap_or_else(|| PathBuf::from(".").join("certstore.yaml"))
}

impl CertStore {
    pub fn resolve_path(path: Option<PathBuf>) -> PathBuf {
        path.unwrap_or_else(default_certstore_path)
    }

    /// Load config from the given `file_path`. If the config
    pub fn from_path(path: &PathBuf) -> Result<Self, BoxError> {
        let yaml = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to open config: {e}"))?;
        let mut cs: Self = serde_yaml::from_str(&yaml)
            .map_err(|e| format!("Failed to parse config: {e}"))?;
        // Use fallback if path is missing

        if cs.dir.is_relative() {
            let config_dir = path.parent().unwrap().to_path_buf();
            cs.dir = config_dir.join(cs.dir);
        };
        Ok(cs)
    }
}

#[cfg(test)]
#[path = "../../tests/ssl/certstore.rs"]
mod tests;
