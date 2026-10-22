//! Public trust material selected by the application, independent of SNI.
//!
//! Reading an authority never loads a private key. Signing is an explicit,
//! separately obtained [`crate::CertSigner`] capability. Trust is boot-pinned;
//! certificate-map reload does not replace the selected authority.

use std::path::{Path, PathBuf};

use rustls::pki_types::CertificateDer;

use crate::error::Error;

pub use crate::signer::IssuedCertificate;

#[derive(Clone)]
pub struct CertAuthority {
    path: Option<PathBuf>,
    roots: Vec<CertificateDer<'static>>,
    pub(crate) issuer: Option<AuthorityIssuer>,
}

#[derive(Clone, Debug)]
pub(crate) struct AuthorityIssuer {
    pub id: i32,
    #[cfg(feature = "db")]
    pub certificate_id: i32,
    pub name: String,
}

impl CertAuthority {
    /// Supply public trust material without selecting or opening a source.
    pub fn new(
        path: PathBuf,
        roots: Vec<CertificateDer<'static>>,
    ) -> Self {
        Self { path: Some(path), roots, issuer: None }
    }

    /// Read only a PEM certificate chain; no sibling private key is opened.
    pub fn load(path: PathBuf) -> Result<Self, Error> {
        let roots = crate::pem::load_der_certs(&path)?;
        if roots.is_empty() {
            return Err(Error::Other(format!(
                "certificate authority {} contains no certificate",
                path.display()
            )));
        }
        Ok(Self { path: Some(path), roots, issuer: None })
    }

    /// Read a selected local issuer's public chain, without its private key.
    /// Selecting trust is the caller's policy, not a HostingStore side effect.
    #[cfg(feature = "db")]
    pub async fn from_issuer(
        store: &claviron_hosting::HostingStore,
        name: &str,
    ) -> Result<Self, Error> {
        use claviron_hosting::{CertificateKind, IssuerKind};
        let issuer = store
            .issuer_by_name(name)
            .await
            .map_err(|error| Error::Other(error.to_string()))?
            .ok_or_else(|| {
                Error::Other(format!("certificate issuer '{name}' not found"))
            })?;
        if issuer.kind != IssuerKind::LocalCa {
            return Err(Error::Other(
                "authority must select a local_ca issuer".into(),
            ));
        }
        let id = issuer.ca_certificate_id.ok_or_else(|| {
            Error::Other("local issuer has no CA certificate".into())
        })?;
        let ca = store
            .public_certificate_by_id(id)
            .await
            .map_err(|error| Error::Other(error.to_string()))?
            .ok_or_else(|| {
                Error::Other("issuer CA certificate not found".into())
            })?;
        if ca.kind != CertificateKind::Ca || !ca.enabled {
            return Err(Error::Other(
                "issuer must reference an enabled CA certificate".into(),
            ));
        }
        let roots =
            crate::pem::parse_cert_chain_from_bytes(&ca.cn, &ca.certs_der)?;
        if roots.is_empty() {
            return Err(Error::Other("issuer CA chain is empty".into()));
        }
        Ok(Self {
            path: None,
            roots,
            issuer: Some(AuthorityIssuer {
                id: issuer.id,
                certificate_id: ca.id,
                name: issuer.name,
            }),
        })
    }

    /// Check that the selected public authority has not been disabled or rotated.
    /// No signing key is read. Installing changed trust requires a restart.
    #[cfg(feature = "db")]
    pub async fn validate_issuer(
        &self,
        store: &claviron_hosting::HostingStore,
    ) -> Result<(), Error> {
        let pinned = self.issuer.as_ref().ok_or_else(|| {
            Error::Other("authority is not database-backed".into())
        })?;
        let issuer = store
            .issuer_by_id(pinned.id)
            .await
            .map_err(|error| Error::Other(error.to_string()))?
            .ok_or_else(|| {
                Error::Other("authority issuer no longer exists".into())
            })?;
        if issuer.kind != claviron_hosting::IssuerKind::LocalCa
            || issuer.ca_certificate_id != Some(pinned.certificate_id)
        {
            return Err(Error::Other(
                "authority changed; restart to load the new issuer".into(),
            ));
        }
        let ca = store
            .public_certificate_by_id(pinned.certificate_id)
            .await
            .map_err(|error| Error::Other(error.to_string()))?
            .ok_or_else(|| {
                Error::Other("authority CA no longer exists".into())
            })?;
        let chain = self
            .roots
            .iter()
            .flat_map(|cert| cert.as_ref().iter().copied())
            .collect::<Vec<_>>();
        if !ca.enabled
            || ca.kind != claviron_types::CertificateKind::Ca
            || ca.certs_der != chain
        {
            return Err(Error::Other("authority CA is disabled or changed; restart after configuring an enabled CA".into()));
        }
        Ok(())
    }

    pub fn issuer_id(&self) -> Option<i32> {
        self.issuer.as_ref().map(|issuer| issuer.id)
    }

    pub fn issuer_name(&self) -> Option<&str> {
        self.issuer.as_ref().map(|issuer| issuer.name.as_str())
    }

    pub fn roots(&self) -> &[CertificateDer<'static>] {
        &self.roots
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

impl std::fmt::Debug for CertAuthority {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("CertAuthority")
            .field("path", &self.path())
            .field("issuer_id", &self.issuer_id())
            .field("issuer_name", &self.issuer_name())
            .field("certificate_count", &self.roots.len())
            .finish_non_exhaustive()
    }
}
