//! The authority this deployment issues its own certificates from.
//!
//! `tls.yaml`'s certstore names one authority, and every certificate this
//! deployment issues chains to it. That makes it the one trust anchor a
//! service needs when it speaks TLS *outward* to another of our own services:
//! not the operating system's store, which knows nothing about us, and not a
//! path each consumer opens for itself. Loading it once, beside the resolver,
//! keeps certificate material in one place — a consumer asks the registry for
//! the anchor without opening a CA file or issuing its own database query.
//!
//! The material is read at boot and does not change afterwards. Rotating the
//! authority changes what every certificate in the deployment chains to, so it
//! is a restart rather than a reload. Named DB authorities include signing
//! material; explicit file authorities remain trust-only.

use std::path::{Path, PathBuf};

use rustls::pki_types::CertificateDer;

use crate::config::TlsConfig;
use crate::error::Error;

pub struct CertAuthority {
    path: PathBuf,
    roots: Vec<CertificateDer<'static>>,
    signer: Option<DatabaseSigner>,
}

impl CertAuthority {
    /// Name anchors the caller already holds. The policy of *where* an
    /// authority comes from stays with [`CertAuthority::load`]; this is the
    /// value itself, for a test or a future non-file source.
    pub fn new(
        path: PathBuf,
        roots: Vec<CertificateDer<'static>>,
    ) -> Self {
        Self { path, roots, signer: None }
    }

    /// Load a trust-only file authority for deployments without DB issuance.
    pub(crate) fn load(path: PathBuf) -> Result<Self, Error> {
        let roots = crate::pem::load_der_certs(&path)?;
        if roots.is_empty() {
            return Err(Error::Other(format!(
                "certificate authority {} contains no certificate",
                path.display()
            )));
        }
        Ok(Self { path, roots, signer: None })
    }

    /// Resolve one configured local issuer and pin its CA material at boot.
    #[cfg(feature = "db")]
    pub async fn from_issuer(
        store: &claviron_hosting::HostingStore,
        name: &str,
    ) -> Result<Self, Error> {
        use claviron_hosting::{CertificateKind, IssuerKind};
        use rustls::pki_types::pem::PemObject;
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
            .certificate_by_id(id)
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
        let pem = claviron_types::ssl::formatters::certificate_chain_to_pem(
            &ca.certs_der,
        )?;
        let roots = CertificateDer::pem_slice_iter(pem.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| Error::Other(error.to_string()))?;
        if roots.is_empty() {
            return Err(Error::Other("issuer CA chain is empty".into()));
        }
        Ok(Self {
            path: PathBuf::new(),
            roots,
            signer: Some(DatabaseSigner {
                issuer_id: issuer.id,
                certificate_id: ca.id,
                name: issuer.name,
                key_der: ca.key_der,
            }),
        })
    }

    /// Refuse issuance after the DB identity was disabled or rotated. Trust
    /// material remains boot-pinned; installing a replacement needs restart.
    #[cfg(feature = "db")]
    pub async fn validate_issuer(
        &self,
        store: &claviron_hosting::HostingStore,
    ) -> Result<(), Error> {
        let signer = self.signer.as_ref().ok_or_else(|| {
            Error::Other("authority is not database-backed".into())
        })?;
        let issuer = store
            .issuer_by_id(signer.issuer_id)
            .await
            .map_err(|error| Error::Other(error.to_string()))?
            .ok_or_else(|| {
                Error::Other("authority issuer no longer exists".into())
            })?;
        if issuer.kind != claviron_hosting::IssuerKind::LocalCa
            || issuer.ca_certificate_id != Some(signer.certificate_id)
        {
            return Err(Error::Other(
                "authority changed; restart to load the new issuer".into(),
            ));
        }
        let ca = store
            .certificate_by_id(signer.certificate_id)
            .await
            .map_err(|error| Error::Other(error.to_string()))?
            .ok_or_else(|| {
                Error::Other("authority CA no longer exists".into())
            })?;
        let pinned_chain = self
            .roots
            .iter()
            .flat_map(|cert| cert.as_ref().iter().copied())
            .collect::<Vec<_>>();
        if !ca.enabled
            || ca.kind != claviron_types::CertificateKind::Ca
            || ca.certs_der != pinned_chain
            || ca.key_der != signer.key_der
        {
            return Err(Error::Other("authority CA is disabled or changed; restart after configuring an enabled CA".into()));
        }
        Ok(())
    }

    pub fn issuer_id(&self) -> Option<i32> {
        self.signer.as_ref().map(|signer| signer.issuer_id)
    }

    pub(crate) fn path_of(config: &TlsConfig) -> Option<PathBuf> {
        config.certstore().authority_cert_path()
    }

    /// Mint certificate material using the boot-pinned database authority.
    /// Call on a blocking thread; key decoding and signing are synchronous.
    /// A file-backed trust anchor has no issuance capability.
    /// No files are created and no resolver entry is installed.
    pub fn issue_certificate(
        &self,
        kind: claviron_types::CertificateKind,
        common_name: &str,
        aliases: &[String],
        validity_days: u32,
        ca_path_len: Option<u8>,
    ) -> Result<IssuedCertificate, Error> {
        use claviron_types::ssl::{formatters, generators, parsers};
        let signer = self.signer.as_ref().ok_or_else(|| {
            Error::Other(
                "issuance requires a database-backed local_ca authority".into(),
            )
        })?;
        let ca_key = generators::read_private_key_from_der(&signer.key_der)?
            .serialize_pem();
        let ca_der = self
            .roots
            .iter()
            .flat_map(|cert| cert.as_ref().iter().copied())
            .collect::<Vec<_>>();
        let ca_pem = formatters::certificate_chain_to_pem(&ca_der)?;
        let key = generators::generate_private_key()?;
        let cert = if kind == claviron_types::CertificateKind::Ca {
            let path_len = ca_path_len.ok_or_else(|| {
                Error::Other(
                    "CA issuance requires an explicit path length".into(),
                )
            })?;
            generators::generate_intermediate_ca_cert(
                key.serialize_pem().as_bytes(),
                ca_pem.as_bytes(),
                ca_key.as_bytes(),
                common_name,
                validity_days,
                &claviron_types::ssl::Organization::default(),
                Some(path_len),
            )?
        } else {
            generators::generate_leaf_cert(
                kind,
                common_name,
                key.serialize_pem().as_bytes(),
                ca_pem.as_bytes(),
                ca_key.as_bytes(),
                validity_days,
                aliases,
            )?
        };
        let mut certs_der = parsers::concat_chain_bytes_from_pem(&cert)?;
        // Store intermediates after the leaf. A self-issued root is a trust
        // anchor and does not belong in the transmitted certificate chain.
        for issuer in &self.roots {
            let parsed = parsers::take_first_cert_from_der(issuer.as_ref())?;
            if parsed.subject() != parsed.issuer() {
                certs_der.extend_from_slice(issuer.as_ref());
            }
        }
        Ok(IssuedCertificate { certs_der, key_der: key.serialize_der() })
    }

    /// The anchors a certificate issued by this deployment chains to.
    pub fn roots(&self) -> &[CertificateDer<'static>] {
        &self.roots
    }

    /// Where the material was read from, for diagnostics; not for reopening.
    pub fn path(&self) -> Option<&Path> {
        self.signer.is_none().then_some(self.path.as_path())
    }
}

/// Private material destined for persistence; deliberately has no Debug output.
pub struct IssuedCertificate {
    pub certs_der: Vec<u8>,
    pub key_der: Vec<u8>,
}

#[cfg(test)]
#[path = "../tests/authority.rs"]
mod tests;

struct DatabaseSigner {
    issuer_id: i32,
    #[cfg_attr(not(feature = "db"), allow(dead_code))]
    certificate_id: i32,
    name: String,
    key_der: Vec<u8>,
}

impl std::fmt::Debug for CertAuthority {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("CertAuthority")
            .field("path", &self.path())
            .field("issuer_id", &self.issuer_id())
            .field(
                "issuer_name",
                &self.signer.as_ref().map(|signer| &signer.name),
            )
            .field("certificate_count", &self.roots.len())
            .finish_non_exhaustive()
    }
}
