//! Explicit local-CA signing capability. Never published with public trust.
use crate::{CertAuthority, TlsError as Error};

/// Holds a CA private key only after an explicit signing request.
/// This is not an authorization check: the caller must enforce admission and
/// use database credentials permitted to read signing material.
pub struct CertSigner {
    authority: CertAuthority,
    key_der: Vec<u8>,
}

impl CertSigner {
    #[cfg(feature = "db")]
    pub async fn from_issuer(
        store: &claviron_hosting::HostingStore,
        name: &str,
    ) -> Result<Self, Error> {
        let authority = CertAuthority::from_issuer(store, name).await?;
        Self::from_authority(store, &authority).await
    }

    /// Acquire signing material for exactly the selected public snapshot.
    #[cfg(feature = "db")]
    pub async fn from_authority(
        store: &claviron_hosting::HostingStore,
        authority: &CertAuthority,
    ) -> Result<Self, Error> {
        authority.validate_issuer(store).await?;
        let issuer = authority.issuer.as_ref().ok_or_else(|| {
            Error::Other(
                "issuance requires a database-backed local_ca authority".into(),
            )
        })?;
        let ca = store
            .certificate_by_id(issuer.certificate_id)
            .await
            .map_err(|error| Error::Other(error.to_string()))?
            .ok_or_else(|| {
                Error::Other("issuer CA certificate not found".into())
            })?;
        let chain = authority
            .roots()
            .iter()
            .flat_map(|cert| cert.as_ref().iter().copied())
            .collect::<Vec<_>>();
        if !ca.enabled
            || ca.kind != claviron_types::CertificateKind::Ca
            || ca.certs_der != chain
        {
            return Err(Error::Other("authority CA is disabled or changed; restart after configuring an enabled CA".into()));
        }
        Ok(Self { authority: authority.clone(), key_der: ca.key_der })
    }

    #[cfg(feature = "db")]
    pub async fn validate_issuer(
        &self,
        store: &claviron_hosting::HostingStore,
    ) -> Result<(), Error> {
        let current = Self::from_authority(store, &self.authority).await?;
        if current.key_der != self.key_der {
            return Err(Error::Other(
                "authority CA key changed; restart to load the new issuer"
                    .into(),
            ));
        }
        Ok(())
    }

    pub fn authority(&self) -> &CertAuthority {
        &self.authority
    }
    pub fn issuer_id(&self) -> Option<i32> {
        self.authority.issuer_id()
    }

    /// Issue client authentication material with an optional application URI identity.
    /// The CN remains a label; this method assigns no application permissions.
    pub fn issue_client_certificate(
        &self,
        common_name: &str,
        aliases: &[String],
        subject: Option<&claviron_types::ssl::SubjectUri>,
        validity_days: u32,
    ) -> Result<IssuedCertificate, Error> {
        self.issue(SigningRequest {
            kind: claviron_types::CertificateKind::Client,
            common_name,
            aliases,
            subject,
            validity_days,
            ca_path_len: None,
            permissions: None,
        })
    }

    /// Validate all requested names before signing. The caller still authorizes
    /// delegation; membership in the catalog is not permission to issue a grant.
    pub fn issue_client_certificate_with_permissions(
        &self,
        common_name: &str,
        aliases: &[String],
        subject: Option<&claviron_types::ssl::SubjectUri>,
        validity_days: u32,
        catalog: &claviron_types::permission::PermissionCatalog,
        names: &[String],
    ) -> Result<IssuedCertificate, Error> {
        let permissions = catalog
            .validate(names)
            .map_err(|error| Error::Other(error.to_string()))?;
        self.issue(SigningRequest {
            kind: claviron_types::CertificateKind::Client,
            common_name,
            aliases,
            subject,
            validity_days,
            ca_path_len: None,
            permissions: Some(permissions),
        })
    }

    /// Mint material without persistence or resolver mutation. Run on a blocking thread.
    pub fn issue_certificate(
        &self,
        kind: claviron_types::CertificateKind,
        common_name: &str,
        aliases: &[String],
        validity_days: u32,
        ca_path_len: Option<u8>,
    ) -> Result<IssuedCertificate, Error> {
        self.issue(SigningRequest {
            kind,
            common_name,
            aliases,
            subject: None,
            validity_days,
            ca_path_len,
            permissions: None,
        })
    }

    fn issue(
        &self,
        request: SigningRequest<'_>,
    ) -> Result<IssuedCertificate, Error> {
        use claviron_types::ssl::{
            formatters, generators, parsers, permission,
        };
        let SigningRequest {
            kind,
            common_name,
            aliases,
            subject,
            validity_days,
            ca_path_len,
            permissions,
        } = request;
        if aliases.iter().any(|name| permission::is_permission_uri(name))
            || subject
                .is_some_and(|uri| permission::is_permission_uri(uri.as_str()))
        {
            return Err(Error::Other("certificate permissions require catalog validation, not SAN aliases".into()));
        }
        let mut names = aliases.to_vec();
        if let Some(subject) = subject {
            names.push(subject.to_string());
        }
        if let Some(permissions) = permissions {
            names.push(permission::to_uri(&permissions));
        }
        let aliases = names.as_slice();
        let ca_key = generators::read_private_key_from_der(&self.key_der)?
            .serialize_pem();
        let ca_der = self
            .authority
            .roots()
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
        for issuer in self.authority.roots() {
            let parsed = parsers::take_first_cert_from_der(issuer.as_ref())?;
            if parsed.subject() != parsed.issuer() {
                certs_der.extend_from_slice(issuer.as_ref());
            }
        }
        Ok(IssuedCertificate { certs_der, key_der: key.serialize_der() })
    }
}

struct SigningRequest<'a> {
    kind: claviron_types::CertificateKind,
    common_name: &'a str,
    aliases: &'a [String],
    subject: Option<&'a claviron_types::ssl::SubjectUri>,
    validity_days: u32,
    ca_path_len: Option<u8>,
    permissions: Option<claviron_types::permission::Permissions>,
}

/// Private material destined for persistence; deliberately has no Debug output.
pub struct IssuedCertificate {
    pub certs_der: Vec<u8>,
    pub key_der: Vec<u8>,
}

impl std::fmt::Debug for CertSigner {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("CertSigner")
            .field("authority", &self.authority)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "../tests/signer.rs"]
mod tests;
