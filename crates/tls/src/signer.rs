//! Explicit local-CA signing capability, independent of public trust loading.
use crate::{CertAuthority, TlsError as Error};

/// Holds a CA private key only after an explicit signing request.
/// This is not an authorization check: the caller must enforce admission and
/// be allowed to read signing material from the selected source.
pub struct CertSigner {
    authority: CertAuthority,
    /// Parsed once when the capability is acquired; every issuance signs
    /// with it as it is.
    key: claviron_types::ssl::generators::KeyPair,
}

impl CertSigner {
    pub(crate) fn from_file(
        authority: CertAuthority,
        key_path: &std::path::Path,
    ) -> Result<Self, Error> {
        use claviron_types::ssl::{
            generators, parsers::take_first_cert_from_der,
        };

        let key = crate::pem::load_private_key(key_path)?;
        rustls::sign::CertifiedKey::from_der(
            authority.roots().to_vec(),
            key.clone_key(),
            &rustls::crypto::aws_lc_rs::default_provider(),
        )?
        .keys_match()?;
        let key = generators::read_private_key_from_der(key.secret_der())?;
        let mut chain = authority
            .roots()
            .iter()
            .map(|cert| take_first_cert_from_der(cert.as_ref()));
        let leaf = chain
            .next()
            .transpose()?
            .ok_or_else(|| Error::Other("signing authority is empty".into()))?;
        let mut current = leaf;
        loop {
            if !current.is_ca()
                || !current.validity().is_valid()
                || current
                    .key_usage()
                    .map_err(|error| Error::Other(error.to_string()))?
                    .is_some_and(|usage| !usage.value.key_cert_sign())
            {
                return Err(Error::Other(
                    "invalid signing authority certificate".into(),
                ));
            }
            let Some(parent) = chain.next().transpose()? else { break };
            if current.issuer() != parent.subject()
                || current.verify_signature(Some(parent.public_key())).is_err()
            {
                return Err(Error::Other(
                    "invalid signing authority chain".into(),
                ));
            }
            current = parent;
        }
        Ok(Self { authority, key })
    }

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
            || ca.revoked_at.is_some()
            || ca.certs_der != chain
        {
            return Err(Error::Other("authority CA is disabled or changed; restart after configuring an enabled CA".into()));
        }
        if ca.key_der.is_empty() {
            return Err(Error::Other(
                "selected hosting issuer has no signing key".into(),
            ));
        }
        let key = claviron_types::ssl::generators::read_private_key_from_der(
            &ca.key_der,
        )?;
        Ok(Self { authority: authority.clone(), key })
    }

    #[cfg(feature = "db")]
    pub async fn validate_issuer(
        &self,
        store: &claviron_hosting::HostingStore,
    ) -> Result<(), Error> {
        let current = Self::from_authority(store, &self.authority).await?;
        if current.key.serialized_der() != self.key.serialized_der() {
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

    /// Renew an existing leaf with its original key and this issuer.
    pub fn renew_certificate(
        &self,
        leaf: &[u8],
        key: &[u8],
        validity_days: u32,
    ) -> Result<Vec<u8>, Error> {
        let mut chain = claviron_types::ssl::renewal::renew_certificate(
            leaf,
            key,
            Some((self.certificate()?, self.key.serialized_der())),
            validity_days,
        )?;
        self.append_intermediates(&mut chain)?;
        Ok(chain)
    }

    /// Sign a verified CSR with the issuer's client profile. The request's
    /// subject and extensions are replaced by the authenticated claims.
    pub fn issue_client_csr(
        &self,
        csr_der: &[u8],
        common_name: &claviron_types::ssl::CommonName,
        subject: Option<&claviron_types::ssl::SubjectUri>,
        permissions: Option<&claviron_types::permission::Permissions>,
        validity_days: u32,
    ) -> Result<Vec<u8>, Error> {
        use claviron_types::ssl::{generators, permission};
        if subject
            .is_some_and(|uri| permission::is_permission_uri(uri.as_str()))
        {
            return Err(Error::Other(
                "subject cannot declare permissions".into(),
            ));
        }
        let mut aliases = Vec::new();
        if let Some(subject) = subject {
            aliases.push(subject.to_string());
        }
        if let Some(permissions) = permissions {
            aliases.push(permission::to_uri(permissions));
        }
        let cert = generators::sign_client_csr(
            common_name,
            csr_der,
            self.certificate()?,
            &self.key,
            validity_days,
            aliases,
        )?;
        let mut chain = cert.to_vec();
        self.append_intermediates(&mut chain)?;
        Ok(chain)
    }

    /// Issue client authentication material with an optional application URI identity.
    /// The CN remains a label; this method assigns no application permissions.
    pub fn issue_client_certificate(
        &self,
        common_name: &claviron_types::ssl::CommonName,
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
        common_name: &claviron_types::ssl::CommonName,
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
        common_name: &claviron_types::ssl::CommonName,
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
        use claviron_types::ssl::{generators, permission};
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
        let ca = self.certificate()?;
        let key = generators::generate_private_key()?;
        let cert = if kind == claviron_types::CertificateKind::Ca {
            let path_len = ca_path_len.ok_or_else(|| {
                Error::Other(
                    "CA issuance requires an explicit path length".into(),
                )
            })?;
            generators::generate_intermediate_ca_cert(
                &key,
                ca,
                &self.key,
                common_name,
                validity_days,
                &claviron_types::ssl::Organization::default(),
                Some(path_len),
            )?
        } else {
            generators::generate_leaf_cert(
                kind,
                common_name,
                &key,
                ca,
                &self.key,
                validity_days,
                aliases,
            )?
        };
        let mut certs_der = cert.to_vec();
        // Store intermediates after the leaf. A self-issued root is a trust
        // anchor and does not belong in the transmitted certificate chain.
        self.append_intermediates(&mut certs_der)?;
        Ok(IssuedCertificate { certs_der, key_der: key.serialize_der() })
    }

    /// The certificate this capability signs with, as DER.
    fn certificate(&self) -> Result<&[u8], Error> {
        self.authority
            .roots()
            .first()
            .map(AsRef::as_ref)
            .ok_or_else(|| Error::Other("signing authority is empty".into()))
    }

    fn append_intermediates(
        &self,
        chain: &mut Vec<u8>,
    ) -> Result<(), Error> {
        for issuer in self.authority.roots() {
            let parsed =
                claviron_types::ssl::parsers::take_first_cert_from_der(
                    issuer.as_ref(),
                )?;
            if parsed.subject() != parsed.issuer() {
                chain.extend_from_slice(issuer.as_ref());
            }
        }
        Ok(())
    }
}

struct SigningRequest<'a> {
    kind: claviron_types::CertificateKind,
    common_name: &'a claviron_types::ssl::CommonName,
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
