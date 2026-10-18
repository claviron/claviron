use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{CertificateError, DigitallySignedStruct, Error, SignatureScheme};
use serde::Deserialize;
use tracing::{debug, warn};
use x509_parser::certificate::X509Certificate;
use x509_parser::prelude::FromDer;

#[derive(Deserialize, Clone, PartialEq)]
pub struct PolicyConfig {
    organization: String,
    unit: String,
}

impl PolicyConfig {
    pub fn new(
        organization: impl Into<String>,
        unit: impl Into<String>,
    ) -> Self {
        Self { organization: organization.into(), unit: unit.into() }
    }
}

pub(crate) struct PolicyVerifier {
    inner: Arc<dyn ClientCertVerifier>,
    required_o: String,
    required_ou: String,
}

impl PolicyVerifier {
    pub fn from_config(
        inner: Arc<dyn ClientCertVerifier>,
        cfg: &PolicyConfig,
    ) -> Self {
        PolicyVerifier {
            inner,
            required_o: cfg.organization.clone(),
            required_ou: cfg.unit.clone(),
        }
    }
}

impl Debug for PolicyVerifier {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("PolicyVerifier")
            .field("inner", &"WebPkiClientVerifier(..)")
            .field("required_o", &self.required_o)
            .field("required_ou", &self.required_ou)
            .finish()
    }
}

impl ClientCertVerifier for PolicyVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        // First, validate chain/time/EKU via WebPKI
        self.inner.verify_client_cert(end_entity, intermediates, now)?;

        // Then enforce O/OU policy on Subject DN
        let (_, cert) = X509Certificate::from_der(end_entity.as_ref())
            .map_err(|_e| {
                Error::InvalidCertificate(CertificateError::BadEncoding)
            })?;

        let o_values: Vec<String> = cert
            .subject()
            .iter_organization()
            .filter_map(|o| o.as_str().ok().map(|s| s.to_string()))
            .collect();
        let ou_values: Vec<String> = cert
            .subject()
            .iter_organizational_unit()
            .filter_map(|ou| ou.as_str().ok().map(|s| s.to_string()))
            .collect();

        // Helpful debug for mTLS policy decisions
        debug!(
            // subject = ?cert.subject(),
            orgs = ?o_values,
            org_units = ?ou_values,
            required_o = %self.required_o,
            required_ou = %self.required_ou,
            "client cert parsed; evaluating DN policy"
        );

        let o_ok = cert
            .subject()
            .iter_organization()
            .any(|o| o.as_str().map(|s| s == self.required_o).unwrap_or(false));

        let ou_ok = cert.subject().iter_organizational_unit().any(|ou| {
            ou.as_str().map(|s| s == self.required_ou).unwrap_or(false)
        });

        if !o_ok {
            warn!(
                subject = ?cert.subject(),
                expected_o = %self.required_o,
                got_orgs = ?o_values,
                "organization mismatch"
            );

            return Err(Error::General(
                "client DN policy mismatch (O/OU)".into(),
            ));
        }
        if !ou_ok {
            warn!(
                subject = ?cert.subject(),
                expected_ou = %self.required_ou,
                got_ous = ?ou_values,
                "organizationalUnit mismatch"
            );
            return Err(Error::General(
                "client DN policy mismatch (O/OU)".into(),
            ));
        }

        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
