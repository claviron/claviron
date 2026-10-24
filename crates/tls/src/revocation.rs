//! Issuer/serial revocation snapshots, independent of SNI and user admission.
use std::{collections::HashSet, fmt, path::Path, sync::Arc};

use arc_swap::ArcSwapOption;
use claviron_types::ssl::{RevocationId, certstore::CertStore};
use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{CertificateError, DigitallySignedStruct, SignatureScheme};

use crate::{CertificateStore, HostingDeps, TlsError};

mod crl;

/// Only identities and the earliest CRL freshness boundary survive loading.
/// Certificate chains, CRL objects and private keys are not retained.
#[derive(Default)]
pub struct Snapshot {
    ids: HashSet<RevocationId>,
    next_update: Option<UnixTime>,
}

impl Snapshot {
    pub fn new(
        ids: HashSet<RevocationId>,
        next_update: Option<UnixTime>,
    ) -> Self {
        Self { ids, next_update }
    }

    fn validate(
        &self,
        now: UnixTime,
    ) -> Result<(), rustls::Error> {
        if self.next_update.is_some_and(|expires| now >= expires) {
            return Err(rustls::Error::InvalidCertificate(
                CertificateError::ExpiredRevocationList,
            ));
        }
        Ok(())
    }
}

/// Verifiers retain this handle across refreshes. No file/DB I/O occurs during
/// admission; refreshes serialize outside the handshake path.
pub struct Revocations {
    current: ArcSwapOption<Snapshot>,
    refreshing: tokio::sync::Mutex<()>,
}

impl Default for Revocations {
    fn default() -> Self {
        Self::new(Snapshot::default())
    }
}

impl Revocations {
    pub fn new(snapshot: Snapshot) -> Self {
        Self {
            current: ArcSwapOption::from(Some(Arc::new(snapshot))),
            refreshing: tokio::sync::Mutex::new(()),
        }
    }

    pub fn replace(
        &self,
        snapshot: Snapshot,
    ) {
        self.current.store(Some(Arc::new(snapshot)));
    }

    pub(crate) fn unavailable(&self) {
        self.current.store(None);
    }

    pub fn check(
        &self,
        id: &RevocationId,
        now: UnixTime,
    ) -> Result<(), rustls::Error> {
        let current = self.current.load();
        let snapshot = current.as_ref().ok_or_else(unknown_status)?;
        snapshot.validate(now)?;
        check_id(snapshot, id)
    }

    fn verify_leaf(
        &self,
        leaf: &[u8],
        now: UnixTime,
    ) -> Result<(), rustls::Error> {
        let current = self.current.load();
        let snapshot = current.as_ref().ok_or_else(unknown_status)?;
        snapshot.validate(now)?;
        if snapshot.ids.is_empty() {
            return Ok(());
        }
        let id = RevocationId::from_certificate_der(leaf).map_err(|_| {
            rustls::Error::InvalidCertificate(CertificateError::BadEncoding)
        })?;
        check_id(snapshot, &id)
    }
}

fn unknown_status() -> rustls::Error {
    rustls::Error::InvalidCertificate(CertificateError::UnknownRevocationStatus)
}

fn check_id(
    snapshot: &Snapshot,
    id: &RevocationId,
) -> Result<(), rustls::Error> {
    if snapshot.ids.contains(id) {
        return Err(rustls::Error::InvalidCertificate(
            CertificateError::Revoked,
        ));
    }
    Ok(())
}

pub(crate) async fn load(
    store: &CertificateStore,
    declaration: &CertStore,
    hosting: &HostingDeps,
) -> Result<Snapshot, TlsError> {
    let now = UnixTime::now();
    let path = declaration.crl_path();
    let bytes =
        tokio::task::spawn_blocking(move || read_file(&path)).await.map_err(
            |error| TlsError::Other(format!("CRL read task failed: {error}")),
        )??;
    #[allow(unused_mut)] // Database-free builds only load the file snapshot.
    let mut snapshot = if let Some(bytes) = bytes {
        let authority = store.authority().await?.ok_or_else(|| {
            TlsError::Other(
                "a CRL requires certstore.authority for signature verification"
                    .into(),
            )
        })?;
        let roots = authority.roots().to_vec();
        tokio::task::spawn_blocking(move || crl::index(&bytes, &roots, now))
            .await
            .map_err(|error| {
                TlsError::Other(format!("CRL index task failed: {error}"))
            })??
    } else {
        Snapshot::default()
    };

    #[cfg(feature = "db")]
    if let Some(service) = hosting.0.as_ref() {
        let as_of = i64::try_from(now.as_secs())
            .ok()
            .and_then(|seconds| {
                time::OffsetDateTime::from_unix_timestamp(seconds).ok()
            })
            .ok_or_else(|| {
                TlsError::Other("snapshot time is out of range".into())
            })?;
        let chains = service
            .store()
            .map_err(|error| TlsError::Other(error.to_string()))?
            .revoked_certificate_chains(as_of)
            .await
            .map_err(|error| TlsError::Other(error.to_string()))?;
        let ids = tokio::task::spawn_blocking(move || {
            chains
                .into_iter()
                .map(|chain| RevocationId::from_certificate_der(&chain))
                .collect::<Result<HashSet<_>, _>>()
        })
        .await
        .map_err(|error| {
            TlsError::Other(format!("revocation index task failed: {error}"))
        })??;
        snapshot.ids.extend(ids);
    }
    #[cfg(not(feature = "db"))]
    let _ = hosting;

    snapshot.validate(UnixTime::now())?;
    Ok(snapshot)
}

fn read_file(path: &Path) -> Result<Option<Vec<u8>>, TlsError> {
    if path.as_os_str().is_empty() {
        return Err(TlsError::Other("CRL path must not be empty".into()));
    }
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => {
            return Err(TlsError::Other(format!(
                "reading CRL {}: {error}",
                path.display()
            )));
        }
    };
    if bytes.iter().all(u8::is_ascii_whitespace) {
        tracing::warn!(path = %path.display(), "CRL file is empty; no file revocations are loaded");
        return Ok(None);
    }
    Ok(Some(bytes))
}

/// Refresh after hosting changes or config reload; failure blocks new admissions.
pub(crate) async fn refresh(
    state: &claviron_runtime::AppState
) -> Result<(), TlsError> {
    let Some(snapshot) = state.registry_ref().resolve::<Revocations>() else {
        return Ok(());
    };
    let _refresh = snapshot.refreshing.lock().await;
    let Some(store) = state.registry_ref().resolve::<CertificateStore>() else {
        snapshot.unavailable();
        return Err(TlsError::Other(
            "certificate source is unavailable".into(),
        ));
    };
    match store.revocations().await {
        Ok(next) => {
            snapshot.replace(next);
            Ok(())
        }
        Err(error) => {
            snapshot.unavailable();
            Err(error)
        }
    }
}

pub(crate) fn verifier(
    inner: Arc<dyn ClientCertVerifier>,
    snapshot: Arc<Revocations>,
) -> Arc<dyn ClientCertVerifier> {
    Arc::new(RevocationVerifier { inner, snapshot })
}

struct RevocationVerifier {
    inner: Arc<dyn ClientCertVerifier>,
    snapshot: Arc<Revocations>,
}

impl fmt::Debug for RevocationVerifier {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.debug_struct("RevocationVerifier").finish_non_exhaustive()
    }
}

impl ClientCertVerifier for RevocationVerifier {
    fn offer_client_auth(&self) -> bool {
        self.inner.offer_client_auth()
    }
    fn client_auth_mandatory(&self) -> bool {
        self.inner.client_auth_mandatory()
    }
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        self.inner.root_hint_subjects()
    }
    fn verify_client_cert(
        &self,
        leaf: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let verified =
            self.inner.verify_client_cert(leaf, intermediates, now)?;
        if let Err(error) = self.snapshot.verify_leaf(leaf.as_ref(), now) {
            let client_cn =
                claviron_types::ssl::parsers::take_first_cert_from_der(
                    leaf.as_ref(),
                )
                .ok()
                .and_then(|certificate| {
                    let mut names = certificate.subject().iter_common_name();
                    let name = names.next()?.as_str().ok()?.to_owned();
                    names.next().is_none().then_some(name)
                });
            tracing::warn!(
                client_cn = client_cn.as_deref(),
                %error,
                "mTLS client certificate rejected by revocation policy"
            );
            return Err(error);
        }
        Ok(verified)
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, signature)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, signature)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
