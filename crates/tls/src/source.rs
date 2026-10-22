//! Certificate sources and explicit capabilities, independent of SNI routing.
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron_runtime::registry::{Bootable, Component, priority};
use claviron_runtime::{AppState, Register};
use claviron_types::CertificateKind;
use claviron_types::ssl::certstore::{
    CLIENT_CERT_FILENAME, CLIENT_KEY_FILENAME, CertStore, CertificateSource,
    FULLCHAIN_CERT_FILENAME, PRIVKEY_FILENAME,
};
use rustls::sign::CertifiedKey;

#[cfg(feature = "db")]
use crate::CertSigner;
use crate::config::TlsConfigSource;
use crate::{CertAuthority, HostingDeps, TlsError as Error};

/// Resolves selected certificate material without creating a listener or SNI map.
/// Public trust and local signing are distinct operations. This API does not
/// authorize a caller: storage credentials and application admission do that.
pub struct CertificateStore {
    files: ArcSwap<CertStore>,
    #[cfg(feature = "db")]
    hosting: HostingDeps,
}

impl CertificateStore {
    pub fn new(files: CertStore) -> Self {
        Self {
            files: ArcSwap::from_pointee(files),
            #[cfg(feature = "db")]
            hosting: (None,),
        }
    }

    #[cfg(feature = "db")]
    pub fn with_hosting(
        files: CertStore,
        hosting: Arc<claviron_hosting::HostingService>,
    ) -> Self {
        Self::with_dependencies(files, (Some(hosting),))
    }

    pub(crate) fn with_dependencies(
        files: CertStore,
        _hosting: HostingDeps,
    ) -> Self {
        Self {
            files: ArcSwap::from_pointee(files),
            #[cfg(feature = "db")]
            hosting: _hosting,
        }
    }

    #[cfg(feature = "db")]
    fn hosting_store(&self) -> Result<claviron_hosting::HostingStore, Error> {
        self.hosting
            .0
            .as_ref()
            .ok_or_else(|| {
                Error::Other(
                    "certificate source requires HostingService".into(),
                )
            })?
            .store()
            .map_err(|error| Error::Other(error.to_string()))
    }

    /// Load one selected public CA chain. A bare name tries files first; an
    /// explicit CN selects hosting only. No private-key query or file is used.
    pub async fn trust(
        &self,
        selection: &CertificateSource,
    ) -> Result<CertAuthority, Error> {
        self.trust_from(self.files.load_full(), selection).await
    }

    async fn trust_from(
        &self,
        files: Arc<CertStore>,
        selection: &CertificateSource,
    ) -> Result<CertAuthority, Error> {
        let path = match selection {
            CertificateSource::Name(name) => {
                let path = files.ca_cert_path(name.as_str());
                if tokio::fs::try_exists(&path).await? {
                    Some(path)
                } else {
                    None
                }
            }
            CertificateSource::Cn(_) => None,
            CertificateSource::Path(path) => Some(files.dir().join(path)),
            CertificateSource::Paths(paths) => Some(
                paths
                    .cert_path()
                    .ok_or_else(|| {
                        Error::Other(
                            "authority requires a certificate path".into(),
                        )
                    })?
                    .clone(),
            ),
        };
        if let Some(path) = path {
            return tokio::task::spawn_blocking(move || {
                CertAuthority::load(path)
            })
            .await
            .map_err(|error| {
                Error::Other(format!(
                    "TLS authority blocking task failed: {error}"
                ))
            })?;
        }
        let name = match selection {
            CertificateSource::Name(name) | CertificateSource::Cn(name) => {
                name.as_str()
            }
            _ => unreachable!("explicit paths were handled above"),
        };
        #[cfg(feature = "db")]
        {
            CertAuthority::from_issuer(&self.hosting_store()?, name).await
        }
        #[cfg(not(feature = "db"))]
        Err(Error::Other(format!(
            "authority CN '{name}' requires database support; select authority.path for file trust"
        )))
    }

    /// The configured public trust selection. Omission publishes no authority.
    pub async fn authority(&self) -> Result<Option<CertAuthority>, Error> {
        let files = self.files.load_full();
        let Some(selection) = files.authority_entry().cloned() else {
            return Ok(None);
        };
        self.trust_from(files, &selection).await.map(Some)
    }

    /// Load a server/client identity and its own private key, without SNI.
    /// CA signing keys cannot be obtained through this identity API.
    pub async fn identity(
        &self,
        selection: &CertificateSource,
        kind: CertificateKind,
    ) -> Result<Arc<CertifiedKey>, Error> {
        if kind == CertificateKind::Ca {
            return Err(Error::Other(
                "CA keys require an explicit signing capability".into(),
            ));
        }
        let files = self.files.load_full();
        let pair = match selection {
            CertificateSource::Name(name) => {
                let dir = if kind == CertificateKind::Client {
                    files.client_dir(name.as_str())
                } else {
                    files.server_dir(name.as_str())
                };
                let (cert, key) = if kind == CertificateKind::Client {
                    (CLIENT_CERT_FILENAME, CLIENT_KEY_FILENAME)
                } else {
                    (FULLCHAIN_CERT_FILENAME, PRIVKEY_FILENAME)
                };
                let pair = (dir.join(cert), dir.join(key));
                if tokio::fs::try_exists(&pair.0).await?
                    || tokio::fs::try_exists(&pair.1).await?
                {
                    Some(pair)
                } else {
                    None
                }
            }
            CertificateSource::Cn(_) => None,
            CertificateSource::Path(path) => {
                let cert = files.dir().join(path);
                let filename = if kind == CertificateKind::Client {
                    CLIENT_KEY_FILENAME
                } else {
                    PRIVKEY_FILENAME
                };
                let key = cert
                    .parent()
                    .ok_or_else(|| {
                        Error::Other("identity path requires a parent".into())
                    })?
                    .join(filename);
                Some((cert, key))
            }
            CertificateSource::Paths(paths) => Some((
                paths.cert_path().ok_or(Error::NoDefaultCert)?.clone(),
                paths
                    .key_path()
                    .ok_or_else(|| Error::NoKey("identity".into()))?
                    .clone(),
            )),
        };
        if let Some((cert, key)) = pair {
            return tokio::task::spawn_blocking(move || {
                let chain = crate::pem::load_der_certs(&cert)?;
                let leaf = chain.first().ok_or_else(|| {
                    Error::Other("identity chain is empty".into())
                })?;
                let parsed =
                    claviron_types::ssl::parsers::take_first_cert_from_der(
                        leaf.as_ref(),
                    )?;
                if parsed
                    .basic_constraints()
                    .map_err(|error| Error::Other(error.to_string()))?
                    .is_some_and(|constraints| constraints.value.ca)
                {
                    return Err(Error::Other(
                        "CA keys require an explicit signing capability".into(),
                    ));
                }
                let key = crate::pem::load_private_key(&key)?;
                CertifiedKey::from_der(
                    chain,
                    key,
                    &rustls::crypto::aws_lc_rs::default_provider(),
                )
                .map(Arc::new)
                .map_err(Into::into)
            })
            .await
            .map_err(|error| {
                Error::Other(format!(
                    "TLS identity blocking task failed: {error}"
                ))
            })?;
        }
        let name = match selection {
            CertificateSource::Name(name) | CertificateSource::Cn(name) => {
                name.as_str()
            }
            _ => unreachable!("explicit paths were handled above"),
        };
        #[cfg(feature = "db")]
        {
            let store = self.hosting_store()?;
            if kind == CertificateKind::Server {
                let service = self
                    .hosting
                    .0
                    .as_ref()
                    .expect("hosting_store checked the service");
                let id = service
                    .require_server_id()
                    .map_err(|error| Error::Other(error.to_string()))?;
                if !store
                    .servers_for_certificate(name)
                    .await
                    .map_err(|error| Error::Other(error.to_string()))?
                    .iter()
                    .any(|server| server.id == id)
                {
                    return Err(Error::Other(
                        "identity certificate is not assigned to this server"
                            .into(),
                    ));
                }
            }
            let cert = store
                .certificate_by_cn_and_kind(name, kind)
                .await
                .map_err(|error| Error::Other(error.to_string()))?
                .ok_or_else(|| Error::Other(format!("enabled {kind:?} identity '{name}' not found in hosting")))?;
            tokio::task::spawn_blocking(move || {
                crate::pem::db_cert_to_certified_key(&cert).map(Arc::new)
            })
            .await
            .map_err(|error| {
                Error::Other(format!(
                    "TLS identity blocking task failed: {error}"
                ))
            })?
        }
        #[cfg(not(feature = "db"))]
        Err(Error::Other(format!(
            "identity '{name}' requires database support"
        )))
    }

    /// Acquire a signing key only for an explicitly selected, pinned authority.
    #[cfg(feature = "db")]
    pub async fn signer(
        &self,
        authority: &CertAuthority,
    ) -> Result<CertSigner, Error> {
        CertSigner::from_authority(&self.hosting_store()?, authority).await
    }
}

/// Owns tls.yaml and the source capability, not an SNI resolver or HTTP host.
pub struct CertificateProvider {
    state: AppState,
    config_path: PathBuf,
}

impl CertificateProvider {
    pub fn new(state: &AppState) -> Self {
        Self {
            state: state.clone(),
            config_path: state.config_dir().join("tls.yaml"),
        }
    }
}

impl Register<AppState> for CertificateProvider {
    fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
        if state.registry_ref().resolve::<Self>().is_none() {
            state.bind_config_environment::<Self>()?;
            state.registry_ref().register(Arc::new(Self::new(state)))?;
        }
        Ok(())
    }
}

pub fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
    <CertificateProvider as Register<AppState>>::register(state)
}

#[async_trait]
impl Configurable for CertificateProvider {
    type Config = TlsConfigSource;
    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }
    async fn configure(
        &self,
        _: &TlsConfigSource,
    ) -> congif::Result<()> {
        Ok(())
    }
    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for CertificateProvider {
    async fn config_changed(
        &self,
        _: &TlsConfigSource,
    ) -> congif::Result<()> {
        let Some(store) =
            self.state.registry_ref().resolve::<CertificateStore>()
        else {
            return Ok(());
        };
        let config = crate::provider::load_tls_config(&self.state)
            .await
            .map_err(congif::Error::msg)?;
        if let Some(resolver) =
            self.state.registry_ref().resolve::<crate::CertResolver>()
        {
            crate::reload_resolver_certs(&resolver, &self.state)
                .await
                .map_err(congif::Error::msg)?;
        }
        store.files.store(Arc::new(config.certstore().clone()));
        // The registry authority stays boot-pinned, as before.
        Ok(())
    }
}

impl Component<AppState> for CertificateProvider {
    type Deps = HostingDeps;
    fn name(&self) -> &'static str {
        "CertificateProvider"
    }
    fn boot_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }
    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }
    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, HostingDeps> for CertificateProvider {
    async fn boot(
        &self,
        hosting: HostingDeps,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        crate::resolver::install_aws_crypto_provider();
        let config = crate::provider::load_tls_config(state).await?;
        let store = Arc::new(CertificateStore::with_dependencies(
            config.certstore().clone(),
            hosting,
        ));
        if let Some(authority) = store.authority().await? {
            if state.is_worker() {
                tracing::debug!(?authority, "📜 Certificate authority loaded");
            } else {
                tracing::info!(?authority, "📜 Certificate authority loaded");
            }
            state.registry_ref().insert(Arc::new(authority))?;
        }
        state.registry_ref().insert(store)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/source.rs"]
mod tests;
