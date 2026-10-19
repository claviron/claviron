use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Once};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron_runtime::registry::{Bootable, Component, Reloadable, priority};
use claviron_runtime::{AppState, Register};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use tracing::{debug, error, info, trace, warn};

use crate::config::TlsConfigSource;
use crate::{HostingDeps, build_certificates, reload_resolver_certs};

/// The owner of `tls.yaml`, and the provider whose boot builds the shared
/// [`CertResolver`] from it. Congif prepares the declaration before boot and
/// in the reload batch; the certstore and certificates resolve in boot, once
/// hosting and the database are up.
pub struct CertResolverProvider {
    state: AppState,
    config_path: PathBuf,
}

impl CertResolverProvider {
    pub fn new(state: &AppState) -> Self {
        Self {
            state: state.clone(),
            config_path: state.config_dir().join("tls.yaml"),
        }
    }
}

impl Register<AppState> for CertResolverProvider {
    fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
        state.bind_config_environment::<Self>()?;
        state.registry_ref().register(Arc::new(Self::new(state)))?;
        Ok(())
    }
}

/// Register the resolver provider with the application's configuration reader.
pub fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
    <CertResolverProvider as Register<AppState>>::register(state)
}

/// `tls.yaml` is required where TLS is built in; there is no absent form.
#[async_trait]
impl Configurable for CertResolverProvider {
    type Config = TlsConfigSource;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    async fn configure(
        &self,
        _config: &TlsConfigSource,
    ) -> congif::Result<()> {
        // Certificates resolve in boot: hosting and the database come first.
        Ok(())
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for CertResolverProvider {
    async fn config_changed(
        &self,
        _config: &TlsConfigSource,
    ) -> congif::Result<()> {
        let Some(resolver) =
            self.state.registry_ref().resolve::<CertResolver>()
        else {
            // Not booted yet: boot reads the prepared declaration itself.
            return Ok(());
        };
        reload_resolver_certs(&resolver, &self.state)
            .await
            .map_err(congif::Error::msg)?;
        info!(
            "📜 Certificate resolver reconfigured (certs={})",
            resolver.count()
        );
        Ok(())
    }
}

/// Custom Certificate Resolver for Multi-Domain TLS (hot-reloadable)
#[derive(Debug)]
pub struct CertResolver {
    // Snapshot of SNI → CertifiedKey map; atomically swappable for hot-reload
    certs: ArcSwap<HashMap<String, Arc<CertifiedKey>>>,
    // Default certificate used when SNI is missing or not found; atomically swappable
    default_cert: ArcSwap<CertifiedKey>,
}

impl CertResolver {
    pub fn new(
        cert_map: HashMap<String, Arc<CertifiedKey>>,
        default_cert: Arc<CertifiedKey>,
    ) -> Self {
        Self {
            certs: ArcSwap::from(Arc::new(cert_map)),
            default_cert: ArcSwap::from(default_cert),
        }
    }

    /// Insert or replace certificate for a hostname. Hostnames are stored lowercased.
    pub fn upsert_host(
        &self,
        host: impl AsRef<str>,
        ck: Arc<CertifiedKey>,
    ) {
        let key = host.as_ref().to_ascii_lowercase();
        let mut next = (**self.certs.load()).clone();
        next.insert(key, ck);
        self.certs.store(Arc::new(next));
    }

    /// Remove the certificate for a hostname, if present.
    pub fn remove_host(
        &self,
        host: &str,
    ) {
        let mut next = (**self.certs.load()).clone();
        next.remove(&host.to_ascii_lowercase());
        self.certs.store(Arc::new(next));
    }

    /// Remove the certificate identified by `cn` together with **every other
    /// hostname pointing to the same `Arc<CertifiedKey>`** (CN entry + all
    /// aliases that were installed via [`Self::upsert_host`] with the same
    /// `Arc::clone`). Returns the number of hostname entries removed.
    ///
    /// **What:** Looks up the `Arc<CertifiedKey>` registered under `cn`,
    /// then retains only the entries whose value points to a *different*
    /// allocation. Pointer identity is the load-bearing invariant —
    /// `CertificateManager::upsert_with_modal` always installs a single
    /// `Arc::clone` chain per cert, so "same Arc" ⇔ "same physical cert".
    ///
    /// **Why:** when a certificate row is deleted from the database, the
    /// notify listener has the CN but typically **not** the alias list —
    /// `ON DELETE CASCADE` on `cert_aliases` may have already removed the
    /// SAN rows by the time the listener queries. Identifying "everything
    /// attached to this cert" by Arc pointer identity is `O(n)` in the
    /// cert map (small, no allocations beyond the snapshot clone) but
    /// exact — it doesn't depend on DB state at the moment of removal.
    ///
    /// **Use when:** purging a cert wholesale — DB DELETE handler for
    /// `hosting.certificates`, or any operator-driven "drop this cert
    /// everywhere" path.
    ///
    /// **Don't use when:** removing a single SAN/alias — use
    /// [`Self::remove_host`] instead. Calling `remove_cert` on a SAN's hostname
    /// would still remove the whole cert (every alias shares the same
    /// Arc), which is almost certainly not what you want.
    pub fn remove_cert(
        &self,
        cn: &str,
    ) -> usize {
        let key = cn.to_ascii_lowercase();
        let mut next = (**self.certs.load()).clone();

        let Some(target) = next.get(&key) else {
            return 0;
        };
        let target_ptr = Arc::as_ptr(target) as usize;

        let before = next.len();
        next.retain(|_, arc| Arc::as_ptr(arc) as usize != target_ptr);
        let removed = before.saturating_sub(next.len());

        if removed > 0 {
            self.certs.store(Arc::new(next));
        }
        removed
    }

    /// Returns a list of all hostnames currently loaded in the resolver.
    pub fn hostnames(&self) -> Vec<String> {
        let map = self.certs.load(); // Arc<HashMap<...>>
        map.keys().cloned().collect()
    }

    /// Change the default certificate used on SNI miss.
    pub fn set_default(
        &self,
        ck: Arc<CertifiedKey>,
    ) {
        self.default_cert.store(ck);
    }

    pub fn get_default(&self) -> Arc<CertifiedKey> {
        self.default_cert.load_full()
    }

    /// Get certificate for a specific hostname
    pub fn get_cert(
        &self,
        hostname: &str,
    ) -> Option<Arc<CertifiedKey>> {
        let key = hostname.to_ascii_lowercase();
        let map = self.certs.load();
        map.get(&key).cloned()
    }

    pub fn contains(
        &self,
        hostname: &str,
    ) -> bool {
        self.get_cert(hostname).is_some()
    }

    /// Get all available certificates for the provided hostnames.
    /// Returns a HashMap mapping found hostnames (lowercased) to their CertifiedKeys.
    pub fn get_any<'a, I>(
        &self,
        hosts: I,
    ) -> HashMap<String, Arc<CertifiedKey>>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let map = self.certs.load();
        let mut found = HashMap::new();
        for h in hosts {
            let key = h.to_ascii_lowercase();
            if let Some(ck) = map.get(&key) {
                found.insert(key, Arc::clone(ck));
            } else {
                warn!("host {} cert not found in resolver", h);
            }
        }
        found
    }

    /// Atomically replace the entire certificate snapshot and default cert.
    pub fn replace_all(
        &self,
        new_map: HashMap<String, Arc<CertifiedKey>>,
        new_default: Arc<CertifiedKey>,
    ) {
        self.certs.store(Arc::new(new_map));
        self.default_cert.store(new_default);
    }

    pub fn count(&self) -> usize {
        (**self.certs.load()).len()
    }
}

impl ResolvesServerCert for CertResolver {
    fn resolve(
        &self,
        client_hello: ClientHello<'_>,
    ) -> Option<Arc<CertifiedKey>> {
        if let Some(server_name) = client_hello.server_name() {
            let sni = server_name.to_ascii_lowercase();
            debug!(resolver = %format_args!("{:p}", self), process_id = std::process::id(), "🔍 handshake: requested sni: {}", sni);
            // Use an atomic snapshot for lookup
            let map = self.certs.load();
            trace!(resolver = %format_args!("{:p}", self), process_id = std::process::id(), snapshot = %format_args!("{:p}", Arc::as_ptr(&map)), cert_count = map.len(), %sni, "TLS resolver lookup");
            if let Some(cert) = map.get(&sni) {
                debug!(resolver = %format_args!("{:p}", self), "📜 handshake: found cert for {}", sni);
                Some(cert.clone())
            } else {
                #[cfg(debug_assertions)]
                if tracing::enabled!(tracing::Level::TRACE) {
                    let mut hostnames: Vec<_> =
                        map.keys().map(String::as_str).collect();
                    hostnames.sort_unstable();
                    trace!(resolver = %format_args!("{:p}", self), process_id = std::process::id(), snapshot = %format_args!("{:p}", Arc::as_ptr(&map)), cert_count = map.len(), %sni, ?hostnames, "TLS resolver SNI miss before rejection");
                }
                error!(resolver = %format_args!("{:p}", self), "🚫 handshake: unknown sni '{}': - rejecting", sni);
                None
            }
        } else {
            error!(resolver = %format_args!("{:p}", self), process_id = std::process::id(), "🚫 handshake: no-sni provided — rejecting");
            None
            // Some(self.default_cert.load_full())
        }
    }
}

impl Component<AppState> for CertResolver {
    type Deps = ();

    fn name(&self) -> &'static str {
        "CertResolver"
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState> for CertResolver {
    async fn boot(
        &self,
        _deps: (),
        _state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Reloadable<AppState> for CertResolver {
    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        reload_resolver_certs(self, state).await?;
        info!("📜 Certificate resolver reloaded (certs={})", self.count());
        Ok(())
    }
}

impl Component<AppState> for CertResolverProvider {
    /// `merge_db_certs` reads the server id `HostingService` resolves in its
    /// boot, so hosting boots first when it is registered. Without it the
    /// database steps are skipped, which is why the dependency is optional.
    type Deps = HostingDeps;

    fn name(&self) -> &'static str {
        "CertResolverProvider"
    }

    /// EARLY so we boot BEFORE any NORMAL provider (HttpsServer, …) that
    /// resolves `CertResolver` from the registry. Priority only orders this
    /// against the components it has no edge to; `HostingService` comes first
    /// through `Deps`.
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
impl Bootable<AppState, HostingDeps> for CertResolverProvider {
    async fn boot(
        &self,
        hosting: HostingDeps,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        install_aws_crypto_provider();

        let registry = state.registry_ref();
        if registry.resolve::<CertResolver>().is_some() {
            return Ok(());
        }

        let certificates = build_certificates(state, &hosting).await?;
        let count = certificates.resolver.count();
        trace!(resolver = %format_args!("{:p}", Arc::as_ptr(&certificates.resolver)), process_id = std::process::id(), cert_count = count, "Shared TLS resolver registered");
        registry.register(certificates.resolver)?;
        if let Some(authority) = certificates.authority {
            if state.is_worker() {
                tracing::debug!(authority = ?authority, "📜 Certificate authority loaded");
            } else {
                info!(authority = ?authority, "📜 Certificate authority loaded");
            }
            registry.insert(authority)?;
        }
        if state.is_worker() {
            tracing::debug!(
                "📜 Certificate resolver initialized certs={count}"
            );
        } else {
            info!("📜 Certificate resolver initialized certs={count}");
        }
        Ok(())
    }
}

pub fn install_aws_crypto_provider() {
    static TLS_PROVIDER_INIT: Once = Once::new();
    TLS_PROVIDER_INIT.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install rustls aws-lc-rs crypto provider");
    });
}

#[cfg(all(test, feature = "db"))]
#[path = "../tests/resolver.rs"]
mod tests;
