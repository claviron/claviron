use std::collections::HashMap;
use std::sync::{Arc, Mutex, Once};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use claviron_runtime::registry::{Bootable, Component, Reloadable, priority};
use claviron_runtime::{AppState, Register};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use tracing::{debug, error, info, trace, warn};

use crate::{
    CertificateProvider, HostingDeps, build_certificates, reload_resolver_certs,
};

/// Optional SNI consumption of the independently registered certificate source.
pub struct CertResolverProvider;

impl CertResolverProvider {
    pub fn new(_state: &AppState) -> Self {
        Self
    }
}

impl Register<AppState> for CertResolverProvider {
    fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
        <CertificateProvider as Register<AppState>>::register(state)?;
        state.registry_ref().register(Arc::new(Self))?;
        Ok(())
    }
}

pub fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
    <CertResolverProvider as Register<AppState>>::register(state)
}

/// A complete TLS publication. Readers keep one immutable generation.
#[derive(Clone, Debug)]
struct Published {
    certs: HashMap<String, Arc<CertifiedKey>>,
    default_cert: Arc<CertifiedKey>,
    // Only managed bindings carry a canonical CN owner. Key allocation identity
    // is not ownership: rotation replaces the key, and a hostname may move.
    owners: HashMap<String, String>,
}

impl Published {
    fn remove_owned(
        &mut self,
        cn: &str,
    ) -> usize {
        let before = self.certs.len();
        self.certs.retain(|host, _| {
            self.owners.get(host).is_none_or(|owner| owner != cn)
        });
        self.owners.retain(|_, owner| owner != cn);
        before - self.certs.len()
    }
}

/// Hot-reloadable certificate resolver. Handshake reads never acquire the
/// writer lock; cold updates are serialized before cloning and publishing.
#[derive(Debug)]
pub struct CertResolver {
    published: ArcSwap<Published>,
    writers: Mutex<()>,
}

impl CertResolver {
    pub fn new(
        cert_map: HashMap<String, Arc<CertifiedKey>>,
        default_cert: Arc<CertifiedKey>,
    ) -> Self {
        Self::new_owned(cert_map, default_cert, HashMap::new())
    }

    pub(crate) fn new_owned(
        cert_map: HashMap<String, Arc<CertifiedKey>>,
        default_cert: Arc<CertifiedKey>,
        owners: HashMap<String, String>,
    ) -> Self {
        Self {
            published: ArcSwap::from_pointee(Published {
                certs: cert_map,
                default_cert,
                owners,
            }),
            writers: Mutex::new(()),
        }
    }

    fn update<R>(
        &self,
        change: impl FnOnce(&mut Published) -> R,
    ) -> R {
        let _writer = self.writers.lock().unwrap_or_else(|e| e.into_inner());
        let mut next = (**self.published.load()).clone();
        let result = change(&mut next);
        self.published.store(Arc::new(next));
        result
    }

    /// Insert or replace certificate for a hostname. Hostnames are stored lowercased.
    pub fn upsert_host(
        &self,
        host: impl AsRef<str>,
        ck: Arc<CertifiedKey>,
    ) {
        let key = host.as_ref().to_ascii_lowercase();
        self.update(|next| {
            next.owners.remove(&key);
            next.certs.insert(key, ck);
        });
    }

    /// Remove the certificate for a hostname, if present.
    pub fn remove_host(
        &self,
        host: &str,
    ) {
        let host = host.to_ascii_lowercase();
        self.update(|next| {
            next.owners.remove(&host);
            next.certs.remove(&host);
        });
    }

    /// Remove all bindings still owned by a managed certificate's canonical CN.
    /// Hostnames reassigned to another certificate are preserved, even if the
    /// canonical hostname itself was reassigned. Names are case-insensitive.
    ///
    /// For maps installed through `new`, `replace_all` or `upsert_host`, which
    /// carry no managed ownership, remove the unowned entries sharing the CN's
    /// key allocation. Use `remove_host` to remove one such alias.
    pub fn remove_cert(
        &self,
        cn: &str,
    ) -> usize {
        let key = cn.to_ascii_lowercase();
        self.update(|next| {
            let removed = next.remove_owned(&key);
            if removed != 0 || next.owners.contains_key(&key) {
                return removed;
            }
            let Some(target) = next.certs.get(&key).cloned() else {
                return 0;
            };
            let before = next.certs.len();
            next.certs.retain(|host, arc| {
                next.owners.contains_key(host) || !Arc::ptr_eq(arc, &target)
            });
            before - next.certs.len()
        })
    }

    /// DB events may retire only managed bindings, never a file/raw replacement
    /// that happens to occupy the same canonical hostname.
    #[cfg(feature = "db")]
    pub(crate) fn remove_managed_cert(
        &self,
        cn: &str,
    ) -> usize {
        let cn = cn.to_ascii_lowercase();
        self.update(|next| next.remove_owned(&cn))
    }

    /// Publish one managed certificate's CN and complete current alias set.
    /// Parsing happens before this call; retirement and insertion share one
    /// publication, so readers never observe a partially rotated identity.
    #[cfg(feature = "db")]
    pub(crate) fn upsert_cert(
        &self,
        cn: &str,
        aliases: &[String],
        ck: Arc<CertifiedKey>,
    ) {
        let cn = cn.to_ascii_lowercase();
        self.update(|next| {
            next.remove_owned(&cn);
            for host in std::iter::once(&cn).chain(aliases) {
                let host = host.to_ascii_lowercase();
                next.owners.insert(host.clone(), cn.clone());
                next.certs.insert(host, ck.clone());
            }
        });
    }

    /// A delayed alias event cannot remove a name now owned by another cert.
    #[cfg(feature = "postgres")]
    pub(crate) fn remove_alias(
        &self,
        cn: &str,
        host: &str,
    ) {
        let cn = cn.to_ascii_lowercase();
        let host = host.to_ascii_lowercase();
        self.update(|next| {
            if host != cn && next.owners.get(&host) == Some(&cn) {
                next.owners.remove(&host);
                next.certs.remove(&host);
            }
        });
    }

    /// Returns a list of all hostnames currently loaded in the resolver.
    pub fn hostnames(&self) -> Vec<String> {
        let snapshot = self.published.load();
        snapshot.certs.keys().cloned().collect()
    }

    /// Change the default material exposed to TLS consumers. SNI resolution
    /// continues to reject missing or unknown names.
    pub fn set_default(
        &self,
        ck: Arc<CertifiedKey>,
    ) {
        self.update(|next| next.default_cert = ck);
    }

    pub fn get_default(&self) -> Arc<CertifiedKey> {
        self.published.load().default_cert.clone()
    }

    /// Get certificate for a specific hostname
    pub fn get_cert(
        &self,
        hostname: &str,
    ) -> Option<Arc<CertifiedKey>> {
        let key = hostname.to_ascii_lowercase();
        let snapshot = self.published.load();
        let map = &snapshot.certs;
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
        let snapshot = self.published.load();
        let map = &snapshot.certs;
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
    /// This is an authoritative replacement, ordered with incremental updates
    /// at publication. Prepare IO outside the writer lock; a later replacement
    /// supersedes earlier updates rather than merging them.
    pub fn replace_all(
        &self,
        new_map: HashMap<String, Arc<CertifiedKey>>,
        new_default: Arc<CertifiedKey>,
    ) {
        self.replace_owned(new_map, new_default, HashMap::new());
    }

    pub(crate) fn replace_owned(
        &self,
        new_map: HashMap<String, Arc<CertifiedKey>>,
        new_default: Arc<CertifiedKey>,
        owners: HashMap<String, String>,
    ) {
        let _writer = self.writers.lock().unwrap_or_else(|e| e.into_inner());
        self.published.store(Arc::new(Published {
            certs: new_map,
            default_cert: new_default,
            owners,
        }));
    }

    pub fn count(&self) -> usize {
        self.published.load().certs.len()
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
            let snapshot = self.published.load();
            let map = &snapshot.certs;
            trace!(resolver = %format_args!("{:p}", self), process_id = std::process::id(), snapshot = %format_args!("{:p}", Arc::as_ptr(&snapshot)), cert_count = map.len(), %sni, "TLS resolver lookup");
            if let Some(cert) = map.get(&sni) {
                debug!(resolver = %format_args!("{:p}", self), "📜 handshake: found cert for {}", sni);
                Some(cert.clone())
            } else {
                #[cfg(debug_assertions)]
                if tracing::enabled!(tracing::Level::TRACE) {
                    let mut hostnames: Vec<_> =
                        map.keys().map(String::as_str).collect();
                    hostnames.sort_unstable();
                    trace!(resolver = %format_args!("{:p}", self), process_id = std::process::id(), snapshot = %format_args!("{:p}", Arc::as_ptr(&snapshot)), cert_count = map.len(), %sni, ?hostnames, "TLS resolver SNI miss before rejection");
                }
                error!(resolver = %format_args!("{:p}", self), "🚫 handshake: unknown sni '{}': - rejecting", sni);
                None
            }
        } else {
            error!(resolver = %format_args!("{:p}", self), process_id = std::process::id(), "🚫 handshake: no-sni provided — rejecting");
            None
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
        // The selected file may change while tls.yaml stays identical.
        crate::revocation::refresh(state).await?;
        reload_resolver_certs(self, state).await?;
        info!("📜 Certificate resolver reloaded (certs={})", self.count());
        Ok(())
    }
}

impl Component<AppState> for CertResolverProvider {
    /// The source boots after HostingService when present. SNI consumption
    /// follows that source, preserving the database/server-identity ordering.
    type Deps = (Arc<CertificateProvider>,);

    fn name(&self) -> &'static str {
        "CertResolverProvider"
    }

    /// EARLY so we boot BEFORE any NORMAL provider (HttpsServer, …) that
    /// resolves `CertResolver` from the registry. Priority only orders this
    /// against components with no dependency edge. The source and its optional
    /// HostingService dependency come first through `Deps`.
    fn boot_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, (Arc<CertificateProvider>,)> for CertResolverProvider {
    async fn boot(
        &self,
        _source: (Arc<CertificateProvider>,),
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        install_aws_crypto_provider();

        let registry = state.registry_ref();
        if registry.resolve::<CertResolver>().is_some() {
            return Ok(());
        }

        use claviron_runtime::registry::Resolve;
        let hosting = HostingDeps::resolve(registry)?;
        let certificates = build_certificates(state, &hosting).await?;
        let count = certificates.resolver.count();
        trace!(resolver = %format_args!("{:p}", Arc::as_ptr(&certificates.resolver)), process_id = std::process::id(), cert_count = count, "Shared TLS resolver registered");
        registry.register(certificates.resolver)?;
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

#[cfg(test)]
#[path = "../tests/publication.rs"]
mod publication;
