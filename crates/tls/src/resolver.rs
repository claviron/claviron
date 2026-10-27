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
        Self {
            published: ArcSwap::from_pointee(Published {
                certs: cert_map,
                default_cert,
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
            next.certs.insert(key, ck);
        });
    }

    /// Remove the certificate for a hostname, if present.
    pub fn remove_host(
        &self,
        host: &str,
    ) {
        self.update(|next| {
            next.certs.remove(&host.to_ascii_lowercase());
        });
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
        self.update(|next| {
            let Some(target) = next.certs.get(&key).cloned() else {
                return 0;
            };
            let before = next.certs.len();
            next.certs.retain(|_, arc| !Arc::ptr_eq(arc, &target));
            before - next.certs.len()
        })
    }

    /// Returns a list of all hostnames currently loaded in the resolver.
    pub fn hostnames(&self) -> Vec<String> {
        let snapshot = self.published.load();
        snapshot.certs.keys().cloned().collect()
    }

    /// Change the default certificate used on SNI miss.
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
        let _writer = self.writers.lock().unwrap_or_else(|e| e.into_inner());
        self.published.store(Arc::new(Published {
            certs: new_map,
            default_cert: new_default,
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
            // Some(self.published.load().default_cert.clone())
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
