use std::sync::Arc;

use claviron_runtime::AppState;
use rustls::ServerConfig;
use rustls::server::ServerSessionMemoryCache;

use super::error::Error;
use super::proto::Proto;
use super::resolver::CertResolver;
use crate::authority::CertAuthority;
use crate::config::TlsConfig;
use crate::resolver::CertResolverProvider;
use crate::snapshot::Snapshot;

/// What certificate loading reads from hosting: the server id
/// `HostingService` resolves in its boot, and the store behind it. Absent when
/// hosting is not registered, in which case the database steps are skipped or
/// refused as each one states. Nothing without `db`.
#[cfg(feature = "db")]
pub type HostingDeps = (Option<Arc<claviron_hosting::HostingService>>,);
#[cfg(not(feature = "db"))]
pub type HostingDeps = ();

/// The prepared `tls.yaml`, resolved: the certstore it names is loaded under
/// a blocking boundary, so boot and reload share one path and the reader does
/// no file IO. Congif prepared the declaration before boot and in the reload
/// batch; nothing here opens the file.
pub(crate) async fn load_tls_config(
    state: &AppState
) -> Result<TlsConfig, Error> {
    let source = state
        .config_reader()
        .snapshot::<CertResolverProvider>()
        .map_err(|error| Error::Other(error.to_string()))?;
    let cfg = tokio::task::spawn_blocking(move || {
        TlsConfig::try_from((*source).clone())
    })
    .await
    .map_err(|error| {
        Error::Other(format!("TLS config blocking task failed: {error}"))
    })?
    .map_err(Error::Other)?;
    // Published before any rustls config is built, so every one of them is
    // built the same way.
    crate::policy::configure(cfg.policy.clone())?;

    Ok(cfg)
}

/// A declaration written inline, resolved the way `load_tls_config` resolves
/// the prepared one: for the tests, which have no reader.
#[cfg(test)]
pub(crate) async fn parse_tls_config(yaml: String) -> Result<TlsConfig, Error> {
    let source: crate::config::TlsConfigSource = serde_yaml::from_str(&yaml)
        .map_err(|error| Error::Other(error.to_string()))?;
    tokio::task::spawn_blocking(move || TlsConfig::try_from(source))
        .await
        .map_err(|error| {
            Error::Other(format!("TLS config blocking task failed: {error}"))
        })?
        .map_err(Error::Other)
}

/// The certificate material this process lends every TLS consumer: the
/// resolver that answers by name, and the authority its own certificates
/// chain to. Both come from one read of `tls.yaml`, so no consumer opens the
/// certificate directory for itself.
pub struct Certificates {
    pub resolver: Arc<CertResolver>,
    pub authority: Option<Arc<CertAuthority>>,
}

/// Named authorities resolve from hosting when DB support is enabled.
/// Explicit trust-only PEM paths retain the blocking file boundary.
async fn load_authority(
    _hosting: &HostingDeps,
    config: &TlsConfig,
) -> Result<Option<Arc<CertAuthority>>, Error> {
    use claviron_types::ssl::certstore::CertificateSource;
    let entry = config.certstore().authority_entry();
    let named = match entry {
        Some(CertificateSource::Cn(name)) => Some(name),
        Some(CertificateSource::Name(name)) => {
            let path = config.certstore().ca_cert_path(name.as_str());
            if tokio::fs::try_exists(&path).await? { None } else { Some(name) }
        }
        _ => None,
    };
    if let Some(name) = named {
        #[cfg(feature = "db")]
        {
            let (service,) = _hosting;
            let service = service.as_ref().ok_or_else(|| {
                Error::Other("named authority requires HostingService".into())
            })?;
            let store = service
                .store()
                .map_err(|error| Error::Other(error.to_string()))?;
            return CertAuthority::from_issuer(&store, name.as_str())
                .await
                .map(|authority| Some(Arc::new(authority)));
        }
        #[cfg(not(feature = "db"))]
        {
            return Err(Error::Other(format!(
                "authority CN '{}' requires database support; select authority.path for file trust",
                name.as_str()
            )));
        }
    }
    let Some(path) = CertAuthority::path_of(config) else {
        return Ok(None);
    };
    tokio::task::spawn_blocking(move || CertAuthority::load(path).map(Arc::new))
        .await
        .map_err(|error| {
            Error::Other(format!("TLS authority blocking task failed: {error}"))
        })?
        .map(Some)
}

/// A named fallback is resolved only after the file snapshot was loaded.
async fn load_default_from_database(
    _hosting: &HostingDeps,
    snapshot: &mut Snapshot,
    source: &claviron_types::ssl::certstore::CertificateSource,
) -> Result<(), Error> {
    use claviron_types::ssl::certstore::CertificateSource;
    if snapshot.default_cert().is_some() {
        return Ok(());
    }
    let name = match source {
        CertificateSource::Name(name) | CertificateSource::Cn(name) => name,
        _ => return Err(Error::NoDefaultCert),
    };
    #[cfg(feature = "db")]
    {
        let (service,) = _hosting;
        let service = service.as_ref().ok_or_else(|| Error::Other(format!("default certificate '{}' not found in files; requires HostingService", name.as_str())))?;
        let store =
            service.store().map_err(|error| Error::Other(error.to_string()))?;
        let cert = store
            .certificate_by_cn(name.as_str())
            .await
            .map_err(|error| Error::Other(error.to_string()))?
            .ok_or_else(|| {
                Error::Other(format!(
                    "default certificate '{}' not found in hosting",
                    name.as_str()
                ))
            })?
            .certificate;
        if !cert.enabled
            || cert.kind != claviron_hosting::CertificateKind::Server
        {
            return Err(Error::Other(
                "default certificate must be an enabled server certificate"
                    .into(),
            ));
        }
        let server_id = service
            .require_server_id()
            .map_err(|error| Error::Other(error.to_string()))?;
        if !store
            .servers_for_certificate(name.as_str())
            .await
            .map_err(|error| Error::Other(error.to_string()))?
            .iter()
            .any(|server| server.id == server_id)
        {
            return Err(Error::Other(
                "default certificate is not assigned to this server".into(),
            ));
        }
        snapshot.set_default_from_database(&cert)?;
        Ok(())
    }
    #[cfg(not(feature = "db"))]
    Err(Error::Other(format!(
        "default certificate '{}' not found in files; database support is disabled",
        name.as_str()
    )))
}

/// PEM file IO, signing-key construction and SAN parsing are synchronous.
async fn load_file_snapshot(config: TlsConfig) -> Result<Snapshot, Error> {
    tokio::task::spawn_blocking(move || Snapshot::with_config(&config))
        .await
        .map_err(|error| {
            Error::Other(format!("TLS snapshot blocking task failed: {error}"))
        })?
}

/// Pull DB-managed certificates onto the snapshot. Reads the `server_id`
/// `HostingService` resolved in its boot and builds a stable HostingStore over
/// the registered `DbService` pool. Skipped (no error) when either is
/// unavailable — the file-backed snapshot is still valid.
#[cfg(feature = "db")]
async fn merge_db_certs(
    snapshot: &mut Snapshot,
    (hosting,): &HostingDeps,
    state: &AppState,
) -> Result<(), Error> {
    use claviron_db::DbService;
    use claviron_hosting::HostingStore;

    let Some(hosting) = hosting else {
        tracing::debug!(
            "HostingService not registered; skipping DB cert merge"
        );
        return Ok(());
    };
    let server_id = hosting
        .require_server_id()
        .map_err(|error| Error::Other(error.to_string()))?;
    let Some(db) = state.registry_ref().resolve::<DbService>() else {
        tracing::debug!("DbService not registered; skipping DB cert merge");
        return Ok(());
    };
    let store = match HostingStore::open(&db) {
        Ok(store) => store,
        Err(error) => {
            tracing::debug!("{error}; skipping DB cert merge");
            return Ok(());
        }
    };
    let added = snapshot.populate_from_db(&store, server_id).await?;
    if state.is_worker() {
        tracing::debug!(
            "📜 merged {added} DB-managed cert SNI entries into TLS snapshot"
        );
    } else {
        tracing::info!(
            "📜 merged {added} DB-managed cert SNI entries into TLS snapshot"
        );
    }
    Ok(())
}

pub fn create_tls_config(
    protos: &[Proto],
    resolver: Arc<CertResolver>,
) -> Result<ServerConfig, Error> {
    let mut server_config = ServerConfig::builder_with_provider(Arc::new(
        crate::policy::provider(),
    ))
    .with_protocol_versions(&crate::policy::versions())?
    .with_no_client_auth()
    .with_cert_resolver(resolver);

    server_config.alpn_protocols = protos
        .iter()
        .flat_map(|p| p.alpn_versions())
        .map(|s| s.to_vec())
        .collect();

    // Increase the stateful session-cache capacity for TCP/TLS. H3-only
    // configs keep rustls' default session storage; QUIC resumption also goes
    // through the rustls ServerConfig supplied to Quinn.
    let has_h1_h2_tls =
        protos.iter().any(|p| matches!(p, Proto::H1 | Proto::H2));
    if has_h1_h2_tls {
        server_config.session_storage = ServerSessionMemoryCache::new(2048);
    }

    Ok(server_config)
}

pub async fn build_certificates(
    state: &AppState,
    hosting: &HostingDeps,
) -> Result<Certificates, Error> {
    let tls_cfg = load_tls_config(state).await?;
    let authority = load_authority(hosting, &tls_cfg).await?;
    let default_source = tls_cfg.default_cert_entry().clone();

    #[allow(unused_mut)]
    let mut snapshot = load_file_snapshot(tls_cfg).await?;
    load_default_from_database(hosting, &mut snapshot, &default_source).await?;

    #[cfg(feature = "db")]
    merge_db_certs(&mut snapshot, hosting, state).await?;

    let default_cert =
        snapshot.default_cert().ok_or(Error::NoDefaultCert)?.clone();

    Ok(Certificates {
        resolver: Arc::new(CertResolver::new(snapshot.cert_map, default_cert)),
        authority,
    })
}

/// A reload receives no component dependencies, so it resolves the same
/// `HostingDeps` boot was handed.
pub async fn reload_resolver_certs(
    resolver: &CertResolver,
    state: &AppState,
) -> claviron_runtime::registry::Result<()> {
    use claviron_runtime::registry::Resolve;

    let hosting = HostingDeps::resolve(state.registry_ref())?;
    let tls_cfg = load_tls_config(state).await?;
    let default_source = tls_cfg.default_cert_entry().clone();
    #[allow(unused_mut)]
    let mut snapshot = load_file_snapshot(tls_cfg).await?;
    load_default_from_database(&hosting, &mut snapshot, &default_source)
        .await?;

    #[cfg(feature = "db")]
    merge_db_certs(&mut snapshot, &hosting, state).await?;

    let default_cert =
        snapshot.default_cert().ok_or(Error::NoDefaultCert)?.clone();
    resolver.replace_all(snapshot.cert_map, default_cert);
    Ok(())
}

#[cfg(test)]
#[path = "../tests/provider.rs"]
mod tests;
