use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "db")]
use claviron_hosting::HostingStore;
use claviron_types::ssl::certstore::{
    CertEntry, CertStore, CertificateSource, FULLCHAIN_CERT_FILENAME,
    PRIVKEY_FILENAME,
};
use claviron_types::{Aliases, Host, Sni};
use rustls::sign::CertifiedKey;

use super::error::Error;
use super::pem;
use crate::config::TlsConfig;

/// Note: In this design, "default host" (routing fallback) and "default cert"
/// (TLS handshake fallback) are independent. Nginx couples these via
/// `default_server`, but here they are decoupled — the default host will still
/// run with whatever default cert is loaded from the bundle/table.
///
pub struct Snapshot {
    pub cert_map: HashMap<String, Arc<CertifiedKey>>,
    default_cert: Option<Arc<CertifiedKey>>,
    pub cert_cache: HashMap<(PathBuf, PathBuf), Arc<CertifiedKey>>,
    pub(crate) owners: HashMap<String, String>,
}

impl Snapshot {
    fn new() -> Self {
        Snapshot {
            cert_map: HashMap::new(),
            default_cert: None,
            cert_cache: HashMap::new(),
            owners: HashMap::new(),
        }
    }

    /// File IO and PEM/key parsing run under the provider's blocking boundary.
    pub fn with_config(cfg: &TlsConfig) -> Result<Self, Error> {
        let mut snap = Snapshot::new();

        let certstore = cfg.certstore();

        match cfg.default_cert_entry() {
            CertificateSource::Name(name) => {
                let dir = certstore.server_dir(name.as_str());
                let cert_path = dir.join(FULLCHAIN_CERT_FILENAME);
                let key_path = dir.join(PRIVKEY_FILENAME);
                // Only absence of both files permits DB fallback. A broken or
                // inaccessible pair is an operator error, not another source.
                if cert_path.try_exists()? || key_path.try_exists()? {
                    snap.load_default(&cert_path, &key_path)?;
                }
            }
            CertificateSource::Cn(_) => {}
            CertificateSource::Path(path) => {
                let cert_path = certstore.dir().join(path);
                let key_path = cert_path
                    .parent()
                    .ok_or(Error::NoDefaultCert)?
                    .join(PRIVKEY_FILENAME);
                snap.load_default(&cert_path, &key_path)?;
            }
            CertificateSource::Paths(paths) => {
                let cert_path =
                    paths.cert_path().ok_or(Error::NoDefaultCert)?;
                let key_path = paths
                    .key_path()
                    .ok_or(Error::NoKey("default cert".to_string()))?;
                snap.load_default(cert_path, key_path)?;
            }
        }

        // Load entries from the host table. (`insert_ck` is first-wins, so table entries take precedence.)
        snap.populate_from_files(certstore, cfg.sni_entries())?;

        Ok(snap)
    }

    /// Get the default CertifiedKey, if set.
    pub fn default_cert(&self) -> Option<&Arc<CertifiedKey>> {
        self.default_cert.as_ref()
    }

    #[cfg(feature = "db")]
    pub(crate) fn set_default_from_database(
        &mut self,
        cert: &claviron_hosting::Certificate,
    ) -> Result<(), Error> {
        self.default_cert =
            Some(Arc::new(pem::db_cert_to_certified_key(cert)?));
        Ok(())
    }

    fn load_default(
        &mut self,
        cert_path: &PathBuf,
        key_path: &PathBuf,
    ) -> Result<(), Error> {
        let ck = pem::certified_key_from_path_pair(cert_path, key_path)?;
        self.default_cert = Some(Arc::new(ck));
        Ok(())
    }

    /// Insert a [`CertifiedKey`] into the snapshot under the given SNI name.
    ///
    /// If the SNI is not already present in the [`cert_map`], the provided
    /// certificate is stored and associated with that name.
    ///
    /// # Important
    /// Only the **first successfully inserted certificate** for a given SNI
    /// will be used. Any subsequent attempts to insert another certificate
    /// with the same name are ignored, and a debug/warn log is emitted.
    ///
    /// This ensures deterministic resolution during TLS handshakes and avoids
    /// accidental overrides of existing mappings.
    ///
    /// # Errors
    /// This function itself does not return errors, but failure to load the
    /// [`CertifiedKey`] earlier in the pipeline will prevent insertion.
    fn insert_ck(
        &mut self,
        sni: &Sni,
        ck: Arc<CertifiedKey>,
    ) -> bool {
        match self.cert_map.entry(sni.key_string()) {
            Entry::Occupied(e) => {
                tracing::trace!("Duplicate SNI entry: {}", e.key()); // first-wins: ignore new
                false
            }
            Entry::Vacant(v) => {
                v.insert(ck);
                true
            }
        }
    }

    fn populate_from_files(
        &mut self,
        cert_store: &CertStore,
        entries: &[CertEntry],
    ) -> Result<usize, Error> {
        let mut loaded_all = 0;

        for entry in entries {
            let loaded = match entry {
                CertEntry::Name(name) => {
                    let cert_path = cert_store.sever_cert_path(name);
                    let key_path = cert_store.server_key_path(name);
                    self.populate_from_paths(&cert_path, &key_path)?
                }
                CertEntry::Paths(paths) => {
                    let Some(cert_path) = paths.cert_path() else {
                        tracing::warn!(
                            "Ignoring TLS server certificate entry without a certificate path"
                        );
                        continue;
                    };
                    let key_path = paths
                        .key_path()
                        .ok_or(Error::NoKey(cert_path.display().to_string()))?;
                    self.populate_from_paths(cert_path, key_path)?
                }
            };
            loaded_all += loaded;
        }

        Ok(loaded_all)
    }

    // Load PEM certificate + key
    fn populate_from_paths(
        &mut self,
        cert_path: &PathBuf,
        key_path: &PathBuf,
    ) -> Result<usize, Error> {
        let mut loaded = 0;
        let ck = self.load_or_cached_tls(cert_path, key_path)?;

        // Read cert file to derive domains
        let cert_bytes = std::fs::read(cert_path)?;
        let aliases = Aliases::from_cert_bytes(&cert_bytes)
            .map_err(|e| Error::extract_failed(cert_path, e))?;
        for alias in aliases {
            if let Host::Dns(sni) = alias
                && self.insert_ck(&sni, ck.clone())
            {
                tracing::trace!(
                    "📜 PEM alias '{}' added to the snapshot",
                    sni.as_str()
                );
                loaded += 1;
            }
        }
        Ok(loaded)
    }

    /// Load a [`CertifiedKey`] from the given certificate and private key paths,
    /// using this snapshot’s internal cache to avoid reparsing the same files.
    ///
    /// If the `(cert_path, key_path)` pair was already loaded, the cached
    /// [`Arc<CertifiedKey>`] is returned. Otherwise, the files are read and a new
    /// [`CertifiedKey`] is constructed and inserted into the cache.
    ///
    /// # Important
    /// Only the **first successfully loaded certificate** for a given path pair
    /// is kept in the cache. If the underlying files are modified later on,
    /// the cached key will still be returned until the entire snapshot is rebuilt.
    ///
    /// # Errors
    /// Returns an [`Error`] if either file cannot be read or if the certificate /
    /// private key data is invalid.
    fn load_or_cached_tls(
        &mut self,
        cert_path: &PathBuf,
        key_path: &PathBuf,
    ) -> Result<Arc<CertifiedKey>, Error> {
        let key = (cert_path.clone(), key_path.clone());
        match self.cert_cache.entry(key) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(v) => {
                let ck = Arc::new(pem::certified_key_from_path_pair(
                    cert_path, key_path,
                )?);
                Ok(v.insert(ck).clone())
            }
        }
    }
}

#[cfg(feature = "db")]
impl Snapshot {
    /// Load and populate snapshot from database
    /// Returns count of SNI entries added (CN + aliases)
    pub async fn populate_from_db(
        &mut self,
        store: &HostingStore,
        server_id: i32,
    ) -> Result<usize, Error> {
        use std::str::FromStr;

        // Load DB certificates with their aliases for this server
        let db_certs =
            store.certificates_for_server(server_id).await.map_err(|e| {
                Error::Other(format!(
                    "Failed to load certificates from DB: {}",
                    e
                ))
            })?;

        let mut loaded = 0;

        for hosted in db_certs {
            let cert = hosted.certificate;
            let cn = cert.cn.clone();
            // Convert DB cert (DER) to CertifiedKey
            let ck = match pem::db_cert_to_certified_key(&cert) {
                Ok(ck) => Arc::new(ck),
                Err(e) => {
                    tracing::warn!("Failed to convert DB cert '{}': {}", cn, e);
                    continue;
                }
            };

            // Insert for CN (Common Name)
            if let Ok(cn_sni) = Sni::from_str(&cn) {
                if self.insert_ck(&cn_sni, ck.clone()) {
                    self.owners
                        .insert(cn_sni.key_string(), cn.to_ascii_lowercase());
                    tracing::trace!("🗄️  DB cert CN '{}' added", cn);
                    loaded += 1;
                }
            } else {
                tracing::warn!("Invalid CN '{}', skipping", cn);
                continue;
            }

            // Insert for each SAN (Subject Alternative Name)
            for alias_model in hosted.aliases {
                if let Ok(alias_sni) = Sni::from_str(&alias_model.hostname) {
                    if self.insert_ck(&alias_sni, ck.clone()) {
                        self.owners.insert(
                            alias_sni.key_string(),
                            cn.to_ascii_lowercase(),
                        );
                        loaded += 1;
                    }
                } else {
                    tracing::warn!(
                        "Invalid SAN '{}', skipping",
                        alias_model.hostname
                    );
                }
            }
        }

        Ok(loaded)
    }
}
