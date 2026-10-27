//! GeoIP country lookup, configured once through Congif.

use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use claviron_runtime::congif::{self, ConfigFile, Configurable};
use claviron_runtime::registry::{Component, Result};
use claviron_runtime::{AppState, Register};
use maxminddb::Reader;
use moka::sync::Cache;
use once_cell::sync::{Lazy, OnceCell};
use serde::Deserialize;
use tracing::error;

/// Locate `geoip.yaml` inside the runtime config directory.
fn geoip_config_path(state: &AppState) -> PathBuf {
    state.config_dir().join("geoip.yaml")
}

static GEO_DB: OnceCell<Arc<Reader<Vec<u8>>>> = OnceCell::new();
static NON_ROUTABLE_CODE: OnceCell<String> = OnceCell::new();

/// The owner of `geoip.yaml`. Congif prepares the file before boot; this
/// provider applies it once and reads nothing itself.
#[derive(Debug)]
pub struct GeoIpProvider {
    config_path: PathBuf,
}

impl GeoIpProvider {
    pub fn new(state: &AppState) -> Self {
        Self { config_path: geoip_config_path(state) }
    }
}

/// Register the GeoIP owner with the application's configuration reader.
pub fn register(state: &AppState) -> Result<()> {
    <GeoIpProvider as Register<AppState>>::register(state)
}

/// Boot-time initializer; call this once during app startup.
pub fn boot_geoip(cfg: &GeoIpConfig) {
    let _ = NON_ROUTABLE_CODE.set(cfg.non_routable.clone());
    if !cfg.enabled {
        return;
    }
    if GEO_DB.get().is_some() {
        return; // already initialized
    }
    match std::fs::read(&cfg.db_path) {
        Ok(bytes) => match Reader::from_source(bytes) {
            Ok(reader) => {
                let _ = GEO_DB.set(Arc::new(reader));
            }
            Err(e) => {
                error!(
                    "GeoIP database '{}' could not be parsed: {e}",
                    cfg.db_path.display()
                );
            }
        },
        Err(e) => {
            error!(
                "GeoIP database could not be opened for reading '{}': {e}",
                cfg.db_path.display()
            );
        }
    }
}

impl Register<AppState> for GeoIpProvider {
    fn register(state: &AppState) -> Result<()> {
        state.bind_config_environment::<Self>()?;
        state.registry_ref().register(Arc::new(Self::new(state)))?;
        Ok(())
    }
}

#[async_trait]
impl Configurable for GeoIpProvider {
    type Config = GeoIpConfig;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    /// No `geoip.yaml` means no database: lookups answer the fallback code.
    fn absent_source(&self) -> Option<GeoIpConfig> {
        Some(GeoIpConfig::disabled())
    }

    async fn configure(
        &self,
        config: &GeoIpConfig,
    ) -> congif::Result<()> {
        boot_geoip(config);
        Ok(())
    }
}

impl Component<AppState> for GeoIpProvider {
    type Deps = ();

    fn name(&self) -> &'static str {
        "GeoIpProvider"
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }
}

// Cache ~ 100k IPs with a 30-minute TTL; tune to your traffic
static GEO_CACHE: Lazy<Cache<IpAddr, String>> = Lazy::new(|| {
    Cache::builder()
        .max_capacity(100_000)
        .time_to_live(Duration::from_secs(1800))
        .build()
});

pub fn geoip_lookup(ip: IpAddr) -> String {
    use maxminddb::PathElement;

    if let Some(hit) = GEO_CACHE.get(&ip) {
        return hit;
    }

    // Skip GeoIP for local/private/reserved ranges
    if is_non_routable(&ip) {
        let code = non_routable_code();
        GEO_CACHE.insert(ip, code.clone());
        return code;
    }

    let db = match GEO_DB.get() {
        Some(db) => db,
        None => {
            let code = non_routable_code();
            GEO_CACHE.insert(ip, code.clone());
            return code;
        }
    };

    let iso: String = match db.lookup(ip) {
        Ok(r) => {
            let country: Option<String> = r
                .decode_path(&[
                    PathElement::Key("country"),
                    PathElement::Key("iso_code"),
                ])
                .ok()
                .flatten();

            let iso = match country {
                Some(code) => Some(code),
                None => r
                    .decode_path(&[
                        PathElement::Key("registered_country"),
                        PathElement::Key("iso_code"),
                    ])
                    .ok()
                    .flatten(),
            };

            iso.unwrap_or_else(non_routable_code)
        }
        Err(_) => non_routable_code(),
    };

    GEO_CACHE.insert(ip, iso.clone());
    iso
}

fn non_routable_code() -> String {
    NON_ROUTABLE_CODE.get().cloned().unwrap_or_else(|| "ZZ".to_string())
}

fn is_non_routable(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unique_local()
                || v6.is_unspecified()
                || v6.is_multicast()
        }
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GeoIpConfig {
    #[serde(default = "default_true")]
    enabled: bool,

    #[serde(default = "default_db_path")]
    db_path: PathBuf,

    #[serde(default = "default_non_routable_code")]
    non_routable: String,
}

fn default_db_path() -> PathBuf {
    "/usr/share/GeoIP/GeoLite2-Country.mmdb".into()
}
fn default_non_routable_code() -> String {
    "ZZ".to_string()
}

impl GeoIpConfig {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// What an absent `geoip.yaml` declares.
    fn disabled() -> Self {
        Self {
            enabled: false,
            db_path: default_db_path(),
            non_routable: default_non_routable_code(),
        }
    }
}
#[inline]
fn default_true() -> bool {
    true
}

#[cfg(test)]
#[path = "../tests/fallback.rs"]
mod fallback_tests;
