use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use claviron_runtime::AppState;
use claviron_runtime::registry::Reloadable;

use super::{CacheRegistry, validate_orbit_reload};
use crate::config::{CacheConfig, CacheStoreConfig};
use crate::{CacheDriver, CacheProvider, CacheStore, CacheValue, Result};

struct MemoryDriver;

struct MemoryStore {
    name: String,
    values: Mutex<HashMap<String, Bytes>>,
}

impl CacheDriver for MemoryDriver {
    fn driver_name(&self) -> &'static str {
        "memory"
    }

    fn create_store(
        self: Arc<Self>,
        store_name: &str,
        _config: &CacheStoreConfig,
    ) -> Result<Arc<dyn CacheStore>> {
        Ok(Arc::new(MemoryStore {
            name: store_name.to_string(),
            values: Mutex::new(HashMap::new()),
        }))
    }
}

#[async_trait]
impl CacheStore for MemoryStore {
    fn store_name(&self) -> &str {
        &self.name
    }

    fn driver_name(&self) -> &str {
        "memory"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn supports_ttl(&self) -> bool {
        false
    }

    async fn get(
        &self,
        key: &str,
    ) -> Result<Option<Bytes>> {
        Ok(self.values.lock().expect("memory store poisoned").get(key).cloned())
    }

    async fn put(
        &self,
        key: &str,
        value: &[u8],
        _ttl: Option<Duration>,
    ) -> Result<()> {
        self.values
            .lock()
            .expect("memory store poisoned")
            .insert(key.to_string(), Bytes::copy_from_slice(value));
        Ok(())
    }

    async fn forget(
        &self,
        key: &str,
    ) -> Result<()> {
        self.values.lock().expect("memory store poisoned").remove(key);
        Ok(())
    }
}

fn config() -> CacheConfig {
    CacheConfig {
        default: "custom".to_string(),
        stores: HashMap::from([(
            "custom".to_string(),
            CacheStoreConfig {
                driver: "memory".to_string(),
                options: HashMap::new(),
            },
        )]),
    }
}

fn memory_store(name: &str) -> Arc<MemoryStore> {
    Arc::new(MemoryStore {
        name: name.to_string(),
        values: Mutex::new(HashMap::new()),
    })
}

#[test]
fn supplied_config_has_no_external_file_or_reload_contract() {
    use claviron_runtime::registry::Component;
    let provider = Arc::new(CacheProvider::from_config(config()));
    assert!(provider.as_configurable().is_none());
    assert!(provider.as_reloadable().is_none());
    let registry = CacheRegistry::new(provider.clone());
    registry.register_driver(Arc::new(MemoryDriver)).unwrap();
    registry.configure(config()).unwrap();
    registry.seal().unwrap();
    assert_eq!(provider.default_store_name().unwrap(), "custom");
}

#[tokio::test]
async fn third_party_driver_builds_a_generic_repository() {
    let provider = Arc::new(CacheProvider::new("cache.yaml".into()));
    let registry = CacheRegistry::new(provider.clone());
    registry.configure(config()).expect("configure");
    registry.register_driver(Arc::new(MemoryDriver)).expect("register driver");
    registry.seal().expect("seal cache");

    let repository = provider.default_store().expect("default repository");
    repository
        .put_value("answer", CacheValue::I64(42), None)
        .await
        .expect("put value");
    assert_eq!(
        repository.get_value("answer").await.expect("get value"),
        Some(CacheValue::I64(42))
    );
    assert!(registry.register_driver(Arc::new(MemoryDriver)).is_err());
}

#[tokio::test]
async fn ready_store_can_use_driver_owned_configuration() {
    let provider = Arc::new(CacheProvider::new("cache.yaml".into()));
    let config =
        CacheConfig { default: "external".to_string(), stores: HashMap::new() };
    let registry = CacheRegistry::new(provider.clone());
    registry.configure(config).expect("configure");
    registry.register_store(memory_store("external")).expect("register store");
    registry.seal().expect("seal cache");

    provider.put("key", b"value", None).await.expect("put");
    assert_eq!(
        provider.get("key").await.expect("get"),
        Some(Bytes::from_static(b"value"))
    );
}

#[tokio::test]
async fn reload_catalog_swap_keeps_resolved_repositories_alive() {
    let provider = Arc::new(CacheProvider::new("cache.yaml".into()));
    let registry = CacheRegistry::new(provider.clone());
    registry.configure(config()).expect("configure");
    registry.register_driver(Arc::new(MemoryDriver)).expect("register driver");
    registry.seal().expect("seal cache");

    let old = provider.default_store().expect("old repository");
    old.put("key", b"old", None).await.expect("old put");

    let mut next = config();
    next.default = "other".to_string();
    next.stores.insert(
        "other".to_string(),
        CacheStoreConfig {
            driver: "memory".to_string(),
            options: HashMap::new(),
        },
    );
    let candidate =
        registry.build_reload_catalog(next).expect("reload catalog");
    provider.publish(candidate);

    assert_eq!(
        old.get("key").await.expect("old get"),
        Some(Bytes::from_static(b"old"))
    );
    assert_eq!(provider.default_store_name().expect("default store"), "other");
    assert_eq!(provider.get("key").await.expect("new get"), None);
}

#[tokio::test]
async fn provider_reload_reads_and_publishes_cache_yaml() {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let config_dir = std::env::temp_dir()
        .join(format!("claviron-cache-reload-{}-{suffix}", std::process::id()));
    tokio::fs::create_dir_all(&config_dir).await.expect("create config dir");
    let config_path = config_dir.join("cache.yaml");

    let provider = Arc::new(CacheProvider::new(config_path.clone()));
    let registry = Arc::new(CacheRegistry::new(provider.clone()));
    registry.configure(config()).expect("configure");
    provider
        .attach_registry(Arc::downgrade(&registry))
        .expect("attach registry");
    registry.register_driver(Arc::new(MemoryDriver)).expect("register driver");
    registry.seal().expect("seal cache");

    tokio::fs::write(
        &config_path,
        r#"
default: other
stores:
  custom:
    driver: memory
  other:
    driver: memory
"#,
    )
    .await
    .expect("write cache config");
    let state = AppState::standalone("cache-reload-test", &config_dir);
    // The reload takes the document Congif prepared, as the runtime's reload
    // batch would have.
    state
        .config_reader()
        .prepare_owners(&[provider.as_ref()])
        .await
        .expect("prepare cache.yaml");
    <CacheProvider as Reloadable<AppState>>::reload(provider.as_ref(), &state)
        .await
        .expect("reload provider");

    assert_eq!(provider.default_store_name().expect("default store"), "other");
    tokio::fs::remove_file(&config_path).await.expect("remove cache config");
    tokio::fs::remove_dir(&config_dir).await.expect("remove config dir");
}

#[test]
fn failed_reload_build_does_not_replace_the_active_catalog() {
    let provider = Arc::new(CacheProvider::new("cache.yaml".into()));
    let registry = CacheRegistry::new(provider.clone());
    registry.configure(config()).expect("configure");
    registry.register_driver(Arc::new(MemoryDriver)).expect("register driver");
    registry.seal().expect("seal cache");
    let before = provider.default_store().expect("repository");

    let invalid =
        CacheConfig { default: "missing".to_string(), stores: HashMap::new() };
    assert!(registry.build_reload_catalog(invalid).is_err());

    let after = provider.default_store().expect("repository");
    assert!(Arc::ptr_eq(&before, &after));
}

#[test]
fn orbit_store_and_backing_topology_require_restart() {
    let current: CacheConfig = serde_yaml::from_str(
        r#"
default: fleet
stores:
  fleet:
    driver: orbit
    l1_capacity: 100
    l2: saved
  saved:
    driver: file
    path: storage/cache/one
"#,
    )
    .expect("current config");

    let mut candidate = current.clone();
    candidate.stores.insert(
        "other".to_string(),
        CacheStoreConfig {
            driver: "memory".to_string(),
            options: HashMap::new(),
        },
    );
    validate_orbit_reload(&current, &candidate)
        .expect("unrelated store may change");

    let mut disabled = current.clone();
    disabled.stores.remove("fleet");
    validate_orbit_reload(&current, &disabled)
        .expect("Orbit store may be disabled");

    let mut enabled = current.clone();
    enabled.stores.insert(
        "session".to_string(),
        CacheStoreConfig {
            driver: "orbit".to_string(),
            options: HashMap::from([(
                "l1_capacity".to_string(),
                serde_yaml::Value::Number(100.into()),
            )]),
        },
    );
    validate_orbit_reload(&current, &enabled)
        .expect("Orbit store may be enabled");

    candidate.stores.get_mut("saved").expect("saved config").options.insert(
        "path".to_string(),
        serde_yaml::Value::String("storage/cache/two".into()),
    );
    assert!(validate_orbit_reload(&current, &candidate).is_err());

    let mut candidate = current.clone();
    candidate.stores.get_mut("fleet").expect("fleet config").options.insert(
        "l1_capacity".to_string(),
        serde_yaml::Value::Number(200.into()),
    );
    assert!(validate_orbit_reload(&current, &candidate).is_err());
}
