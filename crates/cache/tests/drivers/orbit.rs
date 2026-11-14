use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use claviron_orbit::Fleet;
use claviron_orbit::cache::{CacheRead, CacheService};

use super::{OrbitCacheDriver, OrbitCacheStore};
use crate::{CacheDriver, CacheStore, Result};

struct MemoryStore {
    values: Mutex<HashMap<String, Bytes>>,
    /// Whether `get_entry` says its values never expire, or leaves the
    /// expiry unknown as a store without that information does.
    expiry_known: bool,
}

impl MemoryStore {
    fn new() -> Self {
        Self { values: Mutex::new(HashMap::new()), expiry_known: false }
    }

    fn with_known_expiry() -> Self {
        Self { expiry_known: true, ..Self::new() }
    }
}

#[async_trait]
impl CacheStore for MemoryStore {
    fn store_name(&self) -> &str {
        "memory"
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

    async fn get_entry(
        &self,
        key: &str,
    ) -> Result<Option<crate::CacheEntry>> {
        let expiry = if self.expiry_known {
            crate::CacheExpiry::Never
        } else {
            crate::CacheExpiry::Unknown
        };
        Ok(self
            .get(key)
            .await?
            .map(|bytes| crate::CacheEntry { bytes, expiry }))
    }

    async fn put(
        &self,
        key: &str,
        value: Cow<'_, [u8]>,
        _ttl: Option<Duration>,
    ) -> Result<()> {
        self.values
            .lock()
            .expect("memory store poisoned")
            .insert(key.to_string(), Bytes::from(value.into_owned()));
        Ok(())
    }

    async fn forget(
        &self,
        key: &str,
    ) -> Result<()> {
        self.values.lock().expect("memory store poisoned").remove(key);
        Ok(())
    }

    async fn clear(&self) -> Result<()> {
        self.values.lock().expect("memory store poisoned").clear();
        Ok(())
    }
}

fn l2_backed_store() -> (OrbitCacheStore, Arc<MemoryStore>) {
    l2_backed_store_with(MemoryStore::new())
}

fn l2_backed_store_with(
    l2: MemoryStore
) -> (OrbitCacheStore, Arc<MemoryStore>) {
    let fleet = Arc::new(Fleet::join("cache_l2", 1).expect("fleet"));
    let service = CacheService::from_fleet(fleet).expect("cache service");
    let store =
        OrbitCacheStore::new("orbit", service, 8, Some("memory".to_string()))
            .expect("orbit store");
    let l2 = Arc::new(l2);
    store.attach_l2(l2.clone()).expect("attach L2");
    (store, l2)
}

#[test]
fn driver_registers_multiple_logical_orbit_stores() {
    let driver = Arc::new(OrbitCacheDriver::new());
    let config = crate::config::CacheStoreConfig {
        driver: "orbit".to_string(),
        options: HashMap::new(),
    };

    let models =
        driver.clone().create_store("models", &config).expect("models");
    let responses =
        driver.clone().create_store("responses", &config).expect("responses");

    assert_eq!(models.store_name(), "models");
    assert_eq!(responses.store_name(), "responses");
    assert!(driver.has_stores());
}

#[tokio::test]
async fn l2_is_a_fallback_and_writes_through_before_l1_publication() {
    let (store, l2) = l2_backed_store();
    l2.put("cold", b"from-l2"[..].into(), None).await.expect("seed L2");

    assert_eq!(
        CacheStore::get(&store, "cold").await.expect("get"),
        Some(Bytes::from_static(b"from-l2"))
    );
    // Its expiry is unknown, so it answers the read and is not kept.
    assert_eq!(
        store.cache().read_local(b"cold").expect("local read"),
        CacheRead::Miss
    );

    CacheStore::put(&store, "hot", b"fresh"[..].into(), None)
        .await
        .expect("put");
    assert_eq!(
        l2.get("hot").await.expect("L2 get"),
        Some(Bytes::from_static(b"fresh"))
    );
    assert!(matches!(
        store.cache().read_local(b"hot").expect("local read"),
        CacheRead::Hit(_)
    ));

    CacheStore::forget(&store, "hot").await.expect("forget");
    assert_eq!(l2.get("hot").await.expect("L2 get"), None);
    assert_eq!(
        store.cache().read_local(b"hot").expect("local read"),
        CacheRead::Miss
    );
}

#[tokio::test]
async fn an_l2_value_with_a_known_expiry_is_kept_until_a_write_replaces_it() {
    let (store, l2) = l2_backed_store_with(MemoryStore::with_known_expiry());
    l2.put("cold", b"from-l2"[..].into(), None).await.expect("seed L2");

    assert_eq!(
        CacheStore::get(&store, "cold").await.expect("get"),
        Some(Bytes::from_static(b"from-l2"))
    );
    assert!(matches!(
        store.cache().read_local(b"cold").expect("local read"),
        CacheRead::Hit(entry) if entry.value == Bytes::from_static(b"from-l2")
    ));

    CacheStore::put(&store, "cold", b"newer"[..].into(), None)
        .await
        .expect("put");
    assert!(matches!(
        store.cache().read_local(b"cold").expect("local read"),
        CacheRead::Hit(entry) if entry.value == Bytes::from_static(b"newer")
    ));
}

#[tokio::test]
async fn orbit_validation_runs_before_l2_write() {
    let (store, l2) = l2_backed_store();
    let oversized_key = "k".repeat(2_000);

    assert!(
        CacheStore::put(&store, &oversized_key, b"value"[..].into(), None)
            .await
            .is_err()
    );
    assert_eq!(l2.get(&oversized_key).await.expect("L2 get"), None);
}

#[tokio::test]
async fn ttl_write_is_rejected_before_a_non_ttl_l2_is_changed() {
    let (store, l2) = l2_backed_store();

    assert!(
        CacheStore::put(
            &store,
            "expiring",
            b"value"[..].into(),
            Some(Duration::from_secs(1))
        )
        .await
        .is_err()
    );
    assert_eq!(l2.get("expiring").await.expect("L2 get"), None);
    assert_eq!(
        store.cache().read_local(b"expiring").expect("local read"),
        CacheRead::Miss
    );
}

#[tokio::test]
async fn reset_clears_l2_before_publishing_the_reset() {
    let (store, l2) = l2_backed_store();
    CacheStore::put(&store, "value", b"cached"[..].into(), None)
        .await
        .expect("put");

    CacheStore::clear(&store).await.expect("clear");

    assert_eq!(l2.get("value").await.expect("L2 get"), None);
    assert_eq!(
        store.cache().read_local(b"value").expect("local read"),
        CacheRead::Miss
    );
}

#[tokio::test]
async fn shm_store_reads_before_l2_and_keeps_existing_miss_behavior() {
    let fleet = Arc::new(Fleet::join("cache_shm_l2", 1).expect("fleet"));
    let service = CacheService::from_fleet(fleet).expect("cache service");
    let driver = Arc::new(OrbitCacheDriver { service: service.clone() });
    let config =
        serde_yaml::from_str("driver: orbit\nmode: shm\nl2: saved").unwrap();
    let store = driver.clone().create_store("shared", &config).unwrap();
    let orbit = store.as_any().downcast_ref::<OrbitCacheStore>().unwrap();
    let l2 = Arc::new(MemoryStore::new());
    orbit.attach_l2(l2.clone()).unwrap();

    orbit.cache().put("hot", b"orbit", None).await.unwrap();
    l2.put("hot", b"backing"[..].into(), None).await.unwrap();
    l2.put("cold", b"backing"[..].into(), None).await.unwrap();
    assert_eq!(
        store.get("hot").await.unwrap(),
        Some(Bytes::from_static(b"orbit"))
    );
    assert_eq!(
        store.get("cold").await.unwrap(),
        Some(Bytes::from_static(b"backing"))
    );
    assert_eq!(orbit.cache().read_local(b"cold").unwrap(), CacheRead::Miss);
    assert!(service.poll_mutations().unwrap().is_empty());

    let invalid =
        serde_yaml::from_str("driver: orbit\nmode: shm\nl1_capacity: 5")
            .unwrap();
    assert!(driver.create_store("invalid", &invalid).is_err());
}
