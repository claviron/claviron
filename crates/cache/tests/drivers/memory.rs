use super::*;

fn config() -> CacheStoreConfig {
    serde_yaml::from_str("driver: memory\ncapacity: 16").unwrap()
}

#[tokio::test]
async fn memory_cache_stores_own_values_and_expiry() {
    let driver = Arc::new(MemoryCacheDriver);
    let first = driver.clone().create_store("first", &config()).unwrap();
    let second = driver.create_store("second", &config()).unwrap();

    first.put("key", b"first"[..].into(), None).await.unwrap();
    second.put("key", b"second"[..].into(), None).await.unwrap();
    let retained = first.get("key").await.unwrap().unwrap();
    first
        .put("key", b"expired"[..].into(), Some(Duration::ZERO))
        .await
        .unwrap();
    assert!(first.get("key").await.unwrap().is_none());

    first.put("key", b"replacement"[..].into(), None).await.unwrap();
    assert_eq!(first.get("key").await.unwrap().unwrap(), b"replacement"[..]);
    first.clear().await.unwrap();
    assert!(first.get("key").await.unwrap().is_none());
    assert_eq!(retained, b"first"[..]);
    assert_eq!(second.get("key").await.unwrap().unwrap(), b"second"[..]);
    second.forget("key").await.unwrap();
    assert!(second.get("key").await.unwrap().is_none());
}

#[tokio::test]
async fn backing_fill_keeps_original_expiry() {
    let driver = Arc::new(MemoryCacheDriver);
    let config = serde_yaml::from_str("driver: memory\nl2: saved").unwrap();
    let l1 = driver.clone().create_store("local", &config).unwrap();
    let l2 = Arc::new(MemoryCacheStore::new("saved", MemoryLimit::DEFAULT));
    let stores: std::collections::HashMap<String, Arc<dyn CacheStore>> =
        [("saved".to_owned(), l2.clone() as Arc<dyn CacheStore>)].into();
    driver.finish_store(&l1, &stores).unwrap();

    let start = Instant::now();
    let ttl = Duration::from_secs(60);
    let expiry = start + ttl;
    l2.mutate(|cache| {
        cache.insert(
            "key".to_owned(),
            Entry {
                bytes: Bytes::from_static(b"saved"),
                lifetime: Some((start, ttl)),
            },
        );
    });
    assert_eq!(l1.get("key").await.unwrap().unwrap(), b"saved"[..]);

    let l1 = l1.as_any().downcast_ref::<MemoryCacheStore>().unwrap();
    let filled = l1.local_entry("key").unwrap();
    assert!(matches!(filled.expiry, CacheExpiry::At(at) if at == expiry));
}
