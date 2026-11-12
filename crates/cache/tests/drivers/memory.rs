use super::*;

fn config() -> CacheStoreConfig {
    serde_yaml::from_str("driver: memory\ncapacity: 16").unwrap()
}

#[tokio::test]
async fn memory_cache_stores_own_values_and_expiry() {
    let driver = Arc::new(MemoryCacheDriver);
    let first = driver.clone().create_store("first", &config()).unwrap();
    let second = driver.create_store("second", &config()).unwrap();

    first.put("key", b"first", None).await.unwrap();
    second.put("key", b"second", None).await.unwrap();
    let retained = first.get("key").await.unwrap().unwrap();
    first.put("key", b"expired", Some(Duration::ZERO)).await.unwrap();
    assert!(first.get("key").await.unwrap().is_none());

    first.put("key", b"replacement", None).await.unwrap();
    assert_eq!(first.get("key").await.unwrap().unwrap(), b"replacement"[..]);
    first.clear().await.unwrap();
    assert!(first.get("key").await.unwrap().is_none());
    assert_eq!(retained, b"first"[..]);
    assert_eq!(second.get("key").await.unwrap().unwrap(), b"second"[..]);
    second.forget("key").await.unwrap();
    assert!(second.get("key").await.unwrap().is_none());
}
