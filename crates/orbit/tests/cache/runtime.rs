use super::*;

#[tokio::test]
async fn cache_reads_share_bytes_and_survive_replacement_and_removal() {
    let fleet = Arc::new(Fleet::join("cache_read_owner", 1).expect("fleet"));
    let service = CacheService::from_fleet(fleet).expect("cache service");
    let store = service
        .register_store("values", NonZeroUsize::new(8).unwrap())
        .expect("store");
    store.put("key", b"original", None).await.unwrap();
    let first = store.get("key").await.unwrap().unwrap();
    let second = store.get("key").await.unwrap().unwrap();
    let CacheRead::Hit(entry) = store.read_local(b"key").unwrap() else {
        panic!("expected L1 hit");
    };
    assert_eq!(first.as_ptr(), second.as_ptr());
    assert_eq!(first.as_ptr(), entry.value.as_ptr());
    drop(entry);

    store.put("key", b"replacement", None).await.unwrap();
    assert_eq!(store.get("key").await.unwrap().unwrap(), b"replacement"[..]);
    store.forget("key").await.unwrap();
    assert!(store.get("key").await.unwrap().is_none());
    drop(store);
    drop(service);
    assert_eq!(first, b"original"[..]);
    assert_eq!(second, b"original"[..]);
}

#[tokio::test]
async fn logical_stores_share_transport_but_not_l1_state() {
    let fleet = Arc::new(Fleet::join("cache_runtime", 1).expect("fleet"));
    let service = CacheService::from_fleet(fleet).expect("cache service");
    let capacity = NonZeroUsize::new(8).expect("non-zero capacity");
    let models =
        service.register_store("models", capacity).expect("models store");
    let responses =
        service.register_store("responses", capacity).expect("responses store");

    models.put("42", b"model", None).await.expect("model put");
    responses.put("42", b"response", None).await.expect("response put");

    let CacheRead::Hit(model) = models.read_local(b"42").expect("models read")
    else {
        panic!("models L1 must contain its value");
    };
    let CacheRead::Hit(response) =
        responses.read_local(b"42").expect("responses read")
    else {
        panic!("responses L1 must contain its value");
    };
    assert_eq!(&model.value[..], b"model");
    assert_eq!(&response.value[..], b"response");
}

#[test]
fn duplicate_logical_store_is_rejected() {
    let fleet = Arc::new(Fleet::join("cache_duplicate", 1).expect("fleet"));
    let service = CacheService::from_fleet(fleet).expect("cache service");
    let capacity = NonZeroUsize::new(8).expect("non-zero capacity");

    service.register_store("models", capacity).expect("first store");
    let duplicate = service.register_store("models", capacity);

    assert!(matches!(duplicate, Err(Error::Rejected(_))));
}

#[tokio::test]
async fn logical_store_can_be_reopened_with_its_original_capacity() {
    let fleet = Arc::new(Fleet::join("cache_reopen", 1).expect("fleet"));
    let service = CacheService::from_fleet(fleet).expect("cache service");
    let capacity = NonZeroUsize::new(8).expect("non-zero capacity");
    let original =
        service.register_store("models", capacity).expect("register store");

    original.put("key", b"value", None).await.expect("put");
    let reopened =
        service.open_store("models", capacity).expect("reopen store");
    assert_eq!(
        reopened.get("key").await.expect("get"),
        Some(Bytes::from_static(b"value"))
    );

    let other_capacity = NonZeroUsize::new(16).expect("non-zero capacity");
    assert!(matches!(
        service.open_store("models", other_capacity),
        Err(Error::Rejected(_))
    ));
}
