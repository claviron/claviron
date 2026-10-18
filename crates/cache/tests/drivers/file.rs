use std::time::{SystemTime, UNIX_EPOCH};

use super::*;

fn test_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir()
        .join(format!("claviron-cache-{}-{nonce}", std::process::id()))
}

#[tokio::test]
async fn prefixed_clear_keeps_other_store_keys() {
    let path = test_path();
    let first =
        FileCacheStore::new("first", path.clone(), Some("first:".to_string()));
    let second = FileCacheStore::new(
        "second",
        path.clone(),
        Some("second:".to_string()),
    );
    first.put("key", b"one", None).await.expect("put first");
    second.put("key", b"two", None).await.expect("put second");

    first.clear().await.expect("clear first");

    assert_eq!(first.get("key").await.expect("get first"), None);
    assert_eq!(
        second.get("key").await.expect("get second"),
        Some(bytes::Bytes::from_static(b"two"))
    );
    cacache::clear(path).await.expect("clean test cache");
}
