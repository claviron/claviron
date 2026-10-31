use super::*;

#[tokio::test]
async fn file_clear_metadata_lookup_preserves_absent_success() {
    let root = tempfile::tempdir().expect("create isolated fixture");
    let path = root.path().join("absent-cache");
    for prefix in [None, Some(""), Some("owned:")] {
        let store = FileCacheStore::new(
            "missing",
            path.clone(),
            prefix.map(str::to_owned),
        );
        store.clear().await.expect("clear an absent store");
        assert!(!path.try_exists().expect("inspect absent cache path"));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn file_clear_metadata_lookup_errors_are_not_missing_stores() {
    let root = tempfile::tempdir().expect("create isolated fixture");
    std::os::unix::fs::symlink("cycle", root.path().join("cycle"))
        .expect("create a self-referencing symlink");
    let path = root.path().join("cycle").join("cache");
    // A symlink loop produces a real metadata error even for privileged runners.
    let native_error = path.try_exists().expect_err("metadata lookup fails");
    let mut outcomes = Vec::new();
    for prefix in [None, Some(""), Some("owned:")] {
        let store = FileCacheStore::new(
            "inaccessible",
            path.clone(),
            prefix.map(str::to_owned),
        );
        outcomes.push((prefix, store.clear().await));
    }
    root.close().expect("remove the isolated fixture");

    for (prefix, outcome) in outcomes {
        let error = outcome.expect_err("clear must propagate metadata errors");
        assert!(
            error.to_string().contains(&native_error.to_string()),
            "prefix {prefix:?}: {error}"
        );
    }
}

#[tokio::test]
async fn prefixed_clear_keeps_other_store_keys() {
    let root = tempfile::tempdir().expect("create isolated fixture");
    let path = root.path().to_path_buf();
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
    root.close().expect("clean test cache");
}
