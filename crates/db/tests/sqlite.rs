use super::*;

fn entry(
    path: &std::path::Path,
    setting: &str,
) -> SqliteEntry {
    serde_yaml::from_str(&format!(
        "path: {}\nbusy_timeout: 1s\ncreate: true\n{setting}\npool:\n  pool_size: 2\n  idle_size: 0\n  acquire_timeout: 1s\n  idle_timeout: 1s\n  max_lifetime: 1s\n",
        path.display()
    ))
    .expect("valid SQLite profile")
}

#[tokio::test]
async fn configcheck_reads_committed_wal_with_reserved_path_characters() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = entry(std::path::Path::new("fixture.sqlite"), "");
    settings.path = directory.path().join("database #?%.sqlite");
    let writer = build_pool("fixture", &settings, false).unwrap();
    let mut connection = writer.acquire().await.unwrap();
    sqlx::query("CREATE TABLE saved (value INTEGER NOT NULL)")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&mut *connection)
        .await
        .unwrap();
    let main_before = std::fs::read(&settings.path).unwrap();
    sqlx::query("INSERT INTO saved VALUES (37)")
        .execute(&mut *connection)
        .await
        .unwrap();
    assert_eq!(main_before, std::fs::read(&settings.path).unwrap());

    let checking = build_pool("fixture", &settings, true).unwrap();
    let value: i64 = sqlx::query_scalar("SELECT value FROM saved")
        .fetch_one(&checking)
        .await
        .unwrap();
    assert_eq!(value, 37, "checking must include the committed WAL");
    checking.close().await;
    assert_eq!(main_before, std::fs::read(&settings.path).unwrap());
    drop(connection);
    writer.close().await;
}

#[tokio::test]
async fn mmap_is_applied_to_each_connection_without_changing_persistence() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database.sqlite");
    let settings = entry(&path, "mmap_size: 1 MiB");
    let pool = build_pool("test", &settings, false).unwrap();
    let mut first = pool.acquire().await.unwrap();
    let mut second = pool.acquire().await.unwrap();
    for connection in [&mut first, &mut second] {
        let bytes: i64 = sqlx::query_scalar("PRAGMA mmap_size")
            .fetch_one(&mut **connection)
            .await
            .unwrap();
        // SQLite can disable mapping at compile time or on unsupported VFSs.
        assert!(bytes == 0 || bytes == 1024 * 1024);
    }
    sqlx::query("CREATE TABLE saved (value INTEGER)")
        .execute(&mut *first)
        .await
        .unwrap();
    sqlx::query("INSERT INTO saved VALUES (42)")
        .execute(&mut *first)
        .await
        .unwrap();
    drop(first);
    drop(second);
    pool.close().await;

    let pool =
        build_pool("test", &entry(&path, "mmap_size: 0"), false).unwrap();
    let bytes: i64 =
        sqlx::query_scalar("PRAGMA mmap_size").fetch_one(&pool).await.unwrap();
    assert_eq!(bytes, 0);
    let value: i64 = sqlx::query_scalar("SELECT value FROM saved")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(value, 42);
    pool.close().await;
}

#[test]
fn mmap_preserves_default_and_rejects_out_of_range_values() {
    let path = std::path::Path::new("unused.sqlite");
    assert_eq!(entry(path, "").mmap_size, None);
    assert_eq!(entry(path, "mmap_size: null").mmap_size, None);
    assert_eq!(entry(path, "mmap_size: 1048576").mmap_size, Some(1048576));
    let mut settings = entry(path, "");
    settings.mmap_size = Some(u64::MAX);
    assert!(build_pool("test", &settings, false).is_err());
}

#[tokio::test]
async fn memory_survives_connection_replacement_and_isolates_pools() {
    let mut settings = entry(std::path::Path::new("unused.sqlite"), "");
    settings.mode = SqliteMode::Memory;
    settings.path.clear();
    settings.create = false;
    let pool = build_pool("test", &settings, false).unwrap();
    let mut first = pool.acquire().await.unwrap();
    sqlx::query("CREATE TABLE saved (value INTEGER)")
        .execute(&mut *first)
        .await
        .unwrap();
    sqlx::query("INSERT INTO saved VALUES (42)")
        .execute(&mut *first)
        .await
        .unwrap();
    let mut second = pool.acquire().await.unwrap();
    let value: i64 = sqlx::query_scalar("SELECT value FROM saved")
        .fetch_one(&mut *second)
        .await
        .unwrap();
    assert_eq!(value, 42);
    first.close().await.unwrap();
    second.close().await.unwrap();
    let value: i64 = sqlx::query_scalar("SELECT value FROM saved")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(value, 42);
    let other = build_pool("test", &settings, true).unwrap();
    assert!(
        sqlx::query("SELECT value FROM saved").execute(&other).await.is_err()
    );
    other.close().await;
    pool.close().await;
}
