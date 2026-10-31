use super::*;

fn entry(setting: &str) -> SqliteEntry {
    serde_yaml::from_str(&format!(
        "path: unused.sqlite\nbusy_timeout: 1s\ncreate: true\n{setting}\npool:\n  pool_size: 2\n  idle_size: 0\n  acquire_timeout: 1s\n  idle_timeout: 1s\n  max_lifetime: 1s\n",
    ))
    .expect("valid SQLite profile")
}

#[test]
fn mmap_preserves_default_and_rejects_out_of_range_values() {
    assert_eq!(entry("").mmap_size, None);
    assert_eq!(entry("mmap_size: null").mmap_size, None);
    assert_eq!(entry("mmap_size: 1048576").mmap_size, Some(1048576));
    let mut settings = entry("");
    settings.mmap_size = Some(u64::MAX);
    let error = build_pool("test", &settings, false).unwrap_err();
    assert!(
        matches!(&error, Error::Invalid(message) if message == "sqlite `test`: mmap_size exceeds SQLite's signed 64-bit byte limit"),
        "unexpected error: {error}"
    );
}
