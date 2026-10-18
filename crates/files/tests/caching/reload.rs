use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;

use super::super::{CachedEntry, MEMORY_CACHE, TOTAL_CACHE_BYTES};
use super::invalidate_all;

/// The first version emptied the map with `retain`, and releasing an
/// entry's bytes inside it read the map's length for metrics: a worker
/// holding one cached file hung its own reload. Pinned with a real entry,
/// because an empty map never took the lock twice, and it only bites with
/// the `metrics` feature, which is what reads the length.
#[test]
fn emptying_a_populated_cache_returns() {
    let path = PathBuf::from("/claviron-files-reload-test/app.css");
    let body: Arc<Bytes> = Arc::new(Bytes::from_static(b"body{}"));
    TOTAL_CACHE_BYTES.fetch_add(6, std::sync::atomic::Ordering::Relaxed);
    MEMORY_CACHE.insert(
        path.clone(),
        Arc::new(CachedEntry {
            mtime: SystemTime::UNIX_EPOCH,
            len: 6,
            body,
            content_type: "text/css".to_owned(),
            last_modified: String::new(),
            expires_at: Instant::now() + Duration::from_secs(60),
            etag: "\"t\"".to_owned(),
            encoded_body: None,
            encoding: None,
            encoded_len: 0,
        }),
    );

    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || sender.send(invalidate_all()));
    let (memory, _) = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("invalidate_all must not hang");

    assert!(memory >= 1);
    assert!(MEMORY_CACHE.get(&path).is_none());
}
