use super::*;

#[test]
fn snapshot_and_formatter_include_http_cache_metrics() {
    let metrics = HttpMetrics::new();
    metrics.cache_hit_plus();
    metrics.cache_hit();
    metrics.cache_miss();
    metrics.set_memory_cache(4, 4096);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.cache_hit_plus_total, 1);
    assert_eq!(snapshot.cache_hits_total, 1);
    assert_eq!(snapshot.cache_misses_total, 1);
    assert_eq!(snapshot.memory_cache_entries, 4);
    assert_eq!(snapshot.memory_cache_bytes, 4096);

    let mut output = String::new();
    snapshot.write_prometheus(&mut output);
    assert!(output.contains("claviron_http_cache_hit_plus_total 1"));
    assert!(output.contains("claviron_http_cache_hits_total 1"));
    assert!(output.contains("claviron_http_cache_misses_total 1"));
    assert!(output.contains("claviron_http_memory_cache_entries 4"));
    assert!(output.contains("claviron_http_memory_cache_bytes 4096"));
}
