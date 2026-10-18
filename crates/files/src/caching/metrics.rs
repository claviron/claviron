#[cfg(feature = "metrics")]
use claviron_http::metrics::HTTP_METRICS;

#[inline]
pub(crate) fn fast_hit() {
    #[cfg(feature = "metrics")]
    HTTP_METRICS.cache_hit_plus();
}

#[inline]
pub(crate) fn hit() {
    #[cfg(feature = "metrics")]
    HTTP_METRICS.cache_hit();
}

#[inline]
pub(crate) fn miss() {
    #[cfg(feature = "metrics")]
    HTTP_METRICS.cache_miss();
}

#[inline]
pub(crate) fn sync_memory_cache() {
    #[cfg(feature = "metrics")]
    HTTP_METRICS.set_memory_cache(
        super::MEMORY_CACHE.len() as u64,
        super::TOTAL_CACHE_BYTES.load(std::sync::atomic::Ordering::Relaxed),
    );
}
