//! QUIC transport parameters from a listener's declared settings.

use std::sync::Arc;

use claviron_http::config::QuicConfig;
use claviron_runtime::registry::{Error, Result};
use quinn::ServerConfig;

/// A server configuration for `crypto` with this listener's limits.
pub fn server_config(
    crypto: Arc<dyn quinn::crypto::ServerConfig>,
    config: &QuicConfig,
) -> Result<ServerConfig> {
    let mut server = ServerConfig::with_crypto(crypto);
    let transport = Arc::get_mut(&mut server.transport).ok_or_else(|| {
        Error::msg("H3 listener: failed to mutate QUIC transport config")
    })?;
    transport
        .max_concurrent_bidi_streams(config.max_concurrent_bidi_streams.into())
        .max_concurrent_uni_streams(config.max_concurrent_uni_streams.into())
        .max_idle_timeout(Some(config.idle_timeout.try_into().map_err(
            |error| Error::msg(format!("H3 listener: idle_timeout: {error}")),
        )?))
        .keep_alive_interval(config.keep_alive_interval);
    Ok(server)
}
