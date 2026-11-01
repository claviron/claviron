use std::sync::Arc;

use crate::config::HostConfig;

/// A compiled handshake policy and the declaration used for cold reuse.
#[derive(Clone)]
pub(crate) struct TlsHost {
    pub declaration: Arc<HostConfig>,
    pub config: Arc<rustls::ServerConfig>,
}
