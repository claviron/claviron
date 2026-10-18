//! Resource release only: no Runnable/profile stop, signal or SHM cleanup.
use async_trait::async_trait;
use claviron_runtime::registry::Finalizable;
use claviron_runtime::{AppState, registry};

use super::UpstreamProvider;
#[async_trait]
impl Finalizable<AppState> for UpstreamProvider {
    async fn finalize(
        &self,
        _: &AppState,
    ) -> registry::Result<()> {
        if let Some(Some(lock)) = self.locks.get() {
            lock.release_held().map_err(|error| {
                registry::Error::msg(format!(
                    "upstream probe lease release failed: {error}"
                ))
            })?;
        }
        Ok(())
    }
}
