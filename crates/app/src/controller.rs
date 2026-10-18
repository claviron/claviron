use async_trait::async_trait;

use crate::{ApplicationName, ApplicationRuntime, ApplicationSnapshot};

pub type ApplicationControlResult<T> =
    Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

/// Engine-owned control boundary for one application runtime.
///
/// Implementations expose every application hosted by their runtime in the
/// common snapshot format and read from the authoritative execution owner.
/// The application manager does not maintain parallel lifecycle state.
#[async_trait]
pub trait ApplicationController: Send + Sync {
    fn runtime(&self) -> &ApplicationRuntime;

    fn list(&self) -> Vec<ApplicationSnapshot>;

    fn status(
        &self,
        name: &ApplicationName,
    ) -> Option<ApplicationSnapshot>;

    async fn start(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()>;

    async fn stop(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()>;

    async fn restart(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()>;
}
