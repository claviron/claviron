//! Transport-independent calls. Registration owns placement; callers name a
//! logical target and operation, never a Link, lane or fleet.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

pub type Result<T> = std::result::Result<T, Error>;

/// Domain bytes remain opaque. Authorization belongs to the receiving adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub operation: String,
    pub payload: Vec<u8>,
}

/// A remote handler's explicit rejection, not a transport failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Failure {
    Busy,
    Closing,
    Unsupported,
    ResponseTooLarge,
    Rejected(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invoke target is not registered")]
    UnknownTarget,
    #[error("invoke target has more than one endpoint")]
    AmbiguousTarget,
    #[error("invoke target is unavailable")]
    Unavailable,
    #[error("invoke capacity is full")]
    Busy,
    #[error("invoke endpoint is not admitting calls")]
    Closed,
    #[error("invoke result is unknown; the operation may have executed")]
    OutcomeUnknown,
    #[error("invoke payload exceeds its declared bound")]
    PayloadTooLarge,
    #[error("invalid invoke configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("invalid invoke frame: {0}")]
    Protocol(&'static str),
    #[error("invoke transport failed: {0}")]
    Transport(String),
    #[error("invoke rejected by handler: {0:?}")]
    Remote(Failure),
}

/// A host-bound route. Local and IPC adapters implement the same call contract.
/// Dropping a call's future withdraws local interest, not accepted remote work.
#[async_trait]
pub trait Route: Send + Sync {
    async fn call(
        &self,
        request: Request,
    ) -> Result<Vec<u8>>;
}

/// Immutable after boot registration; no discovery or transport on the call API.
#[derive(Default)]
pub struct Client {
    routes: BTreeMap<String, Arc<dyn Route>>,
}

impl Client {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        target: impl Into<String>,
        route: Arc<dyn Route>,
    ) -> Result<()> {
        let target = target.into();
        if target.is_empty() || self.routes.contains_key(&target) {
            return Err(Error::InvalidConfig("empty or duplicate target"));
        }
        self.routes.insert(target, route);
        Ok(())
    }

    pub async fn call(
        &self,
        target: &str,
        operation: impl Into<String>,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>> {
        self.routes
            .get(target)
            .ok_or(Error::UnknownTarget)?
            .call(Request { operation: operation.into(), payload })
            .await
    }
}

#[cfg(test)]
#[path = "../../tests/invoke/call.rs"]
mod tests;
