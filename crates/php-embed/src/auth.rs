//! Request authentication supplied by the embedding application, not by PHP.
//!
//! Bind `dyn PhpRequestAuthenticator` in the runtime registry before PHP boots.
//! The application owns credential extraction, user lookup and session activity
//! policy. The bridge caches only the verified result in the current HTTP request;
//! it never retains a raw credential or exposes a mutable session to PHP.

use async_trait::async_trait;
use claviron_auth::Authenticated;
use claviron_auth::orbit::AuthenticatedSession;
use http::HeaderMap;
use serde_json::{Map, Value};
use std::sync::Arc;

/// Raw model attributes, not an API serializer or accessors. Decimal values and
/// JSON database columns must remain strings; no floating-point money conversion.
pub type ModelAttributes = Map<String, Value>;

/// The verified identity of this PHP HTTP work unit, with its live-session
/// check. Native application extensions may project their own authorization
/// context without accepting a caller-supplied user or session identifier.
/// Do not retain the result for another request or task.
///
/// # Safety
/// Call only on the initialized PHP thread while executing a native PHP
/// function callback; reading SAPI globals requires that thread's PHP context.
pub unsafe fn current() -> Result<Option<Arc<AuthenticatedSession>>, String> {
    crate::functions::auth::current()
}

/// Application-owned field projection, without another lookup. PHP value
/// encoding stays in this crate. Hidden/internal fields are hydration data,
/// never a public response serializer.
pub trait PhpModelAttributes {
    fn model_attributes<'a>(
        &self,
        user: &'a dyn Authenticated,
    ) -> Result<&'a ModelAttributes, String>;
}

/// Called on the host runtime when PHP first asks for its request's identity.
/// Anonymous/invalid credentials return `Ok(None)`; infrastructure failures
/// return an error and become a PHP exception, not a successful anonymous request.
#[async_trait]
pub trait PhpRequestAuthenticator:
    PhpModelAttributes + Send + Sync + 'static
{
    async fn authenticate(
        &self,
        headers: &HeaderMap,
        now: u64,
    ) -> Result<Option<AuthenticatedSession>, String>;
}
