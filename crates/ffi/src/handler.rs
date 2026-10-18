use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;

use crate::ForeignResponse;

pub type ForeignFuture<'a> =
    Pin<Box<dyn Future<Output = ForeignResponse> + Send + 'a>>;

/// A minimal route-facing foreign call.
///
/// The Rust route decides how to build the input bytes from request context.
/// The foreign implementation only receives those bytes and returns a
/// `ForeignResponse`. Bindings should convert loader, symbol, ABI, or call
/// errors into a response before this boundary; the route-facing shape is
/// intentionally infallible.
pub trait ForeignCall: Send + Sync + 'static {
    fn call(
        &self,
        input: Bytes,
    ) -> ForeignFuture<'_>;
}

impl<F, Fut> ForeignCall for F
where
    F: Fn(Bytes) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ForeignResponse> + Send + 'static,
{
    fn call(
        &self,
        input: Bytes,
    ) -> ForeignFuture<'_> {
        Box::pin(self(input))
    }
}

/// Boxed handler shape for registries or route state.
pub type ForeignHandler = dyn ForeignCall;

pub async fn call_json<H>(
    handler: &H,
    body: Bytes,
) -> ForeignResponse
where
    H: ForeignCall + ?Sized,
{
    handler.call(body).await.ensure_json_content_type()
}
