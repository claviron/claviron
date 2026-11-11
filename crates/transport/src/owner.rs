//! Who answers a listener that selects no host.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use claviron_http::ext::{ConnCtx, RequestArrival};
use claviron_http::{OtherHandler, RouteBody, RouteBodyError, RouteResponse};
#[cfg(feature = "pipeline")]
use claviron_pipeline::HostContext;
#[cfg(feature = "pipeline")]
use claviron_pipeline::plan::HostExecution;
use claviron_runtime::ExecutorSpec;
use http::Request;
use http_body_util::BodyExt;

/// The request every door hands over, for an owner that wants it as such.
pub type AppRequest = Request<RequestBody>;

/// A request body as a door hands it over, read as it arrives.
///
/// Its own type rather than [`RouteBody`] itself: with the boxed error's
/// lifetime elided inside a request type, a service future that captures one
/// fails the compiler's `Send` and `Service` checks for every lifetime at
/// once. A name says what it is besides.
pub struct RequestBody(RouteBody);

impl RequestBody {
    /// Box any body. One that is already boxed is taken as it is, with no
    /// second erasure.
    pub fn new<B>(body: B) -> Self
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<RouteBodyError>,
    {
        let mut body = Some(body);
        if let Some(body) =
            (&mut body as &mut dyn Any).downcast_mut::<Option<RouteBody>>()
        {
            return Self(body.take().expect("body is present"));
        }
        if let Some(body) =
            (&mut body as &mut dyn Any).downcast_mut::<Option<RequestBody>>()
        {
            return body.take().expect("body is present");
        }
        Self(body.expect("body is present").map_err(Into::into).boxed_unsync())
    }

    /// A request with no body.
    pub fn empty() -> Self {
        Self::new(http_body_util::Empty::<Bytes>::new())
    }

    /// Recover the existing body without buffering or another erasure wrapper.
    pub fn into_inner(self) -> RouteBody {
        self.0
    }
}

impl http_body::Body for RequestBody {
    type Data = Bytes;
    type Error = RouteBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, RouteBodyError>>> {
        Pin::new(&mut self.0).poll_frame(context)
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.0.size_hint()
    }
}

/// The one owner of every request a `Single` listener accepts: either a
/// [`Handler`] or a site compiled by `claviron_pipeline::compile`.
pub trait Owner: Send + Sync + 'static {
    /// Declared placement of the exchange; a task on the caller's executor
    /// inherits it. This declaration does not schedule work.
    fn execution(&self) -> ExecutorSpec;

    /// Answer one request. `connection` is absent where the connection has no
    /// addresses to describe it. The returned future owns what it needs.
    fn call(
        &self,
        request: Request<RouteBody>,
        connection: Option<&Arc<ConnCtx>>,
        arrival: RequestArrival,
    ) -> impl Future<Output = RouteResponse> + Send + use<Self>;
}

/// A handler that receives the request as the connection produced it, with
/// only its body erased and nothing else attached.
pub struct Handler<H>(pub H);

impl<H: OtherHandler> Owner for Handler<H> {
    fn execution(&self) -> ExecutorSpec {
        ExecutorSpec::Inherit
    }

    fn call(
        &self,
        request: Request<RouteBody>,
        _connection: Option<&Arc<ConnCtx>>,
        _arrival: RequestArrival,
    ) -> impl Future<Output = RouteResponse> + Send + use<H> {
        let answering = self.0.call(request);
        async move {
            match answering.await {
                Ok(response) => response,
                Err(never) => match never {},
            }
        }
    }
}

/// A site compiled by `claviron_pipeline::compile`: its policies, files and
/// backends run on every request, with the connection's facts lent to them.
#[cfg(feature = "pipeline")]
impl Owner for HostExecution {
    fn execution(&self) -> ExecutorSpec {
        ExecutorSpec::Inherit
    }

    fn call(
        &self,
        request: Request<RouteBody>,
        connection: Option<&Arc<ConnCtx>>,
        arrival: RequestArrival,
    ) -> impl Future<Output = RouteResponse> + Send + use<> {
        let execution = self.clone();
        let connection = connection.cloned();
        async move {
            let context = connection
                .as_ref()
                .map(|connection| HostContext::new(connection, arrival));
            match execution.handle_in(request, context).await {
                Ok(response) => response,
                Err(never) => match never {},
            }
        }
    }
}
