use std::convert::Infallible;
use std::error::Error as StdError;
use std::net::IpAddr;
use std::pin::Pin;

use bytes::Bytes;
use http::{HeaderMap, Response, StatusCode, Uri};
use http_body::Body;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};

/// Erased error type for bodies flowing through the routing pipeline.
///
/// **What:** `Box<dyn StdError + Send + Sync>` — a heap-allocated error
/// trait object.
///
/// **Why:** Different transports surface different concrete error types
/// (`hyper::Error` from h1/h2, `Infallible` from h3 buffered, custom
/// stream errors from h3 streaming). Boxing erases the concrete type so
/// the pipeline can stay non-generic.
///
/// **Use when:** writing or implementing anything that constructs a
/// [`RouteBody`] — the body's `Error` associated type must match this
/// alias.
///
/// **Don't use when:** you're inside a single transport boundary and
/// still know the concrete error type. Erase only when the body crosses
/// into the type-erased pipeline (`RouteBody`).
pub type RouteBodyError = Box<dyn StdError + Send + Sync>;

/// The single concrete body type used across the routing pipeline.
///
/// **What:** `UnsyncBoxBody<Bytes, RouteBodyError>` — a type-erased
/// `http_body::Body` from `http_body_util`. "Unsync" = `Send` only,
/// no `Sync` requirement (per-task ownership is fine).
///
/// **Why:** unifies request and response bodies for the Other dispatch
/// chain (PHP FCGI / PHP SAPI / Pusher / CDN file probe). Each transport
/// (h1/h2 `Incoming`, h3 buffered `Full<Bytes>`, h3 streaming
/// `StreamBody<…>`) boxes its native body into this single type via
/// [`boxed_body`], so adapters and handlers downstream are non-generic.
/// Hyper-stack only — **deliberately axum-free** so Other paths don't
/// pull axum into their dependency graph.
///
/// **Use when:** declaring a body slot anywhere in Other adapters,
/// `OtherService`, the `Handler` trait, or response builders that feed
/// the dispatch chain.
///
/// **Don't use when:** you're inside the axum/Tower router pipeline —
/// that path uses `axum::body::Body` and converts at the boundary
/// (see `bindings::into_axum_response`). Don't introduce `RouteBody`
/// inside Tower middleware; it would force unnecessary box/unbox cycles.
pub type RouteBody = UnsyncBoxBody<Bytes, RouteBodyError>;

/// HTTP response with an erased body — the canonical response type
/// returned by every Other adapter.
///
/// **What:** `Response<RouteBody>`.
///
/// **Why:** caller can hand the response to any sink (h1/h2 hyper, h3
/// `send_response_body`) without caring about the original body type.
/// Errors are uniformly carried as `RouteBodyError`.
///
/// **Use when:** writing a function that returns from the native
/// pipeline (any `*Adapter::handle`, [`err_status`], any helper that
/// constructs an end-of-pipeline response).
///
/// **Don't use when:** you're producing a `Response<axum::Body>` for a
/// Tower service — convert at the framework boundary instead.
pub type RouteResponse = Response<RouteBody>;

/// Pinned, boxed future returning a [`RouteResponse`].
///
/// **What:** `Pin<Box<dyn Future<Output = Result<RouteResponse, Infallible>> + Send>>`.
///
/// **Why:** lets Other `tower::Service` impls declare a single
/// `Service::Future` associated type without each adapter exposing its
/// own concrete async future shape. `Infallible` because adapters
/// already convert all internal errors into a `RouteResponse` (5xx
/// fallback) — there is no error path back to the caller.
///
/// **Use when:** declaring `type Future = RouteFuture` on an Other
/// `Service` impl.
///
/// **Don't use when:** you're not on the Other dispatch path. Tower
/// middleware uses its own future types.
pub type RouteFuture = Pin<
    Box<dyn Future<Output = Result<Response<RouteBody>, Infallible>> + Send>,
>;

/// Captured request/response payload pair — used by recording / audit /
/// playback layers (telemetry, request mirror, replay tests) that need
/// the actual bytes plus the status.
///
/// **What:** owned `Bytes` for both directions plus the response status.
///
/// **Why:** when a layer needs to *observe* the exchange (not just route
/// it), this is the minimal struct that captures what passed through.
/// Bytes are pre-collected; do not pass streaming bodies here.
///
/// **Use when:** building telemetry / mirroring / replay middleware
/// that needs both directions of a completed exchange.
///
/// **Don't use when:** you only need one side, or you're in a hot path
/// where buffering both bodies would defeat backpressure. Prefer
/// streaming bodies and emit observation events at frame boundaries.
#[derive(Clone, Debug)]
pub struct HttpExchange {
    pub request_body: Bytes,
    pub response_body: Bytes,
    pub response_status: StatusCode,
}

/// Outcome of a probe-style service call: either the request was fully
/// handled, or it wasn't and the caller should fall through.
///
/// **What:** two-arm enum carrying `T` on the success path and an
/// optional `U` (default `()`) on the fallthrough path.
///
/// **Why:** the file-first probe (`claviron-files::try_handle_files`)
/// and similar stages need a way to say "I claimed this request, here's
/// the response" vs "not mine, keep going". Plain `Option<T>` would
/// lose the ability to ferry context on the unhandled side
/// (e.g. partial state) when needed.
///
/// **Use when:** writing a probe / chain-of-responsibility stage that
/// can either terminate the pipeline or yield to the next stage.
///
/// **Don't use when:** the stage always produces a response (use
/// `RouteResponse` directly) or always errors out (use `Result`).
#[derive(Clone, Debug)]
pub enum ServiceResult<T, U = ()> {
    Handled(T),
    Unhandled(U),
}

/// Borrowed view of a request's routable surface — uri, headers,
/// resolved real IP — without taking ownership of the body.
///
/// **What:** zero-copy struct of references plus an owned `Option<IpAddr>`.
///
/// **Why:** GET-style adapters and probes (file probe, CDN handler,
/// redirect/ACL checks) only need to *look at* the request. Passing a
/// full `Request<B>` would either force a generic on every helper or
/// require boxing the body. `RequestContext` sidesteps both.
///
/// **Use when:** writing a function that inspects routing-relevant
/// fields but never reads the body (file probe, redirect resolver,
/// ACL evaluator, GET-style asset handler).
///
/// **Don't use when:** the handler needs to read the body (POST/PUT
/// JSON parsing, multipart upload). Take `Request<RouteBody>` instead.
pub struct RequestContext<'a> {
    pub uri: &'a Uri,
    pub headers: &'a HeaderMap,
    pub real_ip: Option<IpAddr>,
}

/// Erase any concrete `http_body::Body` into the canonical [`RouteBody`].
///
/// **What:** boxes `body` as `UnsyncBoxBody<Bytes, RouteBodyError>` after
/// converting its error type via `Box::new`.
///
/// **Why:** transports produce different concrete bodies (`Incoming`,
/// `Full<Bytes>`, `StreamBody<…>`); the Other dispatch chain only
/// understands one. This helper is the single boundary where erasure
/// happens.
///
/// **Use when:** crossing into the native pipeline — h1/h2
/// `Service<Request<Incoming>>` impl, h3 dispatch `serve_h3_upstream`,
/// any place that constructs a `RouteBody` from a transport-native body.
///
/// **Don't use when:** the body is already `RouteBody`. Re-boxing is
/// wasted work (extra Box allocation + indirection).
#[inline]
pub fn boxed_body<B>(body: B) -> RouteBody
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: StdError + Send + Sync + 'static,
{
    body.map_err(|err| -> RouteBodyError { Box::new(err) }).boxed_unsync()
}

/// Zero-byte [`RouteBody`].
///
/// **What:** `boxed_body(Full::new(Bytes::new()))` — an empty body whose
/// `Body::is_end_stream()` returns true immediately.
///
/// **Use when:** building responses with no payload (status-only
/// replies: 204, 301, 304, error stubs, OPTIONS preflights).
///
/// **Don't use when:** you have payload bytes — use [`bytes_body`].
#[inline]
pub fn empty_body() -> RouteBody {
    boxed_body(Full::new(Bytes::new()))
}

/// Build a [`RouteBody`] from owned bytes.
///
/// **What:** wraps `bytes` in `http_body_util::Full` and erases the type.
///
/// **Use when:** the entire payload is already in memory (rendered HTML,
/// JSON serialisation result, a small image fetched from disk).
///
/// **Don't use when:** the payload is large or streaming — buffering
/// defeats backpressure. Stream via `StreamBody` and `boxed_body` instead.
#[inline]
pub fn bytes_body(bytes: Bytes) -> RouteBody {
    boxed_body(Full::new(bytes))
}

/// Build an empty [`RouteResponse`] with the given status code.
///
/// **What:** `Response<RouteBody>` with no body, no headers.
///
/// **Why:** common shortcut for handlers (CDN / qr / image / health
/// probe / 5xx fallbacks) that don't need a body — content-length
/// stays 0, no Content-Type set.
///
/// **Use when:** rejecting a request (404, 421, 503), terminating early
/// with a status, or replying to a status-only verb (HEAD/OPTIONS).
/// Caller adds headers via `.headers_mut()` on the returned response if
/// needed.
///
/// **Don't use when:** you have payload bytes — build the response
/// directly with [`bytes_body`].
///
/// `unwrap()` is safe: the only failure path on `Response::builder` is
/// malformed status (impossible since `StatusCode` validates at
/// construction), so this never panics in practice.
#[inline]
pub fn err_status(status: StatusCode) -> RouteResponse {
    Response::builder().status(status).body(empty_body()).unwrap()
}

// axum-specific bindings (into_axum_response / from_axum_response)
// live in `crate::bindings` so this module stays framework-agnostic.
