//! Access records: one per request a host serves, written to the log channel
//! `logging.yaml` names in `access_log`.
//!
//! Made in [`HostService::dispatch`](crate::service::HostService), where every
//! request passes exactly once — Other paths included. A guard is created
//! there, learns the status from the reply head, and moves into the reply
//! body; it counts what the body sends and writes the record when the body
//! ends or is dropped. A streamed reply, an event stream ended at shutdown
//! and a client that left mid-body are recorded with their real duration and
//! size. Replies the edge gives itself (503 at admission, 504 at the head)
//! are recorded too: the guard is dropped with them.
//! gRPC status is recorded separately from HTTP status, from the response
//! headers or final trailers. An absent status is never inferred as success.
//!
//! Writing is a copy into this process's lane of the log ring; nothing here
//! waits or does I/O.

use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use claviron_http::ext::{ClientAuthStatus, ConnCtx};
use claviron_http::{RouteBody, RouteBodyError, RouteResponse};
use claviron_logging::pipeline::{
    ChannelId, Level, LogChannels, Record, Value,
};
use claviron_runtime::registry::{self, Bootable};
use claviron_runtime::{AppState, Component};
use http::{HeaderMap, Method, StatusCode, Version};
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt;

/// Where access records go in this process, once the pipeline has booted.
static ACCESS_LOG: OnceLock<AccessLog> = OnceLock::new();

struct AccessLog {
    channels: Arc<LogChannels>,
}

/// A host binding's process-local producer capability. Channel membership and
/// the default selector remain live in LogChannels across configuration reload.
#[derive(Clone)]
pub(crate) struct AccessLogTarget {
    log: &'static AccessLog,
    channel: Option<Arc<str>>,
}

impl AccessLogTarget {
    pub(crate) fn prepare(channel: Option<&str>) -> Option<Self> {
        if channel == Some(OFF) {
            return None;
        }
        Some(Self { log: ACCESS_LOG.get()?, channel: channel.map(Arc::from) })
    }

    pub(crate) fn channel(&self) -> Option<ChannelId> {
        match self.channel.as_deref() {
            Some(name) => self.log.channels.channel(name),
            None => self.log.channels.access(),
        }
    }

    pub(crate) fn begin(
        &self,
        channel: ChannelId,
        host: &str,
        method: &Method,
        path: &str,
        version: Version,
        connection: &Arc<ConnCtx>,
    ) -> AccessGuard {
        AccessGuard {
            log: self.log,
            channel,
            started: Instant::now(),
            admitted: None,
            head: None,
            host: host.to_owned(),
            method: method.clone(),
            path: path.to_owned(),
            version,
            connection: Arc::clone(connection),
            status: StatusCode::INTERNAL_SERVER_ERROR,
            grpc_status: None,
            bytes: 0,
            complete: false,
        }
    }
}

/// What a host's `access_log` turns off: no record is made for it.
pub(crate) const OFF: &str = "off";

/// Resolves the access channels at boot. Registered in every process, used
/// where requests are served; with no `logging.yaml` it leaves access
/// records off.
/// A host names its own channel, `off`, or nothing — and then writes where
/// `logging.yaml`'s `access_log` says, if it says.
#[derive(Default)]
pub(crate) struct AccessLogProvider;

impl Component<AppState> for AccessLogProvider {
    /// Without declared channels access records stay off.
    type Deps = (Option<Arc<LogChannels>>,);

    fn name(&self) -> &'static str {
        "AccessLog"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, (Option<Arc<LogChannels>>,)> for AccessLogProvider {
    async fn boot(
        &self,
        (channels,): (Option<Arc<LogChannels>>,),
        _state: &AppState,
    ) -> registry::Result<()> {
        let Some(channels) = channels else {
            return Ok(());
        };
        let _ = ACCESS_LOG.set(AccessLog { channels });
        Ok(())
    }
}

/// One request's record while it is being answered.
pub(crate) struct AccessGuard {
    log: &'static AccessLog,
    channel: ChannelId,
    started: Instant,
    admitted: Option<Instant>,
    head: Option<Instant>,
    host: String,
    method: Method,
    path: String,
    version: Version,
    connection: Arc<ConnCtx>,
    status: StatusCode,
    grpc_status: Option<u32>,
    bytes: u64,
    complete: bool,
}

impl AccessGuard {
    pub(crate) fn admitted(&mut self) {
        self.admitted = Some(Instant::now());
    }

    /// A reply the edge gives itself, with no body to follow.
    pub(crate) fn replied(
        mut self,
        status: StatusCode,
    ) {
        self.status = status;
        self.head = Some(Instant::now());
        self.complete = true;
    }

    /// The application's reply head; the record is written when `body` ends.
    pub(crate) fn wrap(
        mut self,
        response: RouteResponse,
    ) -> RouteResponse {
        self.record_head(response.status(), response.headers());
        response.map(|body| self.wrap_body(body))
    }

    pub(crate) fn record_head(
        &mut self,
        status: StatusCode,
        headers: &HeaderMap,
    ) {
        self.status = status;
        self.grpc_status = grpc_status(headers);
        self.head = Some(Instant::now());
    }

    pub(crate) fn wrap_body(
        mut self,
        body: RouteBody,
    ) -> RouteBody {
        // An empty body is never polled.
        self.complete = body.is_end_stream();
        AccessBody { inner: body, guard: self }.boxed_unsync()
    }
}

impl Drop for AccessGuard {
    fn drop(&mut self) {
        let micros = |at: Option<Instant>| {
            at.map(|at| at.duration_since(self.started).as_micros() as u64)
        };
        let total = self.started.elapsed().as_micros() as u64;
        let client = self
            .connection
            .real_ip
            .unwrap_or_else(|| self.connection.remote.ip())
            .to_string();
        let mut fields = vec![
            ("host", Value::Str(&self.host)),
            ("method", Value::Str(self.method.as_str())),
            ("path", Value::Str(&self.path)),
            ("status", Value::U64(u64::from(self.status.as_u16()))),
            ("bytes", Value::U64(self.bytes)),
            ("protocol", Value::Str(protocol(self.version))),
            ("client", Value::Str(&client)),
            ("total_us", Value::U64(total)),
            ("complete", Value::Bool(self.complete)),
        ];
        let client_cn = self
            .connection
            .tls
            .as_ref()
            .filter(|tls| matches!(tls.client_auth, ClientAuthStatus::Verified))
            .and_then(|tls| tls.client_cert.as_ref())
            .and_then(|cert| cert.identity.as_ref())
            .and_then(|identity| match identity.common_names.as_slice() {
                [name] => Some(name.as_str()),
                _ => None,
            });
        if let Some(client_cn) = client_cn {
            fields.push(("client_cn", Value::Str(client_cn)));
        }
        if let Some(status) = self.grpc_status {
            fields.push(("grpc_status", Value::U64(u64::from(status))));
        }
        if let Some(queue) = micros(self.admitted) {
            fields.push(("queue_us", Value::U64(queue)));
        }
        if let Some(head) = micros(self.head) {
            fields.push(("head_us", Value::U64(head)));
        }
        let record = Record {
            level: Level::Info,
            target: "",
            message: "",
            fields: &fields,
        };
        if let Err(error) = self.log.channels.emit(self.channel, &record) {
            tracing::debug!(%error, "access record not written");
        }
    }
}

fn grpc_status(headers: &HeaderMap) -> Option<u32> {
    let value = headers.get("grpc-status")?.to_str().ok()?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn protocol(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 => "HTTP/0.9",
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        Version::HTTP_2 => "HTTP/2",
        Version::HTTP_3 => "HTTP/3",
        _ => "HTTP",
    }
}

/// The reply body, carrying the guard to its end.
struct AccessBody {
    inner: RouteBody,
    guard: AccessGuard,
}

impl Body for AccessBody {
    type Data = Bytes;
    type Error = RouteBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RouteBodyError>>> {
        let this = &mut *self;
        let polled = Pin::new(&mut this.inner).poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.guard.bytes += data.len() as u64;
                }
                if let Some(trailers) = frame.trailers_ref()
                    && trailers.contains_key("grpc-status")
                {
                    // The final RPC outcome supersedes a header status.
                    this.guard.grpc_status = grpc_status(trailers);
                }
                // A body that knows its end is not polled again for `None`.
                if this.inner.is_end_stream() {
                    this.guard.complete = true;
                }
            }
            Poll::Ready(None) => this.guard.complete = true,
            _ => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
