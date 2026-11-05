//! Access-record producer and response-body accounting for HostPipeline.
//! The prepared layer starts a record, captures the response head and transfers
//! its guard to the body until completion/drop. Cancellation before a response
//! retains an unknown HTTP status. Records own only their required metadata.
//! gRPC status is recorded separately from HTTP status, from the response
//! headers or final trailers. An absent status is never inferred as success.
//!
//! Writing is a copy into this process's lane of the log ring; nothing here
//! waits or does I/O.

use std::net::IpAddr;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use claviron_http::ext::{ClientAuthStatus, ConnCtx, RealIpExt};
use claviron_http::{RouteBody, RouteBodyError};
use claviron_logging::pipeline::{
    ChannelId, Level, LogChannels, Record, Value,
};
use claviron_runtime::registry::{self, Bootable};
use claviron_runtime::{AppState, Component};
use claviron_types::PeerIdentity;
use http::request::Parts;
use http::uri::PathAndQuery;
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
        let target =
            Self { log: ACCESS_LOG.get()?, channel: channel.map(Arc::from) };
        target.channel()?;
        Some(target)
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
        host: Arc<str>,
        head: &Parts,
        connection: &ConnCtx,
    ) -> AccessGuard {
        AccessGuard {
            log: self.log,
            channel,
            started: Instant::now(),
            head: None,
            host,
            method: head.method.clone(),
            path: head.uri.path_and_query().cloned(),
            version: head.version,
            client: head
                .extensions
                .get::<RealIpExt>()
                .map(|ip| ip.0)
                .or(connection.real_ip)
                .unwrap_or_else(|| connection.remote.ip()),
            client_identity: connection
                .tls
                .as_ref()
                .filter(|tls| {
                    matches!(tls.client_auth, ClientAuthStatus::Verified)
                })
                .and_then(|tls| tls.client_cert.as_ref())
                .filter(|identity| identity.common_names.len() == 1)
                .cloned(),
            progress: AccessProgress::default(),
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
    head: Option<Instant>,
    host: Arc<str>,
    method: Method,
    path: Option<PathAndQuery>,
    version: Version,
    client: IpAddr,
    client_identity: Option<Arc<PeerIdentity>>,
    progress: AccessProgress,
}

#[derive(Default)]
struct AccessProgress {
    status: Option<StatusCode>,
    grpc_status: Option<u32>,
    bytes: u64,
    complete: bool,
}

impl AsMut<AccessProgress> for AccessGuard {
    fn as_mut(&mut self) -> &mut AccessProgress {
        &mut self.progress
    }
}

impl AccessGuard {
    pub(crate) fn record_head(
        &mut self,
        status: StatusCode,
        headers: &HeaderMap,
    ) {
        self.progress.record_head(status, headers);
        self.head = Some(Instant::now());
    }

    pub(crate) fn wrap_body(
        self,
        body: RouteBody,
    ) -> RouteBody {
        AccessBody::wrap(body, self)
    }
}

impl AccessProgress {
    fn record_head(
        &mut self,
        status: StatusCode,
        headers: &HeaderMap,
    ) {
        self.status = Some(status);
        self.grpc_status = grpc_status(headers);
    }
}

impl Drop for AccessGuard {
    fn drop(&mut self) {
        let micros = |at: Option<Instant>| {
            at.map(|at| at.duration_since(self.started).as_micros() as u64)
        };
        let total = self.started.elapsed().as_micros() as u64;
        let client = self.client.to_string();
        let mut fields = [("", Value::Bool(false)); 12];
        let mut count = 0;
        let mut push = |name, value| {
            fields[count] = (name, value);
            count += 1;
        };
        push("host", Value::Str(&self.host));
        push("method", Value::Str(self.method.as_str()));
        push(
            "path",
            Value::Str(self.path.as_ref().map_or("", PathAndQuery::path)),
        );
        push("bytes", Value::U64(self.progress.bytes));
        push("protocol", Value::Str(protocol(self.version)));
        push("client", Value::Str(&client));
        push("total_us", Value::U64(total));
        push("complete", Value::Bool(self.progress.complete));
        if let Some(status) = self.progress.status {
            push("status", Value::U64(u64::from(status.as_u16())));
        }
        let client_cn =
            self.client_identity.as_ref().and_then(|identity| match &*identity
                .common_names
            {
                [name] => Some(&**name),
                _ => None,
            });
        if let Some(client_cn) = client_cn {
            push("client_cn", Value::Str(client_cn));
        }
        if let Some(status) = self.progress.grpc_status {
            push("grpc_status", Value::U64(u64::from(status)));
        }
        if let Some(head) = micros(self.head) {
            push("head_us", Value::U64(head));
        }
        let record = Record {
            level: Level::Info,
            target: "",
            message: "",
            fields: &fields[..count],
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
struct AccessBody<G> {
    inner: RouteBody,
    guard: G,
}

impl<G: AsMut<AccessProgress> + Send + Unpin + 'static> AccessBody<G> {
    fn wrap(
        inner: RouteBody,
        mut guard: G,
    ) -> RouteBody {
        if inner.is_end_stream() {
            guard.as_mut().complete = true;
            return inner;
        }
        Self { inner, guard }.boxed_unsync()
    }
}

impl<G: AsMut<AccessProgress> + Unpin> Body for AccessBody<G> {
    type Data = Bytes;
    type Error = RouteBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RouteBodyError>>> {
        let this = &mut *self;
        let polled = Pin::new(&mut this.inner).poll_frame(cx);
        let progress = this.guard.as_mut();
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    progress.bytes += data.len() as u64;
                }
                if let Some(trailers) = frame.trailers_ref()
                    && trailers.contains_key("grpc-status")
                {
                    // The final RPC outcome supersedes a header status.
                    progress.grpc_status = grpc_status(trailers);
                }
                // A body that knows its end is not polled again for `None`.
                if this.inner.is_end_stream() {
                    progress.complete = true;
                }
            }
            Poll::Ready(None) => progress.complete = true,
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

#[cfg(test)]
#[path = "../tests/log.rs"]
mod tests;
