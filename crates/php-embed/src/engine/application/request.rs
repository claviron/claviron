//! Typed HTTP delivery to already-running PHP inboxes. No lifecycle lock or spawn.
use super::dispatch::Mailbox;
use super::{ApplicationBody, Job, dispatch, request_body};
use crate::SapiGatewayError;
use arc_swap::ArcSwap;
use bytes::Bytes;
use claviron_http::{RouteBody, RouteResponse, bytes_body};
use http::{Method, Request, Response, StatusCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Notify, Semaphore, SemaphorePermit, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

pub(crate) struct ApplicationRequests {
    pub(super) dispatch: ArcSwap<Vec<Mailbox<Job>>>,
    pub(super) accepting: AtomicBool,
    next: AtomicUsize,
    pub(super) wake: Arc<Notify>,
    pub(super) lifecycle: Arc<Notify>,
    pub(super) admission: Semaphore,
    pub(super) stopping: CancellationToken,
}

struct Admitted<'a> {
    permit: Option<SemaphorePermit<'a>>,
    requests: &'a ApplicationRequests,
}
impl Drop for Admitted<'_> {
    fn drop(&mut self) {
        drop(self.permit.take());
        if !self.requests.accepting.load(Ordering::Acquire) {
            self.requests.lifecycle.notify_one();
        }
    }
}
impl ApplicationRequests {
    pub(super) fn new(
        capacity: usize,
        lifecycle: Arc<Notify>,
    ) -> Arc<Self> {
        Arc::new(Self {
            dispatch: ArcSwap::from_pointee(Vec::new()),
            accepting: AtomicBool::new(true),
            next: AtomicUsize::new(0),
            wake: Arc::new(Notify::new()),
            lifecycle,
            admission: Semaphore::new(capacity),
            stopping: CancellationToken::new(),
        })
    }
    pub(super) fn stop(&self) {
        self.accepting.store(false, Ordering::Release);
        self.stopping.cancel();
        self.admission.close();
        self.dispatch.store(Arc::new(Vec::new()));
        self.wake.notify_waiters();
    }
    pub(crate) async fn execute(
        &self,
        req: Request<RouteBody>,
    ) -> Result<RouteResponse, SapiGatewayError> {
        let permit = self.admission.try_acquire().map_err(|_| {
            SapiGatewayError::ServiceUnavailable(
                "application admission is full or closed".into(),
            )
        })?;
        let _admitted = Admitted { permit: Some(permit), requests: self };
        let (mut parts, body) = req.into_parts();
        // What the route decides for this request — the wait for an instance
        // and the reply's ceiling — carried from the adapter; running limits
        // are this application's own.
        let policy = parts
            .extensions
            .get::<claviron_http::policy::HttpPolicy>()
            .copied()
            .ok_or_else(|| {
                SapiGatewayError::Internal(
                    "HttpPolicy is required for PHP application execution"
                        .into(),
                )
            })?;
        if parts.method == Method::CONNECT
            || parts.headers.contains_key(http::header::UPGRADE)
        {
            return Err(SapiGatewayError::NotImplemented);
        }
        claviron_http::forwarding::reject_request_smuggling(
            parts.version,
            &parts.headers,
        )
        .map_err(|e| SapiGatewayError::BadRequest(e.to_string()))?;
        let head = parts.method == Method::HEAD;
        let bytes = match request_body::read(
            &parts,
            body,
            &policy.requests,
            &self.stopping,
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(request_body::Rejection::TooLarge) => {
                let mut response = Response::new(claviron_http::empty_body());
                *response.status_mut() = StatusCode::PAYLOAD_TOO_LARGE;
                return Ok(response);
            }
            Err(request_body::Rejection::Stopped) => {
                return Err(SapiGatewayError::ServiceUnavailable(
                    "application is stopped".into(),
                ));
            }
            Err(request_body::Rejection::BadRequest(error)) => {
                return Err(SapiGatewayError::BadRequest(error));
            }
        };
        claviron_http::forwarding::strip_hop_by_hop_headers(&mut parts.headers);
        let request = Request::from_parts(parts, bytes);
        let (reply, result) = oneshot::channel();
        let mut job = Some(Job {
            request,
            reply,
            max_body_size: policy.responses.max_body_size,
            write_timeout: policy.responses.write_timeout,
        });
        // Unset waits for an instance; stopping ends the wait either way.
        let deadline = policy
            .requests
            .queue_timeout
            .map(|timeout| tokio::time::Instant::now() + timeout);
        loop {
            if !self.accepting.load(Ordering::Acquire) {
                return Err(SapiGatewayError::ServiceUnavailable(
                    "application is stopped".into(),
                ));
            }
            if dispatch::send_available(
                &self.dispatch.load(),
                &self.next,
                &mut job,
            ) {
                break;
            }
            // The ready-inbox path never registers a waiter. After saturation,
            // register before rechecking so a concurrent completion cannot
            // leave this caller asleep beside an available instance.
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if dispatch::send_available(
                &self.dispatch.load(),
                &self.next,
                &mut job,
            ) {
                break;
            }
            let expired = tokio::select! {
                _ = notified => false,
                _ = self.stopping.cancelled() => false,
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await
                    }
                } => true,
            };
            if expired {
                return Err(SapiGatewayError::ServiceUnavailable(
                    "application has no ready idle worker".into(),
                ));
            }
        }
        let response = result.await.map_err(|_| {
            SapiGatewayError::BadGateway(
                "application worker disappeared; request was not replayed"
                    .into(),
            )
        })??;
        let (mut parts, body) = response.into_parts();
        claviron_http::forwarding::strip_hop_by_hop_headers(&mut parts.headers);
        let mut body = match body {
            ApplicationBody::Whole(body) => body,
            ApplicationBody::Stream(receiver) => {
                // Pieces as the script writes them, held instead only as the
                // reply's head and its policy say — the rule JavaScript and
                // the edge follow.
                let stream = if head {
                    claviron_http::empty_body()
                } else {
                    claviron_http::boxed_body(StreamedReply(receiver))
                };
                let body = claviron_http::body::deliver_reply(
                    &mut parts.headers,
                    stream,
                    &policy.responses,
                )
                .await;
                return Ok(Response::from_parts(parts, body));
            }
        };
        parts.headers.remove(http::header::CONTENT_LENGTH);
        if parts.status == StatusCode::NO_CONTENT
            || parts.status == StatusCode::NOT_MODIFIED
            || parts.status == StatusCode::RESET_CONTENT
        {
            body = Bytes::new();
        } else {
            parts
                .headers
                .insert(http::header::CONTENT_LENGTH, body.len().into());
            if head {
                body = Bytes::new();
            }
        }
        Ok(Response::from_parts(parts, bytes_body(body)))
    }
}

struct StreamedReply(mpsc::Receiver<Result<Bytes, String>>);

#[derive(Debug)]
struct ReplyCut(String);

impl std::fmt::Display for ReplyCut {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ReplyCut {}

impl http_body::Body for StreamedReply {
    type Data = Bytes;
    type Error = ReplyCut;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, ReplyCut>>>
    {
        self.0.poll_recv(context).map(|piece| {
            piece.map(|piece| {
                piece.map(http_body::Frame::data).map_err(ReplyCut)
            })
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/application_request.rs"]
mod tests;
