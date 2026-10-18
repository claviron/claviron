//! Explicit new attempts over a replayable request and a profile-wide budget.
use std::sync::{Arc, Mutex};

use claviron_http::RouteResponse;
use http::Request;
use http::header::{HeaderName, HeaderValue};
use tokio::time::Instant;

use super::UpstreamService;
use super::attempt::{self, Leg};
use super::lifecycle::ServiceEpoch;
use super::request::{self, Form, RequestScope};
use crate::config::profile::retry::{RetryCause, RetryConfig};
use crate::proxy::balance::{Balancer, RequestKeyView};
use crate::proxy::body::buffer::Replay;
use crate::proxy::connect::AcquireConnectionError;
use crate::proxy::failure::RequestFailure;

pub(crate) struct Window {
    config: RetryConfig,
    counters: Mutex<Counters>,
}
struct Counters {
    since: Instant,
    requests: u64,
    retries: u64,
}
impl Window {
    pub(crate) fn new(config: &RetryConfig) -> Option<Self> {
        (config.attempts > 0).then(|| Self {
            config: config.clone(),
            counters: Mutex::new(Counters {
                since: Instant::now(),
                requests: 0,
                retries: 0,
            }),
        })
    }
    fn reset(
        &self,
        counters: &mut Counters,
    ) {
        let now = Instant::now();
        if now.duration_since(counters.since) >= self.config.interval {
            *counters = Counters { since: now, requests: 0, retries: 0 };
        }
    }
    pub(crate) fn record_request(&self) {
        let mut counters =
            self.counters.lock().unwrap_or_else(|p| p.into_inner());
        self.reset(&mut counters);
        counters.requests = counters.requests.saturating_add(1);
    }
    fn reserve_retry(&self) -> Option<Budget<'_>> {
        let mut counters =
            self.counters.lock().unwrap_or_else(|p| p.into_inner());
        self.reset(&mut counters);
        let cap = u128::from(counters.requests)
            * u128::from(self.config.budget_percent)
            / 100;
        if u128::from(counters.retries) >= cap {
            return None;
        }
        counters.retries = counters.retries.saturating_add(1);
        Some(Budget { window: self, since: counters.since, committed: false })
    }
}
struct Budget<'a> {
    window: &'a Window,
    since: Instant,
    committed: bool,
}
impl Budget<'_> {
    fn commit(mut self) {
        self.committed = true;
    }
}
impl Drop for Budget<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut counters =
            self.window.counters.lock().unwrap_or_else(|p| p.into_inner());
        if counters.since == self.since {
            counters.retries = counters.retries.saturating_sub(1);
        }
    }
}
struct Head {
    method: http::Method,
    version: http::Version,
    header_count: usize,
    bytes: Vec<u8>,
}
impl Head {
    fn snapshot(req: &Request<()>) -> Self {
        use std::io::Write;
        // URI/HeaderName/HeaderValue prohibit NUL, so NUL-delimited fields
        // preserve arbitrary valid header bytes without a second header bank
        // or a separately cloned URI allocation. This is private replay metadata,
        // never wire/SHM data or an operator size limit.
        let uri_size = req
            .uri()
            .scheme_str()
            .map_or(0, |scheme| scheme.len() + 3)
            + req
                .uri()
                .authority()
                .map_or(0, |authority| authority.as_str().len())
            + req.uri().path_and_query().map_or(1, |path| path.as_str().len());
        let capacity =
            req.headers().iter().fold(uri_size + 1, |total, (name, value)| {
                total
                    .saturating_add(name.as_str().len())
                    .saturating_add(value.as_bytes().len())
                    .saturating_add(2)
            });
        let mut bytes = Vec::with_capacity(capacity);
        write!(&mut bytes, "{}", req.uri()).expect("infallible vector writer");
        bytes.push(0);
        for (name, value) in req.headers() {
            bytes.extend_from_slice(name.as_str().as_bytes());
            bytes.push(0);
            bytes.extend_from_slice(value.as_bytes());
            bytes.push(0);
        }
        Self {
            method: req.method().clone(),
            version: req.version(),
            header_count: req.headers().len(),
            bytes,
        }
    }
    fn request(&self) -> Request<()> {
        let mut fields = self.bytes.split(|byte| *byte == 0);
        let mut req = Request::new(());
        *req.method_mut() = self.method.clone();
        *req.version_mut() = self.version;
        *req.uri_mut() =
            http::Uri::try_from(fields.next().expect("prepared URI"))
                .expect("valid original URI");
        req.headers_mut().reserve(self.header_count);
        for _ in 0..self.header_count {
            let name =
                HeaderName::from_bytes(fields.next().expect("prepared name"))
                    .expect("valid original name");
            let value =
                HeaderValue::from_bytes(fields.next().expect("prepared value"))
                    .expect("valid original value");
            req.headers_mut().append(name, value);
        }
        req
    }
}
pub(crate) struct Failure {
    error: RequestFailure,
    cause: Option<RetryCause>,
}
impl Failure {
    fn acquire(
        service: &UpstreamService,
        index: usize,
        error: AcquireConnectionError,
    ) -> Self {
        match error {
            AcquireConnectionError::OriginReachabilityFailure(error) => {
                service.record_origin_reachability_failure(index);
                Self {
                    error: error.request_failure(),
                    cause: error.retry_cause(),
                }
            }
            AcquireConnectionError::Pool(error) => {
                if error == RequestFailure::BreakerOpen {
                    service.record_breaker_rejection(index);
                }
                Self { error, cause: None }
            }
        }
    }
}
pub(crate) trait SendLeg: Leg {
    fn send(
        &self,
        service: &UpstreamService,
        attempt: (usize, Instant),
        acquired: Self::Acquired,
        req: Request<()>,
        replay: &Replay,
        scope: &mut RequestScope,
    ) -> impl Future<Output = Result<RouteResponse, Failure>> + Send;
}
pub(crate) async fn run<L: SendLeg + ?Sized>(
    service: &UpstreamService,
    leg: &L,
    epoch: &Arc<ServiceEpoch>,
    req: Request<Replay>,
    scope: &mut RequestScope,
) -> Result<RouteResponse, RequestFailure> {
    let mut order = service.inner.balancer.order(
        &RequestKeyView::new(&req, scope.forwarded.real_ip),
        crate::proxy::balance::Health { service, pools: &epoch.pools },
    );
    let (parts, replay) = req.into_parts();
    let mut first = Some(Request::from_parts(parts, ()));
    let mut head = None;
    let mut failed =
        Failure { error: RequestFailure::NoHealthyOrigin, cause: None };
    let mut failed_index = 0;
    let mut retries = 0;
    let mut initial = true;
    loop {
        let next = if initial {
            order.next()
        } else {
            if retries >= service.inner.profile.retry.attempts
                || failed.cause.is_none_or(|cause| {
                    !service.inner.profile.retry.on.contains(&cause)
                })
            {
                return Err(failed.error);
            }
            match order.next() {
                Some(next) => Some(next),
                None => {
                    order.rewind();
                    order.next()
                }
            }
        };
        let Some(id) = next else {
            if initial {
                initial = false;
                continue;
            }
            return Err(failed.error);
        };
        let index = id.index();
        let budget = if initial {
            None
        } else {
            match epoch
                .retry
                .as_ref()
                .expect("enabled retry budget")
                .reserve_retry()
            {
                Some(budget) => Some(budget),
                None => return Err(failed.error),
            }
        };
        let mut admission = match scope
            .pending
            .take()
            .map(|admission| Ok(Some(admission)))
            .unwrap_or_else(|| {
                epoch
                    .breaker
                    .as_ref()
                    .map(|state| state.admit(!initial))
                    .transpose()
            }) {
            Ok(admission) => admission,
            Err(error) => {
                service.record_breaker_rejection(index);
                return Err(error);
            }
        };
        if !initial {
            budget.expect("retry reservation").commit();
            retries += 1;
            let cause = failed.cause.expect("retry eligibility checked");
            service.record_retry(failed_index, cause);
            tracing::debug!(profile = %service.inner.profile.name, origin = %service.inner.profile.origins[failed_index].label,
                next_origin = %service.inner.profile.origins[index].label, attempt = retries, reason = cause.as_str(), "upstream retry");
        }
        let request_body =
            !replay.bytes.is_empty() || replay.trailers.is_some();
        let lease = scope.lease.as_mut().expect("admitted request");
        if request_body && lease.request.is_none() {
            lease.request = Some(epoch.admit_input()?);
        }
        let origin_admission =
            match epoch.transports[index].admit(lease.request.is_some()) {
                Ok(admission) => admission,
                Err(error) => {
                    failed = Failure { error, cause: None };
                    continue;
                }
            };
        let started = Instant::now();
        let acquired = match leg
            .acquire(service, index, epoch, scope.requests.queue_timeout)
            .await
        {
            Ok(acquired) => acquired,
            Err(error) => {
                failed_index = index;
                failed = Failure::acquire(service, index, error);
                continue;
            }
        };
        service.record_origin_success(index);
        if let Some(admission) = &mut admission {
            admission.ready();
        }
        let lease = scope.lease.as_mut().expect("admitted request");
        origin_admission.attach(lease);
        let _native_phase = lease
            .origin
            .as_ref()
            .expect("selected origin")
            .transport
            .native_phase()?;
        let admission = admission.map(Arc::new);
        if let Some(input) = &mut lease.request {
            input.breaker = admission.clone();
        }
        lease.breaker = admission;
        let req = if let Some(req) = first.take() {
            head = Some(Head::snapshot(&req));
            req
        } else {
            head.as_ref().expect("replay head retained").request()
        };
        match leg
            .send(service, (index, started), acquired, req, &replay, scope)
            .await
        {
            Ok(response) => {
                return Ok(response);
            }
            Err(error) => {
                let lease = scope.lease.as_mut().expect("admitted request");
                lease.breaker.take();
                lease.origin.take();
                failed_index = index;
                failed = error;
                initial = false;
            }
        }
    }
}
impl SendLeg for [Arc<Mutex<crate::proxy::h1::pool::TargetPool>>] {
    async fn send(
        &self,
        service: &UpstreamService,
        (index, started): (usize, Instant),
        (pool, mut pooled): Self::Acquired,
        mut req: Request<()>,
        replay: &Replay,
        scope: &mut RequestScope,
    ) -> Result<RouteResponse, Failure> {
        request::prepare(
            &service.inner.profile.origins[index],
            &service.inner.profile,
            &mut req,
            if pooled.can_reuse {
                Form::OriginKeepAlive
            } else {
                Form::OriginClose
            },
            scope,
        )
        .map_err(|_| Failure {
            error: RequestFailure::ProtocolError,
            cause: None,
        })?;
        if let Some(trailers) = &replay.trailers {
            req.headers_mut().remove(http::header::CONTENT_LENGTH);
            req.headers_mut().insert(
                http::header::TRANSFER_ENCODING,
                HeaderValue::from_static("chunked"),
            );
            let mut names = String::with_capacity(
                trailers.keys().map(|name| name.as_str().len() + 2).sum(),
            );
            for name in trailers.keys() {
                if !names.is_empty() {
                    names.push_str(", ");
                }
                names.push_str(name.as_str());
            }
            if !names.is_empty() {
                req.headers_mut().insert(
                    http::header::TRAILER,
                    HeaderValue::from_str(&names).expect("typed trailer names"),
                );
            }
        }
        let req = req.map(|_| {
            replay.body(
                scope.lease.as_mut().expect("admitted request").request.take(),
            )
        });
        let result = crate::proxy::body::timeout::within(
            scope.requests.head_timeout,
            pooled.connection.sender_mut().send_request(req),
        )
        .await;
        match result {
            Ok(Ok(response)) => {
                attempt::observe(
                    service,
                    service.inner.profile.origins[index].id,
                    started,
                );
                Ok(service
                    .finish_h1_response(index, pool, pooled, response, scope))
            }
            error => {
                // Fence the old reader before checking first-byte evidence. An
                // abort without join could race another worker-thread IO poll.
                pooled.connection.close_and_join().await;
                let no_response = pooled.connection.no_response_byte();
                let failure = match error {
                    Err(_) => Failure {
                        error: RequestFailure::ResponseHeaderTimeout,
                        cause: no_response
                            .then_some(RetryCause::ResponseHeaderTimeout),
                    },
                    Ok(Err(error)) => {
                        let retryable = no_response
                            && !error.is_user()
                            && !error.is_parse();
                        tracing::debug!(%error,"upstream replayable h1 request failed");
                        Failure {
                            error: RequestFailure::RequestWriteFailed,
                            cause: retryable
                                .then_some(RetryCause::ResetBeforeResponse),
                        }
                    }
                    Ok(Ok(_)) => unreachable!(),
                };
                crate::proxy::h1::pool::close_pooled_connection(pool, pooled);
                service.record_request_error(index);
                Err(failure)
            }
        }
    }
}
impl SendLeg for [Arc<crate::proxy::h2::pool::H2TargetPool>] {
    async fn send(
        &self,
        service: &UpstreamService,
        (index, started): (usize, Instant),
        (pool, acquired): Self::Acquired,
        mut req: Request<()>,
        replay: &Replay,
        scope: &mut RequestScope,
    ) -> Result<RouteResponse, Failure> {
        request::prepare(
            &service.inner.profile.origins[index],
            &service.inner.profile,
            &mut req,
            Form::Absolute,
            scope,
        )
        .map_err(|_| Failure {
            error: RequestFailure::ProtocolError,
            cause: None,
        })?;
        let mut sender = acquired.sender;
        let permit = acquired.permit;
        let result = crate::proxy::body::timeout::within(
            scope.requests.head_timeout,
            sender.send_request(req.map(|_| {
                replay.body(
                    scope
                        .lease
                        .as_mut()
                        .expect("admitted request")
                        .request
                        .take(),
                )
            })),
        )
        .await;
        match result {
            Ok(Ok(response)) => {
                attempt::observe(
                    service,
                    service.inner.profile.origins[index].id,
                    started,
                );
                Ok(service.finish_h2_response(index, permit, response, scope))
            }
            error => {
                let conn_id = permit.conn_id;
                drop(permit);
                pool.clear_connection(conn_id).await;
                service.record_request_error(index);
                // Multiplexed Hyper exposes no per-stream first-response-byte
                // fence. Connection/control bytes cannot prove this stream safe;
                // conservatively veto post-send replay. Pre-byte retries above
                // still work under the same budget/eligibility/admission policy.
                Err(Failure {
                    error: if error.is_err() {
                        RequestFailure::ResponseHeaderTimeout
                    } else {
                        RequestFailure::RequestWriteFailed
                    },
                    cause: None,
                })
            }
        }
    }
}
