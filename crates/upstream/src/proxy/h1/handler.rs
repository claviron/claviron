//! Owns the H1 acquire, send-once and response-body flow.

use std::sync::{Arc, Mutex};

use claviron_http::forwarding::strip_hop_by_hop_headers;
use claviron_http::{RouteBody, RouteResponse, boxed_body};
use http::header::CONNECTION;
use http::{Request, Response};

use crate::proxy::body::timeout::wrap;
use crate::proxy::failure::RequestFailure;
use crate::proxy::h1::PooledResponseBody;
use crate::proxy::h1::pool::{TargetPool, close_pooled_connection};
use crate::proxy::service::UpstreamService;
use crate::proxy::service::kind::{
    is_h1_upgrade_request, is_h2_extended_connect_request,
};
use crate::proxy::service::request::{self, Form, RequestScope};

impl UpstreamService {
    pub(crate) async fn handle_h1_inner(
        &self,
        mut req: Request<RouteBody>,
        pools: &[Arc<Mutex<TargetPool>>],
        scope: &mut RequestScope,
    ) -> Result<RouteResponse, RequestFailure> {
        if is_h1_upgrade_request(&req) {
            return self.handle_h1_upgrade_inner(req, pools, scope).await;
        }
        if is_h2_extended_connect_request(&req) {
            return Err(RequestFailure::ProtocolError);
        }

        let (
            origin_id,
            (pool, mut pooled),
            started,
            admission,
            origin_admission,
        ) = crate::proxy::service::attempt::run(
            self,
            pools,
            &scope.lease.as_ref().expect("admitted request").epoch,
            crate::proxy::balance::RequestKeyView::new(
                &req,
                scope.forwarded.real_ip,
            ),
            scope.pending.take(),
            scope.lease.as_ref().expect("admitted request").request.is_some(),
            scope.requests.queue_timeout,
        )
        .await?;
        let lease = scope.lease.as_mut().expect("admitted request");
        origin_admission.attach(lease);
        let _native_phase = lease
            .origin
            .as_ref()
            .expect("selected origin")
            .transport
            .native_phase()?;
        if let Some(input) = &mut lease.request {
            input.breaker = admission.clone();
        }
        lease.breaker = admission;
        let origin_index = origin_id.index();
        let origin = &self.inner.profile.origins[origin_index];
        let pool_reuse = pooled.can_reuse;
        request::prepare(
            origin,
            &self.inner.profile,
            &mut req,
            if pool_reuse { Form::OriginKeepAlive } else { Form::OriginClose },
            scope,
        )
        .map_err(|_| RequestFailure::ProtocolError)?;
        let req = wrap(
            req,
            scope.requests.body_read_timeout,
            scope.lease.as_mut().expect("admitted request").request.take(),
        );
        let response_result = crate::proxy::body::timeout::within(
            scope.requests.head_timeout,
            pooled.connection.sender_mut().send_request(req),
        )
        .await;
        let response = match response_result {
            Ok(Ok(response)) => response,
            Ok(Err(err)) => {
                close_pooled_connection(pool, pooled);
                self.record_request_error(origin_index);
                tracing::debug!(error = %err, "upstream h1 request write failed");
                return Err(RequestFailure::RequestWriteFailed);
            }
            Err(_) => {
                close_pooled_connection(pool, pooled);
                self.record_request_error(origin_index);
                return Err(RequestFailure::ResponseHeaderTimeout);
            }
        };

        crate::proxy::service::attempt::observe(self, origin_id, started);
        Ok(self.finish_h1_response(origin_index, pool, pooled, response, scope))
    }
    pub(crate) fn finish_h1_response(
        &self,
        origin_index: usize,
        pool: Arc<Mutex<TargetPool>>,
        pooled: crate::proxy::h1::pool::H1PooledConnection,
        response: Response<hyper::body::Incoming>,
        scope: &mut RequestScope,
    ) -> RouteResponse {
        let (mut parts, body) = response.into_parts();
        self.record_request_status(origin_index, parts.status);
        self.record_outlier_status(
            origin_index,
            parts.status,
            &scope.lease.as_ref().expect("admitted request").epoch,
        );
        let may_reuse =
            pooled.can_reuse && response_allows_reuse(&parts.headers);
        strip_hop_by_hop_headers(&mut parts.headers);
        let body = PooledResponseBody::new(
            body,
            pool,
            pooled,
            scope.responses.body_idle_timeout,
            may_reuse,
        );
        Response::from_parts(
            parts,
            boxed_body(
                body.with_lifecycle(
                    scope.lease.take().expect("admitted request"),
                ),
            ),
        )
    }
}

pub(crate) fn response_allows_reuse(headers: &http::HeaderMap) -> bool {
    !headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|token| token.trim().eq_ignore_ascii_case("close"))
}
