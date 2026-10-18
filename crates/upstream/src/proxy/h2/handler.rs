//! Owns the H2 acquire, send-once and response-body flow.

use std::sync::Arc;

use claviron_http::forwarding::strip_hop_by_hop_headers;
use claviron_http::{RouteBody, RouteResponse, boxed_body};
use http::{Request, Response};

use crate::proxy::body::timeout::wrap_h2 as wrap;
use crate::proxy::failure::RequestFailure;
use crate::proxy::h2::H2PooledResponseBody;
use crate::proxy::h2::pool::H2TargetPool;
use crate::proxy::service::UpstreamService;
use crate::proxy::service::kind::{
    is_h1_upgrade_request, is_h2_extended_connect_request,
};
use crate::proxy::service::request::{self, Form, RequestScope};

impl UpstreamService {
    pub(crate) async fn handle_h2_inner(
        &self,
        mut req: Request<RouteBody>,
        pools: &[Arc<H2TargetPool>],
        scope: &mut RequestScope,
    ) -> Result<RouteResponse, RequestFailure> {
        if is_h1_upgrade_request(&req) {
            return Err(RequestFailure::ProtocolError);
        }
        if is_h2_extended_connect_request(&req) {
            return self
                .handle_h2_extended_connect_inner(req, pools, scope)
                .await;
        }

        let (origin_id, (pool, acquired), started, admission, origin_admission) =
            crate::proxy::service::attempt::run(
                self,
                pools,
                &scope.lease.as_ref().expect("admitted request").epoch,
                crate::proxy::balance::RequestKeyView::new(
                    &req,
                    scope.forwarded.real_ip,
                ),
                scope.pending.take(),
                scope
                    .lease
                    .as_ref()
                    .expect("admitted request")
                    .request
                    .is_some(),
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
        let mut sender = acquired.sender;
        let permit = acquired.permit;

        request::prepare(
            origin,
            &self.inner.profile,
            &mut req,
            Form::Absolute,
            scope,
        )
        .map_err(|_| RequestFailure::ProtocolError)?;
        let mut req = wrap(
            req,
            scope.requests.body_read_timeout,
            scope.lease.as_mut().expect("admitted request").request.take(),
        );
        prepare_content_length(&mut req);
        let response_result = crate::proxy::body::timeout::within(
            scope.requests.head_timeout,
            sender.send_request(req),
        )
        .await;
        let response = match response_result {
            Ok(Ok(response)) => response,
            Ok(Err(err)) => {
                let conn_id = permit.conn_id;
                drop(permit);
                pool.clear_connection(conn_id).await;
                self.record_request_error(origin_index);
                tracing::debug!(error = %err, "upstream h2 request write failed");
                return Err(RequestFailure::RequestWriteFailed);
            }
            Err(_) => {
                let conn_id = permit.conn_id;
                drop(permit);
                pool.clear_connection(conn_id).await;
                self.record_request_error(origin_index);
                return Err(RequestFailure::ResponseHeaderTimeout);
            }
        };

        crate::proxy::service::attempt::observe(self, origin_id, started);
        Ok(self.finish_h2_response(origin_index, permit, response, scope))
    }
    pub(crate) fn finish_h2_response(
        &self,
        origin_index: usize,
        permit: crate::proxy::h2::permit::H2Permit,
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
        strip_hop_by_hop_headers(&mut parts.headers);
        let body = H2PooledResponseBody::new(
            body,
            permit,
            scope.responses.body_idle_timeout,
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

fn prepare_content_length<B: http_body::Body>(req: &mut Request<B>) {
    use http::header::{CONTENT_LENGTH, HeaderValue};
    if req.headers().contains_key(CONTENT_LENGTH) {
        return;
    }
    let Some(mut length) =
        req.body().size_hint().exact().filter(|length| *length != 0)
    else {
        // Leave unknown and zero-length method semantics to Hyper unchanged.
        return;
    };
    // Twenty decimal digits represent every u64 exactly. Format on the stack
    // then copy once, avoiding HeaderValue::from(u64)'s separate Bytes owner.
    let mut digits = [0_u8; 20];
    let mut start = digits.len();
    while length != 0 {
        start -= 1;
        digits[start] = b'0' + (length % 10) as u8;
        length /= 10;
    }
    let value = HeaderValue::from_bytes(&digits[start..])
        .expect("decimal length is a valid header");
    req.headers_mut().insert(CONTENT_LENGTH, value);
}
