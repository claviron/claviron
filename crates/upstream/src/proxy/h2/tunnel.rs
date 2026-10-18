//! Owns the H2 native tunnel handoff and retained outbound capacity.

use std::sync::Arc;

use claviron_http::forwarding::strip_hop_by_hop_headers;
use claviron_http::session::SessionSpawner;
use claviron_http::{RouteBody, RouteResponse, boxed_body};
use http::{Request, Response, StatusCode};

use crate::proxy::failure::RequestFailure;
use crate::proxy::h2::H2PooledResponseBody;
use crate::proxy::h2::pool::H2TargetPool;
use crate::proxy::service::UpstreamService;
use crate::proxy::service::error::{error_response, session_error_response};
use crate::proxy::service::request::{self, Form, RequestScope};

impl UpstreamService {
    pub(crate) async fn handle_h2_extended_connect_inner(
        &self,
        mut req: Request<RouteBody>,
        pools: &[Arc<H2TargetPool>],
        scope: &mut RequestScope,
    ) -> Result<RouteResponse, RequestFailure> {
        let Some(sessions) = req.extensions().get::<SessionSpawner>().cloned()
        else {
            return Ok(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "upstream CONNECT request is missing its native session owner",
            ));
        };
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

        let on_client = hyper::upgrade::on(&mut req);
        request::prepare(
            origin,
            &self.inner.profile,
            &mut req,
            Form::Absolute,
            scope,
        )
        .map_err(|_| RequestFailure::ProtocolError)?;
        let req = crate::proxy::body::timeout::wrap_h2(
            req,
            scope.requests.body_read_timeout,
            scope.lease.as_mut().expect("admitted request").request.take(),
        );
        let response_result = crate::proxy::body::timeout::within(
            scope.requests.head_timeout,
            sender.send_request(req),
        )
        .await;
        let mut response = match response_result {
            Ok(Ok(response)) => response,
            Ok(Err(err)) => {
                let conn_id = permit.conn_id;
                drop(permit);
                pool.clear_connection(conn_id).await;
                tracing::debug!(error = %err, "upstream h2 extended connect write failed");
                return Err(RequestFailure::RequestWriteFailed);
            }
            Err(_) => {
                let conn_id = permit.conn_id;
                drop(permit);
                pool.clear_connection(conn_id).await;
                return Err(RequestFailure::ResponseHeaderTimeout);
            }
        };

        crate::proxy::service::attempt::observe(self, origin_id, started);
        self.record_outlier_status(
            origin_index,
            response.status(),
            &scope.lease.as_ref().expect("admitted request").epoch,
        );
        if response.status() != StatusCode::OK {
            let (mut parts, body) = response.into_parts();
            strip_hop_by_hop_headers(&mut parts.headers);
            let body = H2PooledResponseBody::new(
                body,
                permit,
                scope.responses.body_idle_timeout,
            );
            return Ok(Response::from_parts(
                parts,
                boxed_body(body.with_lifecycle(
                    scope.lease.take().expect("admitted request"),
                )),
            ));
        }

        let on_upstream = hyper::upgrade::on(&mut response);
        let (mut parts, _body) = response.into_parts();
        parts.headers.remove(http::header::CONTENT_LENGTH);
        let submitted = crate::proxy::service::tunnel::submit(
            &sessions,
            on_client,
            on_upstream,
            scope.lease.take().expect("admitted request"),
            permit,
            self.inner.profile.lifecycle.drain_timeout,
        );
        if let Err(error) = submitted {
            return Ok(session_error_response(error));
        }
        Ok(Response::from_parts(parts, claviron_http::empty_body()))
    }
}
