//! Owns the H1 native tunnel handoff and retained outbound capacity.

use std::sync::{Arc, Mutex};

use claviron_http::forwarding::strip_hop_by_hop_headers;
use claviron_http::session::SessionSpawner;
use claviron_http::{RouteBody, RouteResponse, boxed_body};
use http::{Request, Response, StatusCode};

use crate::proxy::failure::RequestFailure;
use crate::proxy::h1::PooledResponseBody;
use crate::proxy::h1::pool::{
    TargetPool, close_pooled_connection, detach_pooled_upgrade,
};
use crate::proxy::service::UpstreamService;
use crate::proxy::service::error::{error_response, session_error_response};
use crate::proxy::service::request::{self, Form, RequestScope};

impl UpstreamService {
    pub(crate) async fn handle_h1_upgrade_inner(
        &self,
        mut req: Request<RouteBody>,
        pools: &[Arc<Mutex<TargetPool>>],
        scope: &mut RequestScope,
    ) -> Result<RouteResponse, RequestFailure> {
        let Some(sessions) = req.extensions().get::<SessionSpawner>().cloned()
        else {
            return Ok(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "upstream upgrade request is missing its native session owner",
            ));
        };
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
        request::prepare(
            origin,
            &self.inner.profile,
            &mut req,
            Form::OriginUpgrade,
            scope,
        )
        .map_err(|_| RequestFailure::ProtocolError)?;
        let on_client = hyper::upgrade::on(&mut req);
        let req = crate::proxy::body::timeout::wrap(
            req,
            scope.requests.body_read_timeout,
            scope.lease.as_mut().expect("admitted request").request.take(),
        );
        let response_result = crate::proxy::body::timeout::within(
            scope.requests.head_timeout,
            pooled.connection.sender_mut().send_request(req),
        )
        .await;
        let mut response = match response_result {
            Ok(Ok(response)) => response,
            Ok(Err(err)) => {
                close_pooled_connection(pool, pooled);
                tracing::debug!(error = %err, "upstream h1 upgrade request write failed");
                return Err(RequestFailure::RequestWriteFailed);
            }
            Err(_) => {
                close_pooled_connection(pool, pooled);
                return Err(RequestFailure::ResponseHeaderTimeout);
            }
        };

        crate::proxy::service::attempt::observe(self, origin_id, started);
        self.record_outlier_status(
            origin_index,
            response.status(),
            &scope.lease.as_ref().expect("admitted request").epoch,
        );
        if response.status() != StatusCode::SWITCHING_PROTOCOLS {
            let (mut parts, body) = response.into_parts();
            let may_reuse = response_allows_reuse(&parts.headers);
            strip_hop_by_hop_headers(&mut parts.headers);
            let body = PooledResponseBody::new(
                body,
                pool,
                pooled,
                scope.responses.body_idle_timeout,
                may_reuse,
            );
            return Ok(Response::from_parts(
                parts,
                boxed_body(body.with_lifecycle(
                    scope.lease.take().expect("admitted request"),
                )),
            ));
        }

        let on_upstream = hyper::upgrade::on(&mut response);
        let mut connection = detach_pooled_upgrade(pool, pooled);
        if let Err(error) = connection.finish_upgrade().await {
            tracing::error!(%error, "upstream H1 upgrade driver failed to join");
            return Ok(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "upstream H1 upgrade driver failed to join",
            ));
        }
        let connection_budget = connection.breaker.take();
        drop(connection);
        let (parts, _body) = response.into_parts();
        let submitted = crate::proxy::service::tunnel::submit(
            &sessions,
            on_client,
            on_upstream,
            scope.lease.take().expect("admitted request"),
            connection_budget,
            self.inner.profile.lifecycle.drain_timeout,
        );
        if let Err(error) = submitted {
            return Ok(session_error_response(error));
        }
        Ok(Response::from_parts(parts, claviron_http::empty_body()))
    }
}

use super::handler::response_allows_reuse;
