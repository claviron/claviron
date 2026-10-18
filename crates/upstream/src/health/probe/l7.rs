//! Dedicated HTTP connection; this owner never applies reachability verdicts.
use std::future::poll_fn;
use std::pin::Pin;

use http::{Request, Response};
use http_body::Body;

use super::ProbeOutcome;
use crate::proxy::body::UpstreamBody;
use crate::proxy::service::UpstreamService;
use crate::proxy::service::lifecycle::ServiceEpoch;
impl UpstreamService {
    pub(crate) async fn probe_l7(
        &self,
        index: usize,
        epoch: &std::sync::Arc<ServiceEpoch>,
    ) -> ProbeOutcome {
        let result = tokio::time::timeout(
            self.inner.profile.health.l7.timeout,
            self.probe_l7_http(index, epoch),
        )
        .await;
        match result {
            Ok(true) => ProbeOutcome::Ok,
            Ok(false) => ProbeOutcome::Fail,
            Err(_) => ProbeOutcome::Timeout,
        }
    }
    async fn probe_l7_http(
        &self,
        index: usize,
        epoch: &std::sync::Arc<ServiceEpoch>,
    ) -> bool {
        let origin = &self.inner.profile.origins[index];
        let prepared = self
            .inner
            .profile
            .l7_probe
            .as_ref()
            .expect("enabled compiled L7 plan");
        match self.inner.profile.protocol {
            crate::config::profile::UpstreamProtocol::H1 => {
                let Ok(mut connection) =
                    self.connect_h1_connection(None, origin, epoch).await
                else {
                    return false;
                };
                let Ok(response) = connection
                    .sender_mut()
                    .send_request(request(prepared, origin))
                    .await
                else {
                    return false;
                };
                inspect(response, &self.inner.profile.health.l7, prepared).await
            }
            crate::config::profile::UpstreamProtocol::H2 => {
                let Ok(connection) =
                    self.connect_h2_connection(None, origin, 0, epoch).await
                else {
                    return false;
                };
                let Ok(response) = connection
                    .sender()
                    .send_request(request(prepared, origin))
                    .await
                else {
                    return false;
                };
                inspect(response, &self.inner.profile.health.l7, prepared).await
            }
        }
    }
}
fn request<T>(
    prepared: &crate::compile::l7::Prepared,
    origin: &crate::origin::Origin,
) -> Request<UpstreamBody<T>> {
    let mut req = Request::new(UpstreamBody::Empty);
    *req.method_mut() = prepared.method.clone();
    *req.uri_mut() = prepared.uris[origin.id.index()].clone();
    req.headers_mut().insert(http::header::HOST, origin.host.clone());
    req
}
async fn inspect(
    response: Response<hyper::body::Incoming>,
    policy: &crate::config::profile::health::l7::L7Config,
    prepared: &crate::compile::l7::Prepared,
) -> bool {
    let status = response.status().as_u16();
    if !(policy.expect.status[0]..=policy.expect.status[1]).contains(&status) {
        return false;
    }
    if prepared.needle.is_empty() {
        return true;
    }
    let mut body = response.into_body();
    let mut matched = 0;
    while let Some(frame) =
        poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await
    {
        let Ok(frame) = frame else {
            return false;
        };
        if let Ok(bytes) = frame.into_data() {
            for byte in bytes {
                while matched > 0 && byte != prepared.needle[matched] {
                    matched = prepared.prefix[matched - 1];
                }
                if byte == prepared.needle[matched] {
                    matched += 1;
                }
                if matched == prepared.needle.len() {
                    return true;
                }
            }
        }
    }
    false
}
