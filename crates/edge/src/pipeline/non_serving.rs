//! One fixed plain-HTTP flow: optional interception, then the host policy.

use std::future::Future;
use std::pin::Pin;

use super::{HostHandlerResult, HostRequest};
use crate::HostConfig;
use claviron_http::ext::{ConnCtx, HostKey};
use claviron_http::policy::HttpPolicy;

/// Borrowed input to the optional non-serving interceptor.
#[derive(Clone, Copy)]
pub struct NonServingContext<'a> {
    pub config: &'a HostConfig,
    pub key: &'a HostKey,
    pub connection: &'a ConnCtx,
    pub policy: &'a HttpPolicy,
}

/// Optional single interception point before plain HTTP redirect/reject.
/// Registered once, independently of Routable and the normal host pipeline.
/// Handled is terminal; Unhandled retains the same unread request body.
/// TODO: allow independent integrations to compose behind this single binding.
pub trait InterceptsNonServing: Send + Sync + 'static {
    fn intercept<'a>(
        &'a self,
        request: HostRequest,
        context: NonServingContext<'a>,
    ) -> Pin<Box<dyn Future<Output = HostHandlerResult> + Send + 'a>>;
}

#[cfg(feature = "tcp")]
mod plain {
    use std::fmt::Write;
    use std::net::IpAddr;
    use std::num::NonZeroU16;
    use std::sync::Arc;

    use super::*;
    use crate::config::NonSslPolicy;
    use crate::service::{HostResponse, RequestRejection, VhostCtx};
    use claviron_http::ext::ConnCtx;
    use claviron_http::{RouteResponse, ServiceResult};
    use claviron_types::Host;
    use http::{HeaderValue, Request, StatusCode};

    /// Listener-owned concrete flow. Host data is an input, never a built chain.
    pub(crate) struct NonServingPipeline {
        redirect_port: NonZeroU16,
        interception: Option<Arc<dyn InterceptsNonServing>>,
    }

    impl NonServingPipeline {
        pub(crate) fn new(
            redirect_port: NonZeroU16,
            interception: Option<Arc<dyn InterceptsNonServing>>,
        ) -> Self {
            Self { redirect_port, interception }
        }

        pub(crate) async fn call<B>(
            &self,
            request: Request<B>,
            host: VhostCtx,
            connection: Arc<ConnCtx>,
            listener: claviron_http::policy::HttpPolicy,
        ) -> RouteResponse
        where
            B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
            B::Error: Into<claviron_http::RouteBodyError>,
        {
            if let Some(interception) = &self.interception {
                let policy = listener.narrow(&host.vhost_config.policy());
                let context = NonServingContext {
                    config: &host.vhost_config,
                    key: &host.vhost_key,
                    connection: &connection,
                    policy: &policy,
                };
                let request = request.map(claviron_http::boxed_body);
                return match interception
                    .intercept(HostRequest::new(request), context)
                    .await
                {
                    ServiceResult::Handled(reply) => reply,
                    ServiceResult::Unhandled(request) => {
                        self.reply(&request.head().uri, &host)
                    }
                };
            }
            self.reply(request.uri(), &host)
        }

        fn reply(
            &self,
            uri: &http::Uri,
            host: &VhostCtx,
        ) -> RouteResponse {
            match host.vhost_config.transport.non_ssl {
                NonSslPolicy::Reject => RequestRejection::AccessDenied.into(),
                NonSslPolicy::Serve => {
                    claviron_http::err_status(StatusCode::NOT_FOUND)
                }
                NonSslPolicy::Redirect => self.redirect(uri, host),
            }
        }

        fn redirect(
            &self,
            uri: &http::Uri,
            host: &VhostCtx,
        ) -> RouteResponse {
            let mut location = String::from("https://");
            match host.hostname.0.as_ref() {
                Host::Dns(name) => location.push_str(name.as_str()),
                Host::Single(name) => location.push_str(name),
                Host::Ip(IpAddr::V6(ip)) => {
                    write!(location, "[{ip}]").expect("writing to String");
                }
                Host::Ip(IpAddr::V4(ip)) => {
                    write!(location, "{ip}").expect("writing to String");
                }
            }
            if self.redirect_port.get() != 443 {
                write!(location, ":{}", self.redirect_port)
                    .expect("writing to String");
            }
            location.push_str(
                uri.path_and_query().map_or("/", |path| path.as_str()),
            );
            match HeaderValue::from_maybe_shared(bytes::Bytes::from(location)) {
                Ok(location) => HostResponse::Redirect {
                    status: StatusCode::PERMANENT_REDIRECT,
                    location,
                }
                .into(),
                Err(_) => RequestRejection::InvalidRedirect.into(),
            }
        }
    }
}

#[cfg(feature = "tcp")]
pub(crate) use plain::NonServingPipeline;
