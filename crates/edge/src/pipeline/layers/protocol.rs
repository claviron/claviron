//! Replay policy for explicitly prepared H3 plans.

use super::*;
#[cfg(feature = "http3")]
use crate::service::{RequestArrival, RequestRejection};
#[cfg(feature = "http3")]
use claviron_http::listener::HttpProtocol;

/// Replay policy exists only on H3 bindings. AllowAll contributes no layer.
#[cfg(feature = "http3")]
pub struct EarlyDataLayer(crate::config::EarlyDataPolicy);

#[cfg(feature = "http3")]
impl EarlyDataLayer {
    pub fn new(
        protocol: HttpProtocol,
        policy: crate::config::EarlyDataPolicy,
    ) -> Option<Self> {
        (protocol == HttpProtocol::H3
            && !matches!(policy, crate::config::EarlyDataPolicy::AllowAll))
        .then_some(Self(policy))
    }
}

#[cfg(feature = "http3")]
impl<S: AsRef<RequestArrival>, R: From<RequestRejection>> HostLayer<S, R>
    for EarlyDataLayer
{
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) -> crate::Result<()> {
        match self.0 {
            crate::config::EarlyDataPolicy::AllowAll => {}
            crate::config::EarlyDataPolicy::DenyAll => {
                layer.before(|_, state| {
                    if *state.as_ref() == RequestArrival::EarlyData {
                        ControlFlow::Break(RequestRejection::TooEarly.into())
                    } else {
                        ControlFlow::Continue(())
                    }
                });
            }
            crate::config::EarlyDataPolicy::AllowIdempotent => {
                layer.before(|head, state| {
                    if *state.as_ref() == RequestArrival::EarlyData
                        && !matches!(
                            head.method,
                            http::Method::GET
                                | http::Method::HEAD
                                | http::Method::OPTIONS
                        )
                    {
                        ControlFlow::Break(RequestRejection::TooEarly.into())
                    } else {
                        ControlFlow::Continue(())
                    }
                });
            }
        }
        Ok(())
    }
}
