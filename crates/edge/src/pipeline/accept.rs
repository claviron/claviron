//! Work done once while a connection is accepted, for its requests to read.
//!
//! Some of what a request needs is already decided when the handshake ends:
//! who the peer is, which host was selected, what follows from the two. An
//! accept layer does that work once, there, and leaves its result in the
//! connection's [`ConnState`]. Every request of the connection then borrows
//! it through `HostContext::conn_state`, with no lookup, lock or allocation
//! of its own.
//!
//! Layers are registered before the first publication and selected per host
//! and protocol family while the host table is prepared. A host no layer
//! selected carries none: accepting its connections runs nothing and
//! allocates nothing.
//!
//! A layer runs when the connection's host is known, which is after the TLS
//! or QUIC handshake. A cleartext connection has no host until a request
//! names one, and may name another with the next; nothing host-bound can be
//! prepared for it, and no layer runs.

use std::sync::Arc;

use claviron_http::ext::{
    ConnCtx, ConnState, ConnStateBuilder, HostKey, HostName,
};
use claviron_types::Host;

use super::routable::matches_protocol;
use super::{HostAppRegistry, RouteProtocol};
use crate::HostConfig;

/// A connection whose facts are complete: its handshake is over and its host
/// selected, and no request has been read from it.
///
/// More facts of the handshake can be lent here later without changing a
/// layer that does not read them.
#[non_exhaustive]
pub struct Accepted<'a> {
    pub connection: &'a ConnCtx,
    /// The name the peer asked for.
    pub hostname: &'a HostName,
    /// The canonical host that name resolved to.
    pub host: &'a HostKey,
    pub config: &'a HostConfig,
}

/// Prepares connection-scoped values, once, at acceptance.
///
/// It decides nothing about the connection: it cannot refuse it or delay it.
/// It runs on the accepting task before the first request, so it does only
/// what the facts in hand allow without waiting.
pub trait AcceptLayer: Send + Sync + 'static {
    fn accept(
        &self,
        accepted: &Accepted<'_>,
        state: &mut ConnStateBuilder,
    );
}

impl<F> AcceptLayer for F
where
    F: for<'a> Fn(&Accepted<'a>, &mut ConnStateBuilder) + Send + Sync + 'static,
{
    fn accept(
        &self,
        accepted: &Accepted<'_>,
        state: &mut ConnStateBuilder,
    ) {
        self(accepted, state)
    }
}

/// The layers selected for one host on one protocol family.
pub(crate) type AcceptLayers = Arc<[Arc<dyn AcceptLayer>]>;

type Select = dyn Fn(&Host, &HostConfig, RouteProtocol) -> bool + Send + Sync;

pub(in crate::pipeline) struct AcceptRegistration {
    select: Box<Select>,
    layer: Arc<dyn AcceptLayer>,
}

impl HostAppRegistry {
    /// Prepare connection-scoped values for the selected hosts' connections.
    ///
    /// `select` runs at boot and reload, never at acceptance. Register before
    /// the first publication. Layers run in registration order; a value of a
    /// type an earlier layer already supplied replaces it.
    pub fn register_accept_layer(
        &self,
        select: impl Fn(&Host, &HostConfig, RouteProtocol) -> bool
        + Send
        + Sync
        + 'static,
        layer: impl AcceptLayer,
    ) {
        self.accept_registrations
            .write()
            .expect("accept layer registrations lock poisoned")
            .push(Arc::new(AcceptRegistration {
                select: Box::new(select),
                layer: Arc::new(layer),
            }));
    }

    /// The layers this host's connections run on `protocol`, or none.
    pub(in crate::pipeline) fn accept_layers(
        &self,
        host: &Host,
        config: &HostConfig,
        protocol: Option<RouteProtocol>,
    ) -> Option<AcceptLayers> {
        let selected: Vec<_> = self
            .accept_registrations
            .read()
            .expect("accept layer registrations lock poisoned")
            .iter()
            .filter(|registration| {
                matches_protocol(protocol, |protocol| {
                    (registration.select)(host, config, protocol)
                })
            })
            .map(|registration| registration.layer.clone())
            .collect();
        (!selected.is_empty()).then(|| selected.into())
    }
}

impl HostAppRegistry {
    /// Give a newly built host's executions the layers selected for them.
    /// `shared` is the protocol family the shared execution was built for.
    pub(in crate::pipeline) fn with_accept_layers(
        &self,
        host: &Host,
        config: &HostConfig,
        (http, shared): (
            Option<&claviron_http::config::HttpSnapshot>,
            Option<RouteProtocol>,
        ),
        app: super::HostExecution,
        plans: Option<super::apps::ListenerPlans>,
    ) -> (super::HostExecution, Option<super::apps::ListenerPlans>) {
        use super::apps::ListenerHostPlan;
        let app = app.with_accept(self.accept_layers(host, config, shared));
        let plans = plans.map(|plans| {
            plans
                .iter()
                .map(|(id, plan)| {
                    let plan = match plan {
                        ListenerHostPlan::Application(app) => {
                            let protocol = http
                                .and_then(|http| http.listeners.get(id))
                                .map(|listener| {
                                    RouteProtocol::from(listener.as_ref())
                                });
                            ListenerHostPlan::Application(
                                app.clone().with_accept(
                                    self.accept_layers(host, config, protocol),
                                ),
                            )
                        }
                        ListenerHostPlan::NonServing => {
                            ListenerHostPlan::NonServing
                        }
                    };
                    (id.clone(), plan)
                })
                .collect()
        });
        (app, plans)
    }
}

/// Run a host's layers on one of its connections.
pub(in crate::pipeline) fn accept(
    layers: Option<&AcceptLayers>,
    accepted: &Accepted<'_>,
) -> ConnState {
    let Some(layers) = layers else {
        return ConnState::default();
    };
    let mut state = ConnStateBuilder::default();
    for layer in layers.iter() {
        layer.accept(accepted, &mut state);
    }
    state.build()
}
