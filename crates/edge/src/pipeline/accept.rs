//! Which hosts' connections run which accept layers; the layers themselves are
//! `claviron_pipeline::accept`.

use std::sync::Arc;

use claviron_pipeline::accept::{AcceptLayer, AcceptLayers};
use claviron_types::Host;

use super::routable::matches_protocol;
use super::{HostAppRegistry, RouteProtocol};
use crate::Site;

type Select = dyn Fn(&Host, &Site, RouteProtocol) -> bool + Send + Sync;

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
        select: impl Fn(&Host, &Site, RouteProtocol) -> bool + Send + Sync + 'static,
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
        config: &Site,
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
        config: &Site,
        (http, shared): (
            Option<&claviron_http::config::HttpSnapshot>,
            Option<RouteProtocol>,
        ),
        app: claviron_pipeline::plan::HostExecution,
        plans: Option<super::apps::ListenerPlans>,
    ) -> (claviron_pipeline::plan::HostExecution, Option<super::apps::ListenerPlans>) {
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
