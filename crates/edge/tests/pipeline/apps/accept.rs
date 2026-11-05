//! One preparation at acceptance, read by the connection's requests.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use claviron_http::ext::{ConnCtx, ConnStateBuilder, HostKey, HostName};
use claviron_http::policy::HttpPolicy;
use claviron_http::{OtherService, RouteBody, ServiceResult, empty_body};
use claviron_runtime::AppState;
use claviron_types::Host;
use futures_util::FutureExt;
use http::{Request, Response, StatusCode};

use crate::HostConfig;
use crate::pipeline::host::{
    Backend, BackendHandler, DispatchCall, DispatchStep, HostContext,
    HostPipelineBuilder,
};
use crate::pipeline::{Accepted, HostAppRegistry};
use crate::service::RequestArrival;

/// What the layer derives from the accepted facts.
struct PeerPort(u16);

/// Answers with what it finds prepared for the request's connection.
struct Reader;
impl BackendHandler for Reader {
    fn contract(&self) -> crate::pipeline::BackendContract {
        crate::pipeline::BackendContract {
            identity: crate::pipeline::BackendIdentity::None,
            reply: crate::pipeline::BackendReply::Owner,
        }
    }
}
impl DispatchStep for Reader {
    fn call(
        &self,
        request: Request<RouteBody>,
        context: Option<HostContext<'_>>,
    ) -> DispatchCall {
        let prepared =
            context.and_then(|context| context.conn_state().get::<PeerPort>());
        let mut response = Response::new(request.into_body());
        *response.status_mut() = match prepared {
            Some(PeerPort(1234)) => StatusCode::OK,
            Some(_) => StatusCode::CONFLICT,
            None => StatusCode::NOT_FOUND,
        };
        DispatchCall::ready(ServiceResult::Handled(response))
    }
}

struct Application;
impl crate::pipeline::apps::other::OtherHandler for Application {
    fn call(
        &self,
        _request: Request<RouteBody>,
    ) -> claviron_http::RouteFuture {
        Box::pin(std::future::ready(Ok(Response::new(empty_body()))))
    }
}

#[test]
fn a_selected_host_prepares_once_at_acceptance_and_its_requests_read_it() {
    let registry = HostAppRegistry::default();
    let runs = Arc::new(AtomicUsize::new(0));
    let counted = runs.clone();
    registry.register_accept_layer(
        |host, _, _| host.to_string() == "prepared.test",
        move |accepted: &Accepted<'_>, state: &mut ConnStateBuilder| {
            counted.fetch_add(1, Ordering::Relaxed);
            state.insert(PeerPort(accepted.connection.remote.port()));
        },
    );
    for host in ["prepared.test", "plain.test"] {
        registry
            .register_other(host, |_, _, _| Ok(OtherService::new(Application)));
    }
    let table: crate::loader::table::VhostTable = serde_yaml::from_str(
        "prepared.test:\n  root: public\nplain.test:\n  root: public\n",
    )
    .unwrap();
    let state = AppState::standalone("accept-layer", ".");
    let snapshot = registry.build_table(&state, &table, None).unwrap();
    let config = HostConfig::new("public");
    let mut reader = HostPipelineBuilder::<()>::new();
    reader.layer(Backend::new(Reader, HttpPolicy::unlimited())).unwrap();
    let reader = reader.build().into_execution();

    for (name, prepared, status) in [
        ("prepared.test", true, StatusCode::OK),
        ("plain.test", false, StatusCode::NOT_FOUND),
    ] {
        let host: Host = name.parse().unwrap();
        let before = runs.load(Ordering::Relaxed);
        let mut connection = ConnCtx {
            state: Default::default(),
            listener: "public".parse().unwrap(),
            proto: Some(claviron_http::listener::HttpProtocol::H2),
            remote: "192.0.2.1:1234".parse().unwrap(),
            local_ip: "127.0.0.1".parse().unwrap(),
            local_port: 8443,
            real_ip: None,
            country_code: None,
            tls: None,
        };
        connection.state = snapshot.resolve(&host).unwrap().accept(&Accepted {
            connection: &connection,
            hostname: &HostName(Arc::new(host.clone())),
            host: &HostKey(Arc::new(host.clone())),
            config: &config,
        });
        // Once for a connection of the selected host, never for another's,
        // and a connection nothing was prepared for holds nothing.
        let ran = usize::from(prepared);
        assert_eq!(runs.load(Ordering::Relaxed) - before, ran, "{name}");
        assert_eq!(connection.state.is_empty(), !prepared, "{name}");

        let connection = Arc::new(connection);
        let context =
            Some(HostContext::new(&connection, RequestArrival::Established));
        // Requests read what the connection holds; they prepare nothing.
        for _ in 0..2 {
            let response = reader
                .handle_in(Request::new(empty_body()), context)
                .now_or_never()
                .expect("ready fixture")
                .unwrap();
            assert_eq!(response.status(), status, "{name}");
        }
        assert_eq!(runs.load(Ordering::Relaxed) - before, ran, "{name}");
    }
}
