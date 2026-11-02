use super::*;
use bytes::Bytes;
use claviron_http::{RouteFuture, empty_body};
use crate::pipeline::other::{OtherHandler, OtherService};
use crate::service::resolve_sni_from_table;
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

fn service(config: &str, listener: RequestPolicy, owner: OtherService) -> HostService {
    let table = serde_yaml::from_str(&format!("app.test: {{root: public, {config}}}")).unwrap();
    let host = resolve_sni_from_table(&table, Some("app.test")).unwrap();
    HostService::new(RustRouter::Other(owner), Arc::new(ConnCtx {
        state: Default::default(), listener: "public".parse().unwrap(),
        proto: Some(HttpProtocol::H1), remote: "192.0.2.1:1234".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(), local_port: 8080,
        real_ip: None, country_code: None, tls: None,
    }), host, HttpPolicy { requests: listener, responses: claviron_http::policy::ResponsePolicy::unlimited() })
}

#[test]
fn host_request_narrows_the_listener_body_ceiling_without_widening_it() {
    let listener = RequestPolicy { max_body_size: Some(64 * 1024 * 1024), ..RequestPolicy::unlimited() };
    for (config, expected) in [
        ("", listener.max_body_size),
        ("requests: {max_body_size: 10485760}", Some(10 * 1024 * 1024)),
        ("requests: {max_body_size: '10 MiB'}", Some(10 * 1024 * 1024)),
        ("requests: {max_body_size: 134217728}", listener.max_body_size),
    ] {
        let service = service(config, listener, OtherService::Noop);
        let prepared = service.request(Request::new(()), RequestArrival::Established);
        assert_eq!(prepared.max_body_size(), expected);
    }
}

struct OwnedBody { bytes: Bytes, drops: Arc<AtomicUsize> }
impl Drop for OwnedBody {
    fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); }
}

#[test]
fn transport_handoff_and_body_mapping_keep_the_accepted_owners() {
    let service = service("", RequestPolicy::unlimited(), OtherService::Noop);
    let connection = service.policy.connection.clone();
    let config = service.policy.host.vhost_config.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let bytes = Bytes::from(vec![0, 255, 13, 10]);
    let pointer = bytes.as_ptr();
    let request = Request::builder().uri("/original?raw=%2F").header("x-example", "kept")
        .body(OwnedBody { bytes, drops: drops.clone() }).unwrap();
    let prepared = service.request(request, RequestArrival::EarlyData);
    assert!(prepared.request().extensions().is_empty());
    assert!(Arc::ptr_eq(prepared.connection(), &connection));
    assert_eq!(prepared.arrival(), RequestArrival::EarlyData);
    let auth = prepared.auth_context();
    assert!(Arc::ptr_eq(&auth.conn, &connection));
    assert!(Arc::ptr_eq(&auth.host_config, &config));
    assert_eq!(auth.path.as_ref(), "/original");
    drop(service);
    let prepared = prepared.map_body(|body| {
        assert_eq!(body.bytes.as_ptr(), pointer);
        body
    });
    assert_eq!(prepared.request().uri(), "/original?raw=%2F");
    assert_eq!(prepared.request().headers()["x-example"], "kept");
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(prepared.into_request());
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

struct Count(Arc<AtomicUsize>);
impl OtherHandler for Count {
    fn call(&self, request: Request<RouteBody>) -> RouteFuture {
        self.0.fetch_add(1, Ordering::Relaxed);
        Box::pin(std::future::ready(Ok(http::Response::new(request.into_body()))))
    }
}

#[test]
fn native_dispatch_calls_the_owner_only_when_polled() {
    let calls = Arc::new(AtomicUsize::new(0));
    let service = service("", RequestPolicy::unlimited(), OtherService::new(Count(calls.clone())));
    let prepared = service.request(Request::new(empty_body()), RequestArrival::Established);
    let mut answering = pin!(prepared.dispatch());
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    let Poll::Ready(response) = answering.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
        panic!("ready fixture must not need a runtime");
    };
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}
