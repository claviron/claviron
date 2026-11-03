use super::*;
use crate::tcp::plain::server::{
    HttpServerConfig, ServerSnapshot, build_snapshots,
};
use crate::transport::VhostResolver;
use bytes::Bytes;
use claviron_http::ext::ConnCtx;
use claviron_http::listener::{HttpProtocol, ListenerSnapshots};
use http::{Method, header};
use http_body_util::{BodyExt, Empty};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

fn config() -> HttpServerConfig {
    serde_yaml::from_str(
        "defaults:\n  listener:\n    requests: {max_body_size: off, body_read_timeout: off}\nlisteners:\n  challenge:\n    transport: tcp\n    listen: '127.0.0.1:0'\n    protocols: h1\n    acme: true\n  ordinary:\n    transport: tcp\n    listen: '127.0.0.1:0'\n    protocols: h1\n",
    ).unwrap()
}

fn service(snapshot: Arc<ServerSnapshot>) -> Http01Service {
    let context = Arc::new(ConnCtx {
        state: Default::default(),
        listener: snapshot.listener.id.clone(),
        proto: Some(HttpProtocol::H1),
        remote: "192.0.2.1:1234".parse().unwrap(),
        local_ip: "192.0.2.2".parse().unwrap(),
        local_port: 80,
        real_ip: None,
        country_code: None,
        tls: None,
    });
    Http01Service::new(
        H1Service::new(
            snapshot.clone(),
            Arc::new(VhostResolver::default()),
            context,
        ),
        snapshot.http01.clone().unwrap(),
    )
}

fn snapshot(directory: PathBuf) -> Arc<ServerSnapshot> {
    build_snapshots(&config(), Some(Arc::new(directory)))
        .unwrap()
        .remove(&"challenge".parse().unwrap())
        .unwrap()
}

struct Unread(Arc<AtomicUsize>);

impl http_body::Body for Unread {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Infallible>>> {
        panic!("HTTP-01 must not read a request body")
    }
}

impl Drop for Unread {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn http01_serves_an_unconfigured_host_without_reading_the_body() {
    let directory = tempfile::tempdir().unwrap();
    let token = b"token.raw-key\xff";
    std::fs::write(directory.path().join("abc-123_X"), token).unwrap();
    let service = service(snapshot(directory.path().to_path_buf()));
    let drops = Arc::new(AtomicUsize::new(0));
    let request = Request::builder()
        .uri("/.well-known/acme-challenge/abc-123_X")
        .header(header::HOST, "unconfigured.example.test")
        .body(Unread(drops.clone()))
        .unwrap();
    let response = service.call(request).await.unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "text/plain");
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        token.as_slice()
    );
}

#[tokio::test]
async fn http01_head_and_error_replies_stay_before_host_routing() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("token"), b"key").unwrap();
    let service = service(snapshot(directory.path().to_path_buf()));
    for (method, path, host, status) in [
        (
            Method::HEAD,
            "/.well-known/acme-challenge/token",
            Some("example.test"),
            StatusCode::OK,
        ),
        (
            Method::GET,
            "/.well-known/acme-challenge/token",
            None,
            StatusCode::BAD_REQUEST,
        ),
        (
            Method::GET,
            "/.well-known/acme-challenge/../token",
            Some("example.test"),
            StatusCode::FORBIDDEN,
        ),
        (
            Method::POST,
            "/.well-known/acme-challenge/token",
            Some("example.test"),
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        (
            Method::GET,
            "/.well-known/acme-challenge/missing",
            Some("example.test"),
            StatusCode::NOT_FOUND,
        ),
        (Method::GET, "/ordinary", Some("example.test"), StatusCode::NOT_FOUND),
    ] {
        let mut request = Request::builder().method(method.clone()).uri(path);
        if let Some(host) = host {
            request = request.header(header::HOST, host);
        }
        let response = service
            .call(request.body(Empty::<Bytes>::new()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), status, "{method} {path} host={host:?}");
        if method == Method::HEAD {
            assert_eq!(response.headers()[header::CONTENT_LENGTH], "3");
            assert!(
                response
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .is_empty()
            );
        } else if method == Method::POST {
            assert_eq!(response.headers()[header::ALLOW], "GET, HEAD");
        }
    }
}

#[test]
fn http01_cancellation_releases_the_body_before_file_io() {
    let directory = tempfile::tempdir().unwrap();
    let service = service(snapshot(directory.path().to_path_buf()));
    let drops = Arc::new(AtomicUsize::new(0));
    let future = service.call(
        Request::builder()
            .uri("/.well-known/acme-challenge/token")
            .header(header::HOST, "example.test")
            .body(Unread(drops.clone()))
            .unwrap(),
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(future);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn http01_preparation_requires_opt_in_and_a_published_directory() {
    let config = config();
    let id = "challenge".parse().unwrap();
    let ordinary = "ordinary".parse().unwrap();
    let without = build_snapshots(&config, None).unwrap();
    assert!(without[&id].http01.is_none());
    assert!(without[&ordinary].http01.is_none());
    let with =
        build_snapshots(&config, Some(Arc::new(PathBuf::from("tokens"))))
            .unwrap();
    assert!(with[&id].http01.is_some());
    assert!(with[&ordinary].http01.is_none());
    for transport in [
        "transport: tcp\n    protocols: [h1, h2]",
        "transport: quic\n    protocols: h3",
    ] {
        let yaml = format!(
            "listeners:\n  secure:\n    listen: '127.0.0.1:443'\n    {transport}\n    tls: true\n    requests: {{max_body_size: off, body_read_timeout: off}}\n"
        );
        let baseline: claviron_http::config::HttpSnapshot =
            serde_yaml::from_str(&yaml).unwrap();
        assert!(
            build_snapshots(&baseline, Some(Arc::new(PathBuf::from("tokens"))))
                .unwrap()
                .is_empty()
        );
        let error =
            serde_yaml::from_str::<claviron_http::config::HttpSnapshot>(
                &format!("{yaml}    acme: true\n"),
            )
            .err()
            .unwrap();
        assert!(error.to_string().contains("HTTP-01"), "{error}");
    }
}

#[tokio::test]
async fn http01_directory_republication_keeps_existing_connection_snapshots() {
    let first = tempfile::tempdir().unwrap();
    let next = tempfile::tempdir().unwrap();
    std::fs::write(first.path().join("token"), b"first").unwrap();
    std::fs::write(next.path().join("token"), b"next").unwrap();
    let config = config();
    let id = "challenge".parse().unwrap();
    let current = ListenerSnapshots::default();
    current
        .publish(
            build_snapshots(
                &config,
                Some(Arc::new(first.path().to_path_buf())),
            )
            .unwrap(),
        )
        .unwrap();
    let pinned = service(current.get(&id).unwrap());
    current
        .publish(
            build_snapshots(&config, Some(Arc::new(next.path().to_path_buf())))
                .unwrap(),
        )
        .unwrap();
    let updated = service(current.get(&id).unwrap());
    current.publish(build_snapshots(&config, None).unwrap()).unwrap();
    assert!(current.get(&id).unwrap().http01.is_none());
    for (service, expected) in
        [(pinned, b"first".as_slice()), (updated, b"next".as_slice())]
    {
        let request = Request::builder()
            .uri("/.well-known/acme-challenge/token")
            .header(header::HOST, "example.test")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let response = service.call(request).await.unwrap();
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            expected
        );
    }
}
