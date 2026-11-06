use super::*;

use claviron_http::config::HttpSnapshot;
use claviron_http::session::SessionOwner;
use claviron_http::{RouteBody, RouteFuture, bytes_body};
use claviron_runtime::gate::Gate;
use hyper::Request;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

struct Echo;

impl OtherHandler for Echo {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        // Nothing was attached on the way here.
        assert!(request.extensions().is_empty());
        let authority = request
            .headers()
            .get(http::header::HOST)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        Box::pin(async move {
            Ok(http::Response::new(bytes_body(bytes::Bytes::from(authority))))
        })
    }
}

/// The listener serves without any host table: whatever authority a request
/// names, the one handler answers it, over a real accepted connection.
#[tokio::test]
async fn a_single_handler_answers_every_authority_without_a_host_table() {
    let config: HttpSnapshot = serde_yaml::from_str(
        "listeners:\n  example:\n    transport: tcp\n    protocols: h1\n    listen: '127.0.0.1:0'\n    requests: {max_body_size: off, body_read_timeout: off}\n",
    )
    .unwrap();
    let listener = config.listeners.values().next().unwrap().clone();
    let gate = Gate::new(
        listener.limits.max_connections.map(|value| value.get()),
        listener.limits.acquire_timeout,
    );
    let sessions = SessionOwner::new(listener.id.clone());
    let snapshot = Arc::new(Single::new(listener, Echo).unwrap());

    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let serving = tokio::spawn(async move {
        let (stream, peer) = socket.accept().await.unwrap();
        let scope = sessions.bind(gate.try_enter().unwrap());
        serve(stream, peer, snapshot, scope).await;
    });

    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut answers = String::new();
    for authority in ["first.example.test", "second.example.test"] {
        let close = if authority.starts_with("second") {
            "Connection: close\r\n"
        } else {
            ""
        };
        client
            .write_all(
                format!("GET / HTTP/1.1\r\nHost: {authority}\r\n{close}\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
    }
    client.read_to_string(&mut answers).await.unwrap();
    serving.await.unwrap();

    assert_eq!(answers.matches("HTTP/1.1 200 OK").count(), 2, "{answers}");
    let first = answers.find("first.example.test").expect("first answer");
    let second = answers.find("second.example.test").expect("second answer");
    assert!(first < second, "{answers}");
}

/// Cleartext HTTP/2 from a client that speaks it from its first byte, and
/// HTTP/1.1 on the same socket when the listener keeps it.
#[tokio::test]
async fn prior_knowledge_serves_h2_and_keeps_h1_on_the_same_socket() {
    use http_body_util::Empty;
    use hyper_util::rt::{TokioExecutor, TokioIo};

    let config: HttpSnapshot = serde_yaml::from_str(
        "listeners:\n  example:\n    transport: tcp\n    protocols: h1\n    listen: '127.0.0.1:0'\n    requests: {max_body_size: off, body_read_timeout: off}\n",
    )
    .unwrap();
    let listener = config.listeners.values().next().unwrap().clone();
    let gate = Gate::new(
        listener.limits.max_connections.map(|value| value.get()),
        listener.limits.acquire_timeout,
    );
    let sessions = SessionOwner::new(listener.id.clone());
    let snapshot =
        Arc::new(Single::new(listener, Echo).unwrap().prior_knowledge(true));

    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let serving = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, peer) = socket.accept().await.unwrap();
            let scope = sessions.bind(gate.try_enter().unwrap());
            serve(stream, peer, snapshot.clone(), scope).await;
        }
    });

    let stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let (mut h2, connection) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(stream),
    )
    .await
    .unwrap();
    let driving = tokio::spawn(connection);
    let response = h2
        .send_request(
            Request::builder()
                .uri("http://example.test/")
                .header(http::header::HOST, "example.test")
                .body(Empty::<bytes::Bytes>::new())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.version(), http::Version::HTTP_2);
    assert_eq!(response.status(), http::StatusCode::OK);
    drop(response);
    drop(h2);
    driving.await.unwrap().unwrap();

    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client
        .write_all(
            b"GET / HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let mut answer = String::new();
    client.read_to_string(&mut answer).await.unwrap();
    assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
    serving.await.unwrap();
}
