use super::*;

use claviron_http::config::HttpSnapshot;
use claviron_http::session::SessionOwner;
use claviron_http::{OtherHandler, RouteBody, RouteFuture, bytes_body};
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
    let snapshot =
        Arc::new(Single::new(listener, crate::owner::Handler(Echo)).unwrap());

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
    let snapshot = Arc::new(
        Single::new(listener, crate::owner::Handler(Echo))
            .unwrap()
            .prior_knowledge(true),
    );

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

struct Named(&'static str);

impl claviron_pipeline::DispatchStep for Named {
    fn call(
        &self,
        _request: Request<RouteBody>,
        _context: Option<claviron_pipeline::HostContext<'_>>,
    ) -> claviron_pipeline::DispatchCall {
        let answer = bytes::Bytes::from_static(self.0.as_bytes());
        claviron_pipeline::DispatchCall::response(Box::pin(async move {
            Ok(http::Response::new(bytes_body(answer)))
        }))
    }
}

impl claviron_pipeline::BackendHandler for Named {
    fn contract(&self) -> claviron_pipeline::BackendContract {
        claviron_pipeline::BackendContract {
            identity: claviron_pipeline::BackendIdentity::None,
            reply: claviron_pipeline::BackendReply::Owner,
        }
    }
}

/// A site runs on a listener with no virtual host anywhere: its directive
/// reaches the backend registered for it and its files are served, whatever
/// authority the request names. A directive nothing owns stops preparation.
#[tokio::test]
async fn a_compiled_site_serves_its_backend_and_files_without_a_host() {
    use claviron_pipeline::compile::compile_site;
    use claviron_pipeline::factory::{BackendPlacement, factory};
    use claviron_pipeline::{BackendHandler, Site, SiteRegistry};
    use claviron_runtime::AppState;

    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("page.txt"), "from a file").unwrap();
    let state = AppState::standalone("site-listener", root.path());
    let config: HttpSnapshot = serde_yaml::from_str(
        "listeners:\n  example:\n    transport: tcp\n    protocols: h1\n    listen: '127.0.0.1:0'\n    requests: {max_body_size: off, body_read_timeout: off}\n",
    )
    .unwrap();
    let listener = config.listeners.values().next().unwrap().clone();
    let site: Arc<Site> = Arc::new(
        serde_yaml::from_str(&format!(
            "root: {}\nroutes:\n  /: {{static: {{}}}}\n  /app:\n    answers: true\n",
            root.path().display()
        ))
        .unwrap(),
    );

    let unowned = compile_site(
        &state,
        site.clone(),
        &listener,
        Some(&SiteRegistry::default()),
    );
    assert!(unowned.is_err());

    let registry = SiteRegistry::default();
    registry
        .register_backend_factory(factory::<bool, _>(
            "answers",
            BackendPlacement::BeforeFiles,
            |_, _| {
                Ok(vec![Arc::new(Named("from a backend"))
                    as Arc<dyn BackendHandler>])
            },
        ))
        .unwrap();
    let execution =
        compile_site(&state, site, &listener, Some(&registry)).unwrap();

    let gate = Gate::new(
        listener.limits.max_connections.map(|value| value.get()),
        listener.limits.acquire_timeout,
    );
    let sessions = SessionOwner::new(listener.id.clone());
    let snapshot = Arc::new(Single::new(listener, execution).unwrap());
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let serving = tokio::spawn(async move {
        let (stream, peer) = socket.accept().await.unwrap();
        let scope = sessions.bind(gate.try_enter().unwrap());
        serve(stream, peer, snapshot, scope).await;
    });

    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client
        .write_all(
            b"GET /app/x HTTP/1.1\r\nHost: first.example.test\r\n\r\n\
              GET /page.txt HTTP/1.1\r\nHost: second.example.test\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let mut answers = String::new();
    client.read_to_string(&mut answers).await.unwrap();
    serving.await.unwrap();

    assert_eq!(answers.matches("HTTP/1.1 200 OK").count(), 2, "{answers}");
    let backend = answers.find("from a backend").expect("backend answer");
    let file = answers.find("from a file").expect("file answer");
    assert!(backend < file, "{answers}");
}
