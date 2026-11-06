use claviron_http::config::HttpSnapshot;
use claviron_http::{RouteBody, RouteFuture, bytes_body};
use http::{Request, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use super::*;

struct Echo;

impl OtherHandler for Echo {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        let answer = format!("{:?}", request.version());
        Box::pin(std::future::ready(Ok(Response::new(bytes_body(
            answer.into(),
        )))))
    }
}

fn single() -> Arc<plain::Single<Echo>> {
    let config: HttpSnapshot = serde_yaml::from_str(
        "listeners:\n  door:\n    transport: tcp\n    protocols: h1\n    listen: '127.0.0.1:0'\n    requests: {max_body_size: off, body_read_timeout: off}\n",
    )
    .unwrap();
    let listener = config.listeners.values().next().unwrap().clone();
    Arc::new(plain::Single::new(listener, Echo).unwrap())
}

/// A socket answers over its file, a leftover file does not stop it from
/// binding, a live one does, and draining removes the file it created.
#[tokio::test]
async fn a_unix_socket_serves_and_owns_its_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("door.sock");
    // What a process that is gone leaves behind.
    drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
    assert!(path.exists());

    let cancellation = CancellationToken::new();
    let context = RunContext::new(cancellation.clone());
    let serving = {
        let path = path.clone();
        tokio::spawn(async move { serve(&path, single(), &context).await })
    };
    let mut stream = loop {
        match UnixStream::connect(&path).await {
            Ok(stream) => break stream,
            Err(_) => tokio::task::yield_now().await,
        }
    };

    let other =
        serve(&path, single(), &RunContext::new(CancellationToken::new()))
            .await;
    assert!(other.is_err(), "a live socket must not be taken over");

    stream
        .write_all(
            b"GET / HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).await.unwrap();
    assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
    assert!(answer.ends_with("HTTP/1.1"), "{answer}");

    cancellation.cancel();
    serving.await.unwrap().unwrap();
    assert!(!path.exists(), "the listener removes its own file");
}
