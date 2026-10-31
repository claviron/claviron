//! Persistent loopback HTTP/1.1, one connection and one in-flight request.
//!
//! Binding, service preparation, TCP connect and the HTTP handshake are cold.
//! Timing includes client request construction, both protocol drivers, socket IO
//! and response consumption. These are round-trip times, not server CPU/request.
//! The Edge case is Hyper plus a prepared HostService; it deliberately excludes
//! Edge's listener lifecycle and per-request cleartext host selection.

use std::convert::Infallible;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Instant;

use bytes::Bytes;
use criterion::{Criterion, Throughput};
use http::{Request, Response};
use http_body::Body;
use hyper::body::Incoming;
use hyper::service::Service;
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::json;

pub struct Server {
    address: SocketAddr,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl Server {
    async fn shutdown(self) {
        let _ = self.stop.send(());
        self.task.await.expect("HTTP benchmark server must finish cleanly");
    }
}

pub async fn hyper<S, B>(service: S) -> Server
where
    S: Service<Request<Incoming>, Response = Response<B>, Error = Infallible>
        + Send + 'static,
    S::Future: Send,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        drop(listener);
        stream.set_nodelay(true).unwrap();
        let connection = hyper::server::conn::http1::Builder::new()
            .keep_alive(true)
            .serve_connection(TokioIo::new(stream), service);
        tokio::pin!(connection);
        tokio::select! {
            result = &mut connection => result.unwrap(),
            _ = stopped => {
                connection.as_mut().graceful_shutdown();
                connection.await.unwrap();
            }
        }
    });
    Server { address, stop, task }
}

#[cfg(feature = "axum")]
pub async fn axum() -> Server {
    use claviron::axum::serve::ListenerExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        claviron::axum::serve(
            listener.tap_io(|stream| stream.set_nodelay(true).unwrap()),
            super::axum::router(),
        )
        .with_graceful_shutdown(async { let _ = stopped.await; })
        .await
        .unwrap();
    });
    Server { address, stop, task }
}

pub fn run<F, Started>(criterion: &mut Criterion, name: &str, start: F)
where
    F: FnOnce() -> Started,
    Started: Future<Output = Server>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all().build().unwrap();
    let server = runtime.block_on(start());
    let (mut client, driver) = runtime.block_on(async {
        let stream = TcpStream::connect(server.address).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (client, connection) = hyper::client::conn::http1::handshake(
            TokioIo::new(stream),
        ).await.unwrap();
        (client, tokio::spawn(connection))
    });
    runtime.block_on(async {
        json::consume(client.send_request(json::request()).await.unwrap()).await;
    });
    let mut group = criterion.benchmark_group(name);
    group.throughput(Throughput::Elements(1));
    group.bench_function("request", |bencher| {
        bencher.iter_custom(|iterations| runtime.block_on(async {
            let started = Instant::now();
            for _ in 0..iterations {
                let response = client.send_request(json::request()).await.unwrap();
                json::consume(response).await;
            }
            started.elapsed()
        }));
    });
    group.finish();
    runtime.block_on(async {
        drop(client);
        driver.await.unwrap().unwrap();
        server.shutdown().await;
    });
}
