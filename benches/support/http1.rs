//! Persistent loopback HTTP/1.1, one connection and one in-flight request.
//!
//! Binding, service preparation, TCP connect and the HTTP handshake are cold.
//! Timing includes client request construction, both protocol drivers, socket IO
//! and response consumption. These are round-trip times, not server CPU/request.
//! Edge cases use the native accepted-stream handler, including request-time
//! host selection and body adaptation.

use std::convert::Infallible;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Instant;

use bytes::Bytes;
use criterion::{Criterion, Throughput};
use http::{Request, Response, StatusCode, header};
use http_body::Body;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::service::Service;
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::plain;

pub struct Server {
    pub address: SocketAddr,
    pub(crate) stop: oneshot::Sender<()>,
    pub(crate) task: JoinHandle<()>,
    pub(crate) selects_host: bool,
}

impl Server {
    pub async fn shutdown(self) {
        let _ = self.stop.send(());
        self.task.await.expect("HTTP benchmark server must finish cleanly");
    }
}

pub async fn hyper<S, B>(service: S) -> Server
where
    S: Service<Request<Incoming>, Response = Response<B>, Error = Infallible>
        + Send
        + 'static,
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
            .half_close(false)
            .pipeline_flush(false)
            .header_read_timeout(None)
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
    Server { address, stop, task, selects_host: false }
}

#[cfg(feature = "axum")]
pub async fn axum() -> Server {
    axum_router(super::axum::plain_router()).await
}

#[cfg(feature = "axum")]
async fn axum_router(router: claviron::axum::Router) -> Server {
    use claviron::axum::serve::ListenerExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        claviron::axum::serve(
            listener.tap_io(|stream| stream.set_nodelay(true).unwrap()),
            router,
        )
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    Server { address, stop, task, selects_host: false }
}

/// Accept once, then run the same connection handler as the native Edge listener.
pub async fn edge(connection: super::edge::Connection) -> Server {
    use claviron::http::session::SessionOwner;
    use claviron::runtime::gate::Gate;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let limits = &connection.listener.limits;
    let gate = Gate::new(
        limits.max_connections.map(|value| value.get()),
        limits.acquire_timeout,
    );
    let sessions = SessionOwner::new(connection.listener.id.clone());
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.unwrap();
        drop(listener);
        let scope = sessions
            .bind(gate.try_enter().expect("admit the benchmark connection"));
        let serving = connection.handler.serve(stream, peer, scope);
        tokio::pin!(serving);
        tokio::select! {
            () = &mut serving => {},
            _ = stopped => {
                gate.graceful_shutdown(None);
                serving.await;
            }
        }
        sessions.close().join().await;
        assert_eq!(
            gate.count(),
            0,
            "native connection/session permits must be released"
        );
    });
    Server { address, stop, task, selects_host: true }
}

pub fn run<F, Started>(
    criterion: &mut Criterion,
    name: &str,
    start: F,
) where
    F: FnOnce() -> Started,
    Started: Future<Output = Server>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let server = runtime.block_on(start());
    let (mut client, driver) = runtime.block_on(async {
        let stream = TcpStream::connect(server.address).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (client, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .unwrap();
        (client, tokio::spawn(connection))
    });
    runtime.block_on(preflight(&mut client, server.selects_host));
    let mut group = criterion.benchmark_group(name);
    group.throughput(Throughput::Elements(1));
    group.bench_function("request", |bencher| {
        bencher.iter_custom(|iterations| {
            runtime.block_on(async {
                let started = Instant::now();
                for _ in 0..iterations {
                    let response =
                        client.send_request(plain::request()).await.unwrap();
                    plain::consume(response).await;
                }
                started.elapsed()
            })
        });
    });
    group.finish();
    runtime.block_on(async {
        drop(client);
        driver.await.unwrap().unwrap();
        server.shutdown().await;
    });
}

/// Send the known, unknown, alias and known hosts on one connection.
///
/// Each request waits for the connection to accept it. A reply can complete
/// before the connection task has asked for the next request, and sending
/// into that gap fails instead of waiting.
pub(crate) async fn preflight(
    client: &mut hyper::client::conn::http1::SendRequest<
        http_body_util::Empty<Bytes>,
    >,
    selects_host: bool,
) {
    client.ready().await.unwrap();
    plain::consume(client.send_request(plain::request()).await.unwrap()).await;
    let mut missing = plain::request();
    missing.headers_mut().insert(header::HOST, "missing.test".parse().unwrap());
    client.ready().await.unwrap();
    let response = client.send_request(missing).await.unwrap();
    if selects_host {
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "host selection must run on this existing connection"
        );
        response.into_body().collect().await.unwrap();
    } else {
        plain::consume(response).await;
    }
    let mut alias = plain::request();
    alias
        .headers_mut()
        .insert(header::HOST, "alias.benchmark.test".parse().unwrap());
    client.ready().await.unwrap();
    plain::consume(client.send_request(alias).await.unwrap()).await;
    client.ready().await.unwrap();
    plain::consume(client.send_request(plain::request()).await.unwrap()).await;
}
