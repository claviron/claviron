//! Isolated upstream warm-maintenance lifecycle checks through the facade.
use claviron::runtime::{
    AppState, RunContext,
    congif::ConfigRegistration,
    gate::GateDrainOutcome,
    registry::{Bootable, Runnable},
};
use claviron::upstream::{UpstreamProvider, proxy::service::UpstreamLifecycle};
use futures_util::poll;
use http_body_util::BodyExt;
use std::{error::Error, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;

struct FailureSignal(Arc<tokio::sync::Notify>);
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for FailureSignal {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Message(bool);
        impl tracing::field::Visit for Message {
            fn record_debug(
                &mut self,
                field: &tracing::field::Field,
                value: &dyn std::fmt::Debug,
            ) {
                if field.name() == "message"
                    && format!("{value:?}")
                        .contains("upstream warm connection failed")
                {
                    self.0 = true;
                }
            }
        }
        let mut message = Message(false);
        event.record(&mut message);
        if message.0 {
            self.0.notify_one();
        }
    }
}

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
async fn provider(
    address: std::net::SocketAddr,
    tls: bool,
    bound: Duration,
) -> Result<(tempfile::TempDir, AppState, Arc<UpstreamProvider>)> {
    let directory = tempfile::tempdir()?;
    let mut profile = serde_json::json!({
        "protocol": "h1", "origins": [format!("{}://{address}", if tls {"tls"} else {"tcp"})],
        "capacity": {"max_live": 1, "max_idle": 1, "min_idle": 1,
            "replenish": {"initial_backoff": "1h", "max_backoff": "1h"}},
        "connection": {"connect_timeout": format!("{}s", bound.as_secs()), "idle_timeout": "1h"}
    });
    if tls {
        profile["tls"] = serde_json::json!({"server_name": "example.test", "handshake_timeout": "1h"});
    }
    // JSON is a YAML subset; all generated configuration is disposable.
    std::fs::write(
        directory.path().join("upstream.yaml"),
        serde_json::to_vec(
            &serde_json::json!({"upstreams": {"example": profile}}),
        )?,
    )?;
    let state = AppState::standalone("upstream-lifecycle", directory.path());
    let provider = Arc::new(UpstreamProvider::new(&state));
    let owner: &dyn ConfigRegistration = provider.as_ref();
    state.config_reader().prepare_owners(&[owner]).await?;
    provider
        .boot((None, None, None, Default::default()), &state)
        .await
        .map_err(|error| error.to_string())?;
    Ok((directory, state, provider))
}
fn run(
    provider: Arc<UpstreamProvider>,
    state: AppState,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<claviron::runtime::registry::Result<()>> {
    tokio::spawn(provider.run(
        (None, None, None, Default::default()),
        state,
        RunContext::new(cancel),
    ))
}
async fn head(socket: &mut TcpStream) -> Result<()> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(socket.read_u8().await?);
        if bytes.len() > 8192 {
            return Err("unexpected request header size".into());
        }
    }
    Ok(())
}
async fn eof(socket: &mut TcpStream) -> Result<()> {
    assert_eq!(socket.read(&mut [0; 1]).await?, 0, "warm transport must close");
    Ok(())
}
async fn cold(bound: Duration) -> Result<()> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let (_directory, state, provider) =
        provider(listener.local_addr()?, false, bound).await?;
    let cancel = CancellationToken::new();
    let mut future = Box::pin(provider.clone().run(
        (None, None, None, Default::default()),
        state.clone(),
        RunContext::new(cancel.clone()),
    ));
    assert!(poll!(future.as_mut()).is_pending());
    cancel.cancel();
    future.await.map_err(|error| error.to_string())?;
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    assert_eq!(
        provider.service("example").unwrap().stop().await,
        GateDrainOutcome::Drained
    );
    state.shutdown_token().cancel();
    println!("PASS cancellation before first warm poll");
    Ok(())
}
async fn accepted(bound: Duration) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let (_directory, state, provider) =
        provider(listener.local_addr()?, false, bound).await?;
    let service = provider.service("example").unwrap();
    for _ in 0..3 {
        let generation = service.generation();
        let cancel = CancellationToken::new();
        let running = run(provider.clone(), state.clone(), cancel.clone());
        let (mut socket, _) = listener.accept().await?;
        let client = service.clone();
        let request = tokio::spawn(async move {
            let response = client
                .handle(
                    http::Request::builder()
                        .uri("http://example.test/")
                        .body(claviron::http::empty_body())
                        .unwrap(),
                )
                .await;
            assert_eq!(response.status(), http::StatusCode::OK);
            response.into_body().collect().await.map(|body| body.to_bytes())
        });
        head(&mut socket).await?;
        cancel.cancel();
        running.await?.map_err(|error| error.to_string())?;
        assert_eq!(service.lifecycle(), UpstreamLifecycle::Running);
        assert_eq!(service.generation(), generation);
        assert!(!state.shutdown_token().is_cancelled());
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok").await?;
        assert_eq!(request.await??.as_ref(), b"ok");
        assert_eq!(service.stop().await, GateDrainOutcome::Drained);
        eof(&mut socket).await?;
        assert_ne!(service.start().await, generation);
    }
    assert_eq!(service.stop().await, GateDrainOutcome::Drained);
    state.shutdown_token().cancel();
    println!(
        "PASS three provider restarts preserve accepted IO and explicit profile stop/start"
    );
    Ok(())
}
async fn handshake(bound: Duration) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let (_directory, state, provider) =
        provider(listener.local_addr()?, true, bound).await?;
    let cancel = CancellationToken::new();
    let running = run(provider.clone(), state.clone(), cancel.clone());
    let (mut socket, _) = listener.accept().await?;
    let mut hello = [0; 4096];
    assert!(socket.read(&mut hello).await? > 0);
    // The backend deliberately never answers TLS; configured handshake budget is one hour.
    cancel.cancel();
    running.await?.map_err(|error| error.to_string())?;
    eof(&mut socket).await?;
    assert_eq!(
        provider.service("example").unwrap().stop().await,
        GateDrainOutcome::Drained
    );
    state.shutdown_token().cancel();
    println!("PASS cancellation joins a stalled TLS warm dial");
    Ok(())
}
async fn backoff(
    bound: Duration,
    failed: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let (_directory, state, provider) =
        provider(listener.local_addr()?, true, bound).await?;
    let cancel = CancellationToken::new();
    let running = run(provider.clone(), state.clone(), cancel.clone());
    let (mut socket, _) = listener.accept().await?;
    assert!(socket.read(&mut [0; 4096]).await? > 0);
    drop(socket);
    // The diagnostic event is emitted immediately before the configured retry wait.
    // On this current-thread runtime the warm task parks before this waiter resumes.
    failed.notified().await;
    cancel.cancel();
    running.await?.map_err(|error| error.to_string())?;
    assert_eq!(
        provider.service("example").unwrap().stop().await,
        GateDrainOutcome::Drained
    );
    state.shutdown_token().cancel();
    println!("PASS cancellation joins the configured one-hour retry wait");
    Ok(())
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let seconds: u64 = std::env::args()
        .nth(1)
        .ok_or("usage: upstream-lifecycle <per-case-bound-seconds>")?
        .parse()?;
    if seconds == 0 {
        return Err("diagnostic bound must be positive".into());
    }
    use tracing_subscriber::prelude::*;
    let failed = Arc::new(tokio::sync::Notify::new());
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(FailureSignal(failed.clone())),
    )?;
    let bound = Duration::from_secs(seconds);
    tokio::time::timeout(bound, cold(bound)).await??;
    tokio::time::timeout(bound, accepted(bound)).await??;
    tokio::time::timeout(bound, handshake(bound)).await??;
    tokio::time::timeout(bound, backoff(bound, failed)).await??;
    Ok(())
}
