//! Isolated FastCGI lifecycle checks; no PHP interpreter or existing listener.
use bytes::Bytes;
use claviron::fcgi::{PhpFcgiService, config::FpmDocument, service::Stdin};
use claviron::runtime::{AppState, congif::Configurable};
use fastcgi_client::Params;
use http_body_util::BodyExt;
use std::error::Error;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
#[derive(Clone, Copy, Debug)]
enum Case {
    Idle,
    BeforeHead,
    Upload,
    Backpressure,
    Natural,
}
struct PendingInput;
impl AsyncRead for PendingInput {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}
fn record(
    kind: u8,
    body: &[u8],
) -> Vec<u8> {
    let length = u16::try_from(body.len()).unwrap().to_be_bytes();
    let mut bytes = vec![1, kind, 0, 1, length[0], length[1], 0, 0];
    bytes.extend_from_slice(body);
    bytes
}
async fn request(
    socket: &mut UnixStream,
    params_only: bool,
) -> Result<()> {
    loop {
        let mut head = [0; 8];
        socket.read_exact(&mut head).await?;
        let length = u16::from_be_bytes([head[4], head[5]]) as usize;
        let mut data = vec![0; length + head[6] as usize];
        socket.read_exact(&mut data).await?;
        if length == 0 && head[1] == if params_only { 4 } else { 5 } {
            return Ok(());
        }
    }
}
async fn peer_closed(socket: &mut UnixStream) -> Result<()> {
    let mut bytes = [0; 4096];
    while socket.read(&mut bytes).await? != 0 {}
    Ok(())
}
async fn finish(socket: &mut UnixStream) -> Result<()> {
    socket
        .write_all(&record(
            6,
            b"Status: 200\r\nContent-Type: text/plain\r\n\r\nok",
        ))
        .await?;
    socket.write_all(&record(3, &[0; 8])).await?;
    Ok(())
}
async fn exercise(
    case: Case,
    pooled: bool,
    bound: Duration,
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("fpm.sock");
    let listener = UnixListener::bind(&path)?;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let backend = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        request(&mut socket, matches!(case, Case::Upload)).await?;
        if matches!(case, Case::Natural) {
            socket
                .write_all(&record(
                    6,
                    b"Status: 200\r\nContent-Type: text/plain\r\n\r\n",
                ))
                .await?;
            ready_tx.send(()).unwrap();
            finish_rx.await?;
            socket.write_all(&record(6, b"ok")).await?;
            socket.write_all(&record(3, &[0; 8])).await?;
            return Ok::<_, Box<dyn Error + Send + Sync>>(());
        }
        if matches!(case, Case::Idle | Case::Backpressure) {
            socket
                .write_all(&record(
                    6,
                    b"Status: 200\r\nContent-Type: text/plain\r\n\r\n",
                ))
                .await?;
        }
        ready_tx.send(()).unwrap();
        if matches!(case, Case::Backpressure) {
            // More than the response channel can retain; closure must also
            // interrupt an upload/read/write that is currently backpressured.
            let chunk = record(6, &[42; 65_535]);
            for _ in 0..128 {
                if socket.write_all(&chunk).await.is_err() {
                    break;
                }
            }
        }
        peer_closed(&mut socket).await?;
        let (mut next, _) = listener.accept().await?;
        request(&mut next, false).await?;
        finish(&mut next).await?;
        Ok(())
    });
    let service = PhpFcgiService::from_state(AppState::standalone(
        "fcgi-lifecycle",
        directory.path(),
    ));
    let mut entry = serde_json::json!({ "unix": path, "connect_timeout": format!("{}s", bound.as_secs()), "head_timeout": format!("{}s", bound.as_secs()), "exec_timeout": format!("{}s", bound.as_secs()) });
    if pooled {
        entry["pool"] = serde_json::json!({ "min_live": 0, "max_idle": 1, "max_live": 1, "acquire_timeout": format!("{}s", bound.as_secs()) });
    }
    let config: FpmDocument = serde_json::from_value(
        serde_json::json!({ "housekeep": { "enabled": false }, "fpm": { "example": entry } }),
    )?;
    service.configure(&config).await?;
    let producer = service.clone();
    let input = if matches!(case, Case::Upload) {
        Stdin::Live(Box::new(PendingInput))
    } else {
        Stdin::Whole(Bytes::new())
    };
    let caller = tokio::spawn(async move {
        producer
            .fcgi_execute_stream(
                &["example".into()],
                Params::default()
                    .request_method("GET")
                    .script_name("/index.php"),
                input,
            )
            .await
            .map_err(|e| e.to_string())
    });
    ready_rx.await?;
    match case {
        Case::BeforeHead | Case::Upload => {
            caller.abort();
            match caller.await {
                Err(error) => assert!(error.is_cancelled()),
                Ok(_) => panic!("cancelled caller completed"),
            }
        }
        Case::Idle | Case::Backpressure => {
            let (status, _, body) = caller.await??;
            assert_eq!(status.as_u16(), 200);
            drop(body);
        }
        Case::Natural => {
            let (_, _, body) = caller.await??;
            finish_tx.send(()).unwrap();
            assert_eq!(body.collect().await?.to_bytes(), "ok");
            backend.await??;
            return Ok(());
        }
    }
    // At max_live=1 this cannot complete until abandonment releases capacity.
    let (_, _, body) = service
        .fcgi_execute_stream(
            &["example".into()],
            Params::default().request_method("GET").script_name("/index.php"),
            Stdin::Whole(Bytes::new()),
        )
        .await?;
    assert_eq!(body.collect().await?.to_bytes(), "ok");
    backend.await??;
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let seconds = std::env::args()
        .nth(1)
        .ok_or("usage: fcgi-lifecycle DEADLINE_SECONDS")?
        .parse::<u64>()?;
    if seconds == 0 {
        return Err("deadline must be positive".into());
    }
    let bound = Duration::from_secs(seconds);
    for pooled in [false, true] {
        for case in [
            Case::Idle,
            Case::BeforeHead,
            Case::Upload,
            Case::Backpressure,
            Case::Natural,
        ] {
            tokio::time::timeout(bound, exercise(case, pooled, bound))
                .await??;
            println!("passed: pooled={pooled} case={case:?}");
        }
    }
    Ok(())
}
