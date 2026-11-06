use std::convert::Infallible;

use bytes::Bytes;
use claviron_http::policy::HttpPolicy;
use claviron_http::session::{SessionOwner, SessionSpawner};
use claviron_runtime::gate::Gate;
use claviron_transport::tcp::listener::TcpListenerSnapshot;
use claviron_transport::tcp::plain;
use http::Response;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};

use super::*;
use crate::routing::{Owner, Routing, ensure_application, register_owner};

/// The application, reporting what the door attached to its request.
fn application() -> Arc<Application> {
    let state = AppState::standalone("runner-socket", ".");
    register_owner(
        &state,
        Owner::new(tower::service_fn(|request: AppRequest| async move {
            let extensions = request.extensions();
            let answer = format!(
                "facts={} sessions={} received={}",
                extensions.get::<Arc<ConnCtx>>().is_some(),
                extensions.get::<SessionSpawner>().is_some(),
                extensions.get::<ReceivedAt>().is_some(),
            );
            Ok::<_, Infallible>(Response::new(claviron_http::bytes_body(
                Bytes::from(answer),
            )))
        })),
    )
    .unwrap();
    state
        .registry_ref()
        .insert(Arc::new(Routing::new(HttpPolicy::unlimited())))
        .unwrap();
    #[cfg(feature = "axum")]
    claviron_pipeline::routing::register(&state).unwrap();
    ensure_application(&state).unwrap();
    state.registry_ref().resolve::<Application>().unwrap()
}

async fn ask<S: AsyncRead + AsyncWrite + Unpin>(mut stream: S) -> String {
    stream
        .write_all(
            b"GET / HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).await.unwrap();
    answer
}

/// What each door gives the application: a TCP connection its facts, a Unix
/// one none because it has no addresses. An ordinary request carries no owner
/// of sessions; only one that can take its connection over does.
#[tokio::test]
async fn each_door_gives_the_application_what_it_knows() {
    let door = Doors::new(Vec::new(), Protocols::default())
        .cleartext(
            None,
            Arc::new(Front {
                application: application(),
                trust: Arc::default(),
            }),
        )
        .unwrap();
    let limits = &door.listener().limits;
    let gate = Gate::new(
        limits.max_connections.map(|value| value.get()),
        limits.acquire_timeout,
    );
    let sessions = SessionOwner::new(door.listener().id.clone());

    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, accepted) = tokio::join!(
        TcpStream::connect(socket.local_addr().unwrap()),
        socket.accept()
    );
    let (stream, peer) = accepted.unwrap();
    let scope = sessions.bind(gate.try_enter().unwrap());
    let serving = tokio::spawn(plain::serve(stream, peer, door.clone(), scope));
    let answer = ask(client.unwrap()).await;
    serving.await.unwrap();
    assert!(
        answer.ends_with("facts=true sessions=false received=true"),
        "{answer}"
    );

    let (client, stream) = UnixStream::pair().unwrap();
    let scope = sessions.bind(gate.try_enter().unwrap());
    let serving = tokio::spawn(claviron_transport::unix::serve_stream(
        stream, door, scope,
    ));
    let answer = ask(client).await;
    serving.await.unwrap();
    assert!(
        answer.ends_with("facts=false sessions=false received=true"),
        "{answer}"
    );
}
