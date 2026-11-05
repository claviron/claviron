use std::{
    convert::Infallible,
    future::Future,
    io::{BufRead, Write},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{oneshot, watch},
    task::JoinSet,
};

use super::{
    Args, Command, Mode, Reply, Result, ServerExecution, Target, allocation,
    cpu::Usage, execution, process, tls,
};
use crate::support::{edge, http1::Server, plain};

pub(super) fn run(args: &Args) -> Result<()> {
    // axum::serve owns its connection tasks. Observe even a detached task panic
    // so a completed last reply cannot turn a server failure into a valid run.
    let panicked = Arc::new(AtomicBool::new(false));
    let observed = panicked.clone();
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        observed.store(true, Ordering::Relaxed);
        previous(info);
    }));
    let server = execution::Running::start(args)?;
    let mut output = std::io::stdout().lock();
    let trust = match args.mode {
        Mode::Plain => None,
        Mode::Tls => Some(tls::Identity::get().trust.to_vec()),
    };
    process::write(
        &mut output,
        &Reply::Ready { address: server.address(), trust },
    )?;
    let mut began = None;
    let control = (|| -> Result<()> {
        for line in std::io::stdin().lock().lines() {
            let command: Command = serde_json::from_str(&line?)?;
            match command {
                Command::Begin => {
                    if began.replace(Usage::process()?).is_some() {
                        return Err("CPU sample already started".into());
                    }
                    process::write(&mut output, &Reply::Begun)?;
                }
                Command::End => {
                    let used = Usage::process()?
                        .since(began.take().ok_or("CPU sample not started")?)?;
                    process::write(&mut output, &Reply::Used(used))?;
                }
                Command::BeginCount => {
                    if !allocation::begin() {
                        return Err("allocation count already started".into());
                    }
                    process::write(&mut output, &Reply::Counting)?;
                }
                Command::EndCount => {
                    let allocated = allocation::end()
                        .ok_or("allocation count not started")?;
                    process::write(&mut output, &Reply::Allocated(allocated))?;
                }
                Command::Shutdown => break,
            }
        }
        Ok(())
    })();
    server.shutdown()?;
    control?;
    if panicked.load(Ordering::Relaxed) {
        return Err("a server task panicked".into());
    }
    process::write(&mut output, &Reply::Stopped)?;
    output.flush()?;
    Ok(())
}

pub(super) async fn start(args: &Args) -> Result<Server> {
    let expected = match args.server_execution {
        ServerExecution::Shared => tokio::runtime::RuntimeFlavor::MultiThread,
        ServerExecution::CurrentThread => {
            tokio::runtime::RuntimeFlavor::CurrentThread
        }
    };
    assert_eq!(tokio::runtime::Handle::current().runtime_flavor(), expected);
    match args.target {
        Target::Axum => Ok(crate::support::http1::axum_plain().await),
        Target::Hyper => {
            let acceptor = match args.mode {
                Mode::Plain => None,
                Mode::Tls => Some(tls::Identity::get().acceptor()?),
            };
            accept(args.connections.get(), false, move |stream, _, stop| {
                let acceptor = acceptor.clone();
                async move {
                    stream.set_nodelay(true).unwrap();
                    match acceptor {
                        None => hyper_connection(stream, stop).await,
                        Some(acceptor) => {
                            let stream = acceptor
                                .accept(stream)
                                .await
                                .expect("benchmark TLS handshake");
                            hyper_connection(stream, stop).await
                        }
                    }
                }
            })
            .await
        }
        Target::EdgeOther if args.mode == Mode::Tls => {
            let edge = edge::secure_connection(
                edge::Owner::Other,
                tls::Identity::get().certified()?,
            );
            let sessions = edge.sessions;
            let handler = edge.handler;
            edge_connections(
                args.connections.get(),
                &edge.listener,
                move |stream, peer, scope| {
                    handler.clone().serve(stream, peer, scope)
                },
                move || {
                    sessions
                        .unlink()
                        .expect("remove the benchmark TLS session table")
                },
            )
            .await
        }
        Target::EdgeOther | Target::EdgeAxum | Target::EdgeRoutable => {
            let owner = match args.target {
                Target::EdgeOther => edge::Owner::Other,
                Target::EdgeAxum => edge::Owner::Axum,
                Target::EdgeRoutable => edge::Owner::Routable,
                _ => unreachable!(),
            };
            let edge = edge::connection(owner);
            // Clone the listener snapshot, not a connection's HostService.
            // The production handler constructs a fresh ConnCtx and host cache.
            let handler = edge.handler;
            edge_connections(
                args.connections.get(),
                &edge.listener,
                move |stream, peer, scope| {
                    handler.clone().serve(stream, peer, scope)
                },
                || {},
            )
            .await
        }
    }
}

/// One native Hyper connection over cleartext or TLS, until the peer ends it
/// or the server drains.
async fn hyper_connection<I>(
    stream: I,
    mut stop: watch::Receiver<()>,
) where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let connection = hyper::server::conn::http1::Builder::new()
        .keep_alive(true)
        .half_close(false)
        .pipeline_flush(false)
        .header_read_timeout(None)
        .serve_connection(
            TokioIo::new(stream),
            service_fn(|request| async {
                Ok::<_, Infallible>(plain::response(request))
            }),
        );
    tokio::pin!(connection);
    tokio::select! {
        result = &mut connection => result.unwrap(),
        _ = stop.changed() => {
            connection.as_mut().graceful_shutdown();
            connection.await.unwrap();
        }
    }
}

/// Accept for a prepared Edge listener and serve each stream with its native
/// handler. `released` runs once every connection and session has finished.
async fn edge_connections<S, A>(
    connections: usize,
    listener: &claviron::http::config::ListenerConfig,
    serve: S,
    released: impl FnOnce() + Send + 'static,
) -> Result<Server>
where
    S: Fn(TcpStream, SocketAddr, claviron::http::session::ConnectionScope) -> A
        + Send
        + 'static,
    A: Future<Output = ()> + Send + 'static,
{
    let limits = &listener.limits;
    let gate = claviron::runtime::gate::Gate::new(
        limits.max_connections.map(|limit| limit.get()),
        limits.acquire_timeout,
    );
    let sessions = std::sync::Arc::new(
        claviron::http::session::SessionOwner::new(listener.id.clone()),
    );
    let retained = sessions.clone();
    let retained_gate = gate.clone();
    let server = accept(connections, true, move |stream, peer, mut stop| {
        let scope = sessions
            .bind(gate.try_enter().expect("admit benchmark connection"));
        let gate = gate.clone();
        let connection = serve(stream, peer, scope);
        async move {
            tokio::pin!(connection);
            tokio::select! {
                () = &mut connection => {},
                _ = stop.changed() => {
                    gate.graceful_shutdown(None);
                    connection.await;
                }
            }
        }
    })
    .await?;
    let Server { address, stop, task, selects_host } = server;
    let task = tokio::spawn(async move {
        task.await.expect("native accepted connections finish");
        match std::sync::Arc::try_unwrap(retained) {
            Ok(sessions) => sessions.close().join().await,
            Err(_) => {
                panic!("connection tasks retained the session owner")
            }
        }
        assert_eq!(
            retained_gate.count(),
            0,
            "connection capacity must be released"
        );
        released();
    });
    Ok(Server { address, stop, task, selects_host })
}

async fn accept<F, A>(
    connections: usize,
    selects_host: bool,
    serve: F,
) -> Result<Server>
where
    F: Fn(TcpStream, SocketAddr, watch::Receiver<()>) -> A + Send + 'static,
    A: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (drain, draining) = watch::channel(());
        let mut children = JoinSet::new();
        let mut stopping = false;
        for _ in 0..connections {
            let accepted = tokio::select! {
                _ = &mut stopped => { stopping = true; break; },
                accepted = listener.accept() => accepted.unwrap(),
            };
            children.spawn(serve(accepted.0, accepted.1, draining.clone()));
        }
        drop(listener);
        drop(serve);
        if !stopping {
            let _ = stopped.await;
        }
        drain.send_replace(());
        while let Some(result) = children.join_next().await {
            result.expect("benchmark connection must finish successfully");
        }
    });
    Ok(Server { address, stop, task, selects_host })
}
