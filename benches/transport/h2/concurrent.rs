//! HTTP/2 cleartext between two processes, with nothing else in the way.
//!
//! The same exchange as `link-concurrent`, over a socket. The application is
//! the runner's production TCP door, serving HTTP/2 by prior knowledge. The
//! edge is the Edge's own upstream: `UpstreamProvider` booted from an
//! `upstream.yaml` naming that door, its H2 pool sized by `--max-live` and
//! `--max-streams`, each exchange through `UpstreamService::handle`. Only the
//! Edge's listener and host routing are left out, as they are for the Link.
//!
//! ```sh
//! cargo bench --all-features --bench h2-concurrent -- \
//!   --workload plain --max-live 1 --max-streams 128 --in-flight 8 --edge-threads 2 \
//!   --application-threads 2 --warmup-seconds 2 --measurement-seconds 10 \
//!   --samples 5 --output benches/results/h2/plain.json
//! ```

#[cfg(unix)]
#[path = "../../support/application.rs"]
mod application;

#[cfg(unix)]
mod h2 {
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;

    use bytes::Bytes;
    use claviron::http::config::{ListenerDefinition, ListenerOverrides};
    use claviron::http::listener::HttpProtocol;
    use claviron::http::session::SessionOwner;
    use claviron::http::{bytes_body, empty_body};
    use claviron::runtime::AppState;
    use claviron::runtime::gate::Gate;
    use claviron::transport::tcp::listener::TcpListenerSnapshot;
    use claviron::transport::tcp::plain;
    use claviron::upstream::proxy::service::UpstreamService;
    use claviron::upstream::{self, UpstreamProvider};
    use http::StatusCode;
    use http_body_util::BodyExt;
    use tokio::net::TcpListener;
    use tokio::runtime::Runtime;
    use tokio::sync::oneshot;

    use super::application::{
        Args, Bench, Result, Serving, Transport, Workload,
    };

    /// The profile's name in the bench's `upstream.yaml`.
    const UPSTREAM: &str = "application";

    pub struct H2 {
        service: UpstreamService,
        /// Holds the provider and its pools for as long as the bench runs.
        _state: AppState,
        config: PathBuf,
    }

    impl Transport for H2 {
        const NAME: &'static str = "h2";

        fn serve(
            runtime: &Runtime,
            _args: &Args,
            _fleet: &str,
        ) -> Result<Serving> {
            let listener =
                runtime.block_on(TcpListener::bind("127.0.0.1:0"))?;
            let address = listener.local_addr()?;
            let config = Arc::new(
                ListenerDefinition::tcp(address, vec![HttpProtocol::H2])
                    .resolve(
                        "application".parse()?,
                        &ListenerOverrides::default(),
                    )?,
            );
            let snapshot = Arc::new(
                plain::Single::shared(config, Arc::new(Bench))
                    .map_err(|error| error.into_std())?
                    .prior_knowledge(false),
            );
            let config = snapshot.listener();
            let gate = Gate::new(
                config.limits.max_connections.map(|value| value.get()),
                config.limits.acquire_timeout,
            );
            let sessions = SessionOwner::new(config.id.clone());
            let (stop, stopped) = oneshot::channel::<()>();
            // Accepting is this bench's; each connection is served by the
            // door's own per-connection path.
            let accepting = runtime.spawn(async move {
                let mut connections = tokio::task::JoinSet::new();
                tokio::pin!(stopped);
                loop {
                    tokio::select! {
                        _ = &mut stopped => break,
                        accepted = listener.accept() => {
                            let (stream, peer) = accepted?;
                            let scope = sessions.bind(
                                gate.try_enter().map_err(|error| error.to_string())?,
                            );
                            connections.spawn(plain::serve(
                                stream,
                                peer,
                                snapshot.clone(),
                                scope,
                            ));
                        }
                    }
                }
                gate.graceful_shutdown(None);
                while connections.join_next().await.is_some() {}
                sessions.close().join().await;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            });
            Ok(Serving {
                address: Some(address),
                stop: Box::new(move |runtime| {
                    let _ = stop.send(());
                    runtime.block_on(accepting)??;
                    Ok(())
                }),
            })
        }

        fn connect(
            runtime: &Runtime,
            args: &Args,
            fleet: &str,
            address: Option<SocketAddr>,
        ) -> Result<Self> {
            let address = address.ok_or("the application gave no address")?;
            let max_live =
                args.max_live.ok_or("--max-live is required for HTTP/2")?;
            let max_streams = args
                .max_streams
                .ok_or("--max-streams is required for HTTP/2")?;
            // The Edge's own upstream, declared as an operator would and
            // booted the way the server boots it.
            let config = std::env::temp_dir().join(format!("{fleet}-upstream"));
            std::fs::create_dir_all(&config)?;
            std::fs::write(
                config.join("upstream.yaml"),
                format!(
                    "upstreams:\n  {UPSTREAM}:\n    protocol: h2\n    \
                     origins: [tcp://{address}]\n    \
                     connection: {{ connect_timeout: off }}\n    \
                     capacity: {{ max_live: {max_live}, max_streams: {max_streams} }}\n"
                ),
            )?;
            let state =
                AppState::standalone_in_fleet("h2-bench-edge", fleet, &config);
            let service = runtime
                .block_on(async {
                    upstream::register(&state)?;
                    let mut manager = claviron::runtime::Runtime::default();
                    state.registry_ref().insert(manager.manager())?;
                    manager.configure(&state).await?;
                    state.registry_ref().boot_all(&state).await?;
                    state
                        .registry_ref()
                        .resolve::<UpstreamProvider>()
                        .and_then(|provider| provider.service(UPSTREAM))
                        .ok_or_else(|| {
                            claviron::registry::Error::msg(
                                "the upstream was not prepared",
                            )
                        })
                })
                .map_err(|error| error.into_std())?;
            Ok(Self { service, _state: state, config })
        }

        async fn exchange(
            &self,
            _caller: usize,
            workload: Workload,
            body: &Bytes,
        ) -> Result<Bytes> {
            let length = body.len();
            let body = match workload {
                Workload::Plain => empty_body(),
                Workload::Echo => bytes_body(body.clone()),
            };
            let response =
                self.service.handle(workload.request(body, length)).await;
            if response.status() != StatusCode::OK {
                return Err(super::application::ResponseStatus(
                    response.status(),
                )
                .into());
            }
            Ok(response.into_body().collect().await?.to_bytes())
        }
    }

    impl Drop for H2 {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.config);
        }
    }
}

#[cfg(unix)]
fn main() -> application::Result<()> {
    application::run::<h2::H2>()
}

#[cfg(not(unix))]
fn main() {
    panic!("the HTTP/2 benchmark needs two Unix processes and getrusage");
}
