//! Orbit Link between two processes, with nothing else in the way.
//!
//! The edge calls the production `LinkBackend` — the dispatch an Edge runs for
//! an `orbit:` route — with requests it builds itself, so no listener, HTTP
//! decoding or TLS is measured. The application is the production `OrbitDoor`.
//! Between them is only the Link: inbox frames, bells and, where a body
//! streams, the exchange's stream. A voluntary context switch is here almost
//! always a park that a bell ended.
//!
//! ```sh
//! cargo bench --all-features --bench link-concurrent -- \
//!   --workload plain --in-flight 8 --edge-threads 2 --application-threads 2 \
//!   --warmup-seconds 2 --measurement-seconds 10 --samples 5 \
//!   --output benches/results/link/plain.json
//! ```

#[cfg(unix)]
#[path = "../../support/application.rs"]
mod application;

#[cfg(unix)]
mod link {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use bytes::Bytes;
    use claviron::http::config::routes::OrbitRouteConfig;
    use claviron::http::policy::{Buffering, Limit, RequestPolicyOverride};
    use claviron::http::{ServiceResult, bytes_body, empty_body};
    use claviron::orbit::link::application::{ApplicationService, LinkJoined};
    use claviron::pipeline::link::LinkBackend;
    use claviron::pipeline::{DispatchCall, DispatchStep};
    use claviron::runtime::{AppState, Bootable, FleetName, RunContext};
    use claviron::transport::orbit::OrbitDoor;
    use http::StatusCode;
    use http_body_util::BodyExt;
    use tokio::runtime::Runtime;

    use super::application::{
        Args, Bench, Result, Serving, Transport, Workload,
    };

    /// The name the application advertises in its lane.
    const APPLICATION: &str = "link-bench";

    pub struct Link {
        backend: LinkBackend,
        /// Holds the edge's Link membership for as long as the backend runs.
        _state: AppState,
    }

    impl Transport for Link {
        const NAME: &'static str = "link";

        fn serve(
            runtime: &Runtime,
            args: &Args,
            fleet: &str,
        ) -> Result<Serving> {
            let state =
                AppState::standalone_in_fleet("link-bench-app", fleet, ".");
            let mut joined = state.events().subscribe::<LinkJoined>();
            let context = RunContext::new(Default::default());
            let stop = context.cancellation_token();
            let door = OrbitDoor::new(
                APPLICATION,
                "rust",
                args.max_in_flight.map(std::num::NonZeroUsize::get),
            );
            let serving = runtime.spawn(async move {
                door.serve(Arc::new(Bench), None, state, context).await
            });
            // The door says when its lane is claimed; nothing is polled.
            runtime.block_on(joined.recv())?;
            Ok(Serving {
                address: None,
                stop: Box::new(move |runtime| {
                    stop.cancel();
                    runtime
                        .block_on(serving)?
                        .map_err(|error| error.to_string())?;
                    Ok(())
                }),
            })
        }

        fn connect(
            runtime: &Runtime,
            args: &Args,
            fleet: &str,
            _address: Option<SocketAddr>,
        ) -> Result<Self> {
            let state =
                AppState::standalone_in_fleet("link-bench-edge", "lbedge", ".");
            let service = Arc::new(ApplicationService::default());
            state
                .registry_ref()
                .register(Arc::clone(&service))
                .map_err(|error| error.into_std())?;
            runtime
                .block_on(service.boot((), &state))
                .map_err(|error| error.into_std())?;
            let policy = RequestPolicyOverride {
                buffering: Some(Buffering::StreamDefault),
                buffer_size: args.buffer_size.map(Limit::Value),
                queue_timeout: args.queue_timeout,
                ..Default::default()
            };
            let route = OrbitRouteConfig {
                fleet_name: FleetName::try_new(fleet)?,
                application: APPLICATION.to_owned(),
            };
            // The lane is joined here, inside the runtime that reads replies.
            let backend = runtime
                .block_on(async {
                    LinkBackend::new(&state, &[route], None, policy, false)
                })
                .ok_or("no Link route was prepared")?;
            Ok(Self { backend, _state: state })
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
            let request = workload.request(body, length);
            let result = match self.backend.call(request, None) {
                DispatchCall::Ready(result) => result,
                DispatchCall::Pending(work) => work.await,
            };
            let ServiceResult::Handled(response) = result else {
                return Err("the Link route did not take the request".into());
            };
            if response.status() != StatusCode::OK {
                return Err(super::application::ResponseStatus(
                    response.status(),
                )
                .into());
            }
            Ok(response.into_body().collect().await?.to_bytes())
        }
    }
}

#[cfg(unix)]
fn main() -> application::Result<()> {
    application::run::<link::Link>()
}

#[cfg(not(unix))]
fn main() {
    panic!("the Link benchmark needs two Unix processes and getrusage");
}
