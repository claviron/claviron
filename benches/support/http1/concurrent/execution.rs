//! Cold execution placement shared by native and Edge HTTP controls.

use std::net::SocketAddr;

use claviron::runtime::{
    ExecutionDriver, ExecutionOwner, PreparedExecution, RunContext,
    execution::{CurrentThreadDriver, execution_spec},
};
use tokio::{runtime::Runtime, sync::oneshot, task::JoinHandle};

use claviron::Execution;

use super::{Args, Result, server};
use crate::support::http1::Server;

pub(super) struct Running {
    runtime: Runtime,
    server: Server,
    current: Option<Current>,
}

struct Current {
    driver: CurrentThreadDriver,
    release: oneshot::Sender<()>,
    task: JoinHandle<claviron::registry::Result<()>>,
}

impl Running {
    pub(super) fn start(args: &Args) -> Result<Self> {
        // Keep the main runtime budget unchanged when HTTP moves to its own
        // executor, matching the production listener's execution contract.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(args.server_threads.get())
            .enable_all()
            .build()?;
        let listener = args.listener()?;
        if listener.execution == Execution::Inherit {
            let server = runtime.block_on(server::start(args))?;
            return Ok(Self { runtime, server, current: None });
        }

        let driver = CurrentThreadDriver::default();
        let prepared = driver
            .prepare(
                &ExecutionOwner::of::<Self>(listener.id.to_string()),
                execution_spec(listener.execution),
            )
            .map_err(claviron::registry::Error::into_std)?;
        let (ready, received) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let task =
            spawn(&runtime, prepared.as_ref(), args.clone(), ready, released);
        let server = match runtime.block_on(received) {
            Ok(server) => server,
            Err(error) => {
                drop(release);
                // Join failed startup before releasing its executor, too.
                let joined = runtime.block_on(task);
                let finalized = runtime.block_on(driver.finalize());
                joined?.map_err(claviron::registry::Error::into_std)?;
                finalized.map_err(claviron::registry::Error::into_std)?;
                return Err(error.into());
            }
        };
        Ok(Self {
            runtime,
            server,
            current: Some(Current { driver, release, task }),
        })
    }

    pub(super) fn address(&self) -> SocketAddr {
        self.server.address
    }

    pub(super) fn shutdown(self) -> Result<()> {
        let Self { runtime, server, current } = self;
        runtime.block_on(async {
            server.shutdown().await;
            if let Some(Current { driver, release, task }) = current {
                let _ = release.send(());
                let joined = task.await;
                let drained = driver.drain().await;
                let finalized = driver.finalize().await;
                joined?.map_err(claviron::registry::Error::into_std)?;
                drained.map_err(claviron::registry::Error::into_std)?;
                finalized.map_err(claviron::registry::Error::into_std)?;
            }
            Ok(())
        })
    }
}

fn spawn(
    runtime: &Runtime,
    prepared: &dyn PreparedExecution,
    args: Args,
    ready: oneshot::Sender<Server>,
    released: oneshot::Receiver<()>,
) -> JoinHandle<claviron::registry::Result<()>> {
    let context = RunContext::new(Default::default());
    runtime.spawn(prepared.execute(
        Box::pin(async move {
            let server = server::start(&args)
                .await
                .map_err(claviron::registry::Error::Other)?;
            if let Err(server) = ready.send(server) {
                server.shutdown().await;
                return Err(claviron::registry::Error::msg(
                    "benchmark readiness receiver closed",
                ));
            }
            let _ = released.await;
            Ok(())
        }),
        context,
    ))
}
