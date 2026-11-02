//! Cold execution placement shared by native and Edge HTTP controls.

use std::net::SocketAddr;

use claviron::runtime::{
    ExecutionDriver, RunContext, execution::CurrentThreadDriver,
};
use tokio::{runtime::Runtime, sync::oneshot, task::JoinHandle};

use super::{Args, Result, ServerExecution, server};
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
        if matches!(args.server_execution, ServerExecution::Shared) {
            let server = runtime.block_on(server::start(args))?;
            return Ok(Self { runtime, server, current: None });
        }

        let driver = CurrentThreadDriver::default();
        let listener = "benchmark".parse()?;
        let prepared = driver
            .prepare(
                claviron::http::config::PlainExecution::CurrentThread
                    .for_listener(&listener),
            )
            .map_err(claviron::registry::Error::into_std)?;
        let args = args.clone();
        let (ready, received) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let task = runtime.block_on(async {
            let context = RunContext::new(Default::default());
            tokio::spawn(prepared.execute(
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
            )).await
        });
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
