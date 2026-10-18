//! Connection-local structured cleanup. The cancellable protocol driver
//! borrows its task set; the connection owner outlives both driver and children.

use std::future::Future;
use std::panic::AssertUnwindSafe;

use claviron_runtime::gate::Permit;
use futures_util::FutureExt;
use tokio::task::JoinSet;
use tracing::{debug, error};

pub(super) enum DriverExit {
    Complete,
    Forced,
    Panicked,
}

pub(super) async fn drive(
    driver: impl Future<Output = ()>,
    permit: &Permit,
) -> DriverExit {
    tokio::select! {
        biased;
        _ = permit.wait_forced_shutdown() => DriverExit::Forced,
        result = AssertUnwindSafe(driver).catch_unwind() => match result {
            Ok(()) => DriverExit::Complete,
            Err(_) => {
                // Do not resume a partially unwound driver. Only its outer
                // owner survives to close transport and join child cleanup.
                error!("H3 connection driver panicked; joining owned streams");
                DriverExit::Panicked
            }
        }
    }
}

pub(super) async fn finish(
    mut streams: JoinSet<()>,
    permit: &Permit,
    exit: DriverExit,
    close: impl FnOnce(),
) {
    if matches!(exit, DriverExit::Complete) {
        tokio::select! {
            _ = join(&mut streams) => return,
            _ = permit.wait_forced_shutdown() => {}
        }
    }
    close();
    streams.abort_all();
    join(&mut streams).await;
    debug!("H3 connection stopped after joining owned streams");
}

async fn join(streams: &mut JoinSet<()>) {
    while let Some(result) = streams.join_next().await {
        if let Err(error) = result
            && !error.is_cancelled()
        {
            error!(%error, "H3 stream task failed during drain");
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/quic/listener/drain.rs"]
mod tests;
