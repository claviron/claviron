//! Process-local Orbit observations, independent of Python's lifecycle.

use claviron::AppState;
use claviron::orbit::transport::{EdgeConnected, EdgeDisconnected};
use claviron::runner::EdgeShutdownInitiated;
use claviron::runtime::registry;

pub fn register(state: &AppState) -> registry::Result<()> {
    state.events().listen::<EdgeConnected, _>(connected)?;
    state.events().listen::<EdgeDisconnected, _>(disconnected)?;
    state.events().listen::<EdgeShutdownInitiated, _>(shutdown_initiated)
}

async fn connected(
    _state: AppState,
    event: EdgeConnected,
) -> registry::Result<()> {
    tracing::info!(
        edge = %event.edge.name,
        fleet = %event.edge.fleet,
        application = %event.application,
        lane = event.edge.lane,
        "Orbit edge connected"
    );
    Ok(())
}

async fn disconnected(
    _state: AppState,
    event: EdgeDisconnected,
) -> registry::Result<()> {
    // One worker leaving is not the whole edge disconnecting.
    if event.last {
        tracing::info!(
            edge = %event.edge.name,
            fleet = %event.edge.fleet,
            application = %event.application,
            "Orbit edge disconnected"
        );
    }
    Ok(())
}

async fn shutdown_initiated(
    _state: AppState,
    event: EdgeShutdownInitiated,
) -> registry::Result<()> {
    tracing::info!(
        edge = %event.edge,
        fleet = %event.fleet,
        application = %event.application,
        "Orbit edge shutdown initiated"
    );
    Ok(())
}
