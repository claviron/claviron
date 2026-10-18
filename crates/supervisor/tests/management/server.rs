use std::path::PathBuf;

use claviron_orbit::FleetEventCodec;
use claviron_runtime::AppState;

use super::{
    SERVER_RELOADED_BYTES, ServerManager, ServerManagerError, ServerReloaded,
};

#[tokio::test]
async fn commands_are_rejected_during_shutdown_without_signalling() {
    let state = AppState::standalone("server-manager-test", PathBuf::from("."));
    state.initiate_shutdown();
    let manager = ServerManager::new(&state);

    assert!(matches!(
        manager.reload().await,
        Err(ServerManagerError::ShuttingDown)
    ));
    assert!(matches!(
        manager.shutdown(),
        Err(ServerManagerError::ShuttingDown)
    ));
}

#[test]
fn server_reloaded_round_trips_its_revision() {
    let event = ServerReloaded::new(u64::MAX);
    let payload = event.encode_orbit();

    assert_eq!(payload.len(), SERVER_RELOADED_BYTES);
    assert_eq!(
        ServerReloaded::decode_orbit(&payload).expect("decode event"),
        event
    );
}
