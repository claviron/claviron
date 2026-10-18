//! Optional bridge from Link lifecycle commands to the runner's existing
//! application manager. The runner stays alive while its hosted application
//! changes state.

use claviron_app::fleet::{FleetApplicationManager, link_snapshot_rows};
use claviron_app::management::{
    ApplicationFailed, ApplicationStarted, ApplicationStopped,
};
use claviron_app::{
    ApplicationId, ApplicationManager, ApplicationName, ApplicationRuntime,
};
use claviron_orbit::transport::TransportSegment;
use claviron_runtime::{AppState, registry};

use crate::LinkCommandReceived;

/// Accept Start, Stop, and Restart for a runtime-qualified application in
/// this runner. `launch::register_doors` installs this for Orbit launches;
/// custom ingress compositions may install it once themselves. A runner
/// may host several applications; the Link address never substitutes for the
/// hosted application's identity.
pub fn register_link_application_management(
    state: &AppState
) -> registry::Result<()> {
    state.events().listen::<ApplicationStarted, _>(|state, _| async move {
        publish_if_linked(&state)
    })?;
    state.events().listen::<ApplicationStopped, _>(|state, _| async move {
        publish_if_linked(&state)
    })?;
    state.events().listen::<ApplicationFailed, _>(|state, _| async move {
        publish_if_linked(&state)
    })?;
    state.events().listen::<LinkCommandReceived, _>(
        |state, command| async move {
            if !matches!(command.name.as_str(), "start" | "stop" | "restart") {
                return Ok(());
            }
            let target = command.target.ok_or_else(|| {
                registry::Error::msg(
                    "Link application command has no hosted application id",
                )
            })?;
            let runtime = ApplicationRuntime::new(target.runtime)
                .map_err(|error| registry::Error::msg(error.to_string()))?;
            let name = ApplicationName::new(target.name)
                .map_err(|error| registry::Error::msg(error.to_string()))?;
            let id = ApplicationId::new(runtime, name);
            if state.is_standalone() {
                let manager = state
                    .registry_ref()
                    .resolve::<ApplicationManager>()
                    .ok_or_else(|| {
                        registry::Error::msg(
                            "ApplicationManager is not registered",
                        )
                    })?;
                let result = match command.name.as_str() {
                    "start" => manager.start(&id).await,
                    "stop" => manager.stop(&id).await,
                    "restart" => manager.restart(&id).await,
                    _ => unreachable!("validated Link command"),
                };
                return result
                    .map_err(|error| registry::Error::msg(error.to_string()));
            }
            let manager = FleetApplicationManager::new(&state)
                .map_err(|error| registry::Error::msg(error.to_string()))?;
            let result = match command.name.as_str() {
                "start" => manager.start(&id),
                "stop" => manager.stop(&id),
                "restart" => manager.restart(&id),
                _ => unreachable!("validated Link command"),
            };
            result.map_err(|error| registry::Error::msg(error.to_string()))
        },
    )
}

fn publish_if_linked(state: &AppState) -> registry::Result<()> {
    let Some(segment) =
        TransportSegment::existing_for_fleet(state.fleet_name())
            .map_err(|error| registry::Error::msg(error.to_string()))?
    else {
        return Ok(());
    };
    publish_application_snapshot(state, &segment, state.name())
}

/// Report the real local application snapshot through this runner's Link lane.
pub(crate) fn publish_application_snapshot(
    state: &AppState,
    segment: &TransportSegment,
    link_name: &str,
) -> registry::Result<()> {
    let Some(rows) = link_snapshot_rows(state).map_err(registry::Error::msg)?
    else {
        return Ok(());
    };
    segment
        .publish_application_state(link_name, &rows)
        .map_err(|error| registry::Error::msg(error.to_string()))?;
    Ok(())
}
