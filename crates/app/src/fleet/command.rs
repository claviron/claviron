use claviron_orbit::{EVENT_PAYLOAD_MAX, FleetEventCodec, FleetEventRegistry};
use claviron_runtime::events::Event;
use claviron_runtime::{AppState, registry};

use crate::{
    ApplicationId, ApplicationManager, ApplicationManagerError,
    ApplicationName, ApplicationRuntime,
};

const APPLICATION_COMMAND_TOPIC: &str = "claviron.applications.command.v1";
const ORBIT_EVENT_HEADER_BYTES: usize = 12;
const APPLICATION_COMMAND_HEADER_BYTES: usize = 3;
const MAX_ID_BYTES: usize = EVENT_PAYLOAD_MAX
    - ORBIT_EVENT_HEADER_BYTES
    - APPLICATION_COMMAND_TOPIC.len()
    - APPLICATION_COMMAND_HEADER_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ApplicationCommandAction {
    Start,
    Stop,
    Restart,
}

impl ApplicationCommandAction {
    const fn code(self) -> u8 {
        match self {
            Self::Start => 1,
            Self::Stop => 2,
            Self::Restart => 3,
        }
    }

    fn from_code(code: u8) -> Result<Self, String> {
        match code {
            1 => Ok(Self::Start),
            2 => Ok(Self::Stop),
            3 => Ok(Self::Restart),
            _ => Err(format!("unknown application command action {code}")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ApplicationFleetCommand {
    pub(super) action: ApplicationCommandAction,
    pub(super) id: ApplicationId,
}

pub(super) fn command(
    action: ApplicationCommandAction,
    id: ApplicationId,
) -> Result<ApplicationFleetCommand, String> {
    let runtime_len = id.runtime().as_str().len();
    let identity_len = runtime_len + id.name().as_str().len();
    if runtime_len > usize::from(u16::MAX) || identity_len > MAX_ID_BYTES {
        return Err(format!(
            "application command identity is too long: {identity_len} bytes, max {MAX_ID_BYTES}"
        ));
    }
    Ok(ApplicationFleetCommand { action, id })
}

impl Event for ApplicationFleetCommand {}

impl FleetEventCodec for ApplicationFleetCommand {
    const TOPIC: &'static str = APPLICATION_COMMAND_TOPIC;

    fn encode_orbit(&self) -> Vec<u8> {
        let runtime = self.id.runtime().as_str().as_bytes();
        let name = self.id.name().as_str().as_bytes();
        let runtime_len = u16::try_from(runtime.len())
            .expect("validated application runtime length fits u16");
        let mut payload = Vec::with_capacity(
            APPLICATION_COMMAND_HEADER_BYTES + runtime.len() + name.len(),
        );
        payload.push(self.action.code());
        payload.extend_from_slice(&runtime_len.to_le_bytes());
        payload.extend_from_slice(runtime);
        payload.extend_from_slice(name);
        payload
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < APPLICATION_COMMAND_HEADER_BYTES {
            return Err("application command payload is too short".to_owned());
        }
        let action = ApplicationCommandAction::from_code(bytes[0])?;
        let runtime_len = usize::from(u16::from_le_bytes([bytes[1], bytes[2]]));
        let id_bytes = &bytes[APPLICATION_COMMAND_HEADER_BYTES..];
        if id_bytes.len() > MAX_ID_BYTES {
            return Err(format!(
                "application command identity is too long: {} bytes, max {MAX_ID_BYTES}",
                id_bytes.len()
            ));
        }
        if runtime_len == 0 || runtime_len >= id_bytes.len() {
            return Err(
                "application command runtime or name is empty".to_owned()
            );
        }
        let (runtime, name) = id_bytes.split_at(runtime_len);
        let runtime = std::str::from_utf8(runtime).map_err(|error| {
            format!("application command runtime is not UTF-8: {error}")
        })?;
        let name = std::str::from_utf8(name).map_err(|error| {
            format!("application command name is not UTF-8: {error}")
        })?;
        Ok(Self {
            action,
            id: ApplicationId::new(
                ApplicationRuntime::new(runtime)
                    .map_err(|error| error.to_string())?,
                ApplicationName::new(name)
                    .map_err(|error| error.to_string())?,
            ),
        })
    }
}

pub(super) fn register_listener(state: &AppState) -> registry::Result<()> {
    state.events().listen::<ApplicationFleetCommand, _>(
        |state, command| async move {
            if state.is_shutting_down() {
                return Ok(());
            }
            let manager = state
                .registry_ref()
                .resolve::<ApplicationManager>()
                .ok_or_else(|| {
                    registry::Error::msg("ApplicationManager is unavailable")
                })?;
            let result = match command.action {
                ApplicationCommandAction::Start => {
                    manager.start(&command.id).await
                }
                ApplicationCommandAction::Stop => {
                    manager.stop(&command.id).await
                }
                ApplicationCommandAction::Restart => {
                    manager.restart(&command.id).await
                }
            };
            match result {
                Ok(())
                | Err(ApplicationManagerError::RuntimeNotFound(_))
                | Err(ApplicationManagerError::ApplicationNotFound(_)) => {
                    Ok(())
                }
                Err(error) => Err(registry::Error::msg(error.to_string())),
            }
        },
    )
}

pub(super) fn register_bridge(state: &AppState) -> registry::Result<()> {
    let events =
        state.registry_ref().resolve::<FleetEventRegistry>().ok_or_else(
            || registry::Error::msg("FleetEventRegistry is unavailable"),
        )?;
    events.register_bridge::<ApplicationFleetCommand>(state)
}

#[cfg(test)]
#[path = "../../tests/fleet/command.rs"]
mod tests;
