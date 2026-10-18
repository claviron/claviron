use std::io;

use claviron_runtime::AppState;
use claviron_sys::process::ProcessLock;

/// Server-start lifetime, not a provider/runnable generation. Keep the guard
/// across reload, worker rotation, runnable drain and provider finalization.
pub(super) fn acquire(state: &AppState) -> super::Result<Option<ProcessLock>> {
    if state.is_worker() {
        return Ok(None);
    }

    ProcessLock::try_acquire(&format!("claviron.server:{}", state.name()))
        .map(Some)
        .map_err(|error| {
            if error.kind() == io::ErrorKind::WouldBlock {
                super::Error::InstanceInUse { name: state.name().to_owned() }
            } else {
                super::Error::Io(io::Error::new(
                    error.kind(),
                    format!(
                        "cannot acquire server instance lock for {:?}: {error}",
                        state.name()
                    ),
                ))
            }
        })
}
