//! The process owner binds once; worker generations borrow duplicate FDs.

use std::any::Any;
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use claviron_http::config::{ListenerConfig, ListenerSettings, UdsConfig};
use claviron_http::listener::ListenAddress;
use claviron_runtime::process::{ProcessResources, SharedDescriptor};
use claviron_runtime::{AppState, RunMode, registry};

struct SharedUnix {
    path: PathBuf,
    settings: UdsConfig,
    socket: Mutex<Option<UnixListener>>,
}

impl SharedDescriptor for SharedUnix {
    fn duplicate(&self) -> io::Result<OwnedFd> {
        let mut socket = self
            .socket
            .lock()
            .map_err(|_| io::Error::other("shared Unix socket poisoned"))?;
        if socket.is_none() {
            *socket = Some(super::bind(&self.path, &self.settings)?);
        }
        socket.as_ref().expect("bound Unix socket").as_fd().try_clone_to_owned()
    }
}

fn key(
    listener: &claviron_http::ListenerId,
    index: usize,
) -> Box<str> {
    format!("Listener:{listener}:unix:{index}").into()
}

/// Retire retained copies when a declaration is removed. A serving generation
/// retains its own FD until its accepted work drains; no path is unlinked here.
pub fn retire(
    state: &AppState,
    config: &ListenerConfig,
) {
    let Some(resources) = state.registry_ref().resolve::<ProcessResources>()
    else {
        return;
    };
    for (index, address) in config.listen.iter().enumerate() {
        if matches!(address, ListenAddress::Unix(_)) {
            resources.unshare(&key(&config.id, index));
        }
    }
}

/// Declare resources without binding. Check-config never opens sockets.
/// The supervisor duplicates them just before starting a child process.
pub fn prepare(
    state: &AppState,
    config: &ListenerConfig,
) -> registry::Result<()> {
    if !state.is_master()
        || !config
            .listen
            .iter()
            .any(|address| matches!(address, ListenAddress::Unix(_)))
    {
        return Ok(());
    }
    let ListenerSettings::Tcp(settings) = &config.settings else {
        return Ok(());
    };
    let resources = ProcessResources::register(state)?;
    for (index, address) in config.listen.iter().enumerate() {
        let ListenAddress::Unix(path) = address else { continue };
        let name = key(&config.id, index);
        if let Some(previous) = resources.shared(&name) {
            let previous: &dyn Any = previous.as_ref();
            let Some(previous) = previous.downcast_ref::<SharedUnix>() else {
                return Err(registry::Error::msg(
                    "listener descriptor has another owner",
                ));
            };
            if previous.path != *path || previous.settings != settings.uds {
                return Err(registry::Error::msg(format!(
                    "listener '{}': changing a shared Unix bind requires a process-family restart",
                    config.id
                )));
            }
            continue;
        }
        resources.share(
            name,
            Arc::new(SharedUnix {
                path: path.clone(),
                settings: settings.uds.clone(),
                socket: Mutex::new(None),
            }),
        )?;
    }
    Ok(())
}

pub(super) fn acquire(
    state: &AppState,
    config: &ListenerConfig,
    index: usize,
    path: &std::path::Path,
    settings: &UdsConfig,
) -> io::Result<(tokio::net::UnixListener, Option<super::SocketFile>)> {
    match state.run_mode() {
        RunMode::Master => {
            Err(io::Error::other("master does not accept connections"))
        }
        RunMode::Worker(_) => {
            let resources = state
                .registry_ref()
                .resolve::<ProcessResources>()
                .ok_or_else(|| {
                    io::Error::other(
                        "worker has no inherited listener descriptors",
                    )
                })?;
            let socket = UnixListener::from(
                resources.inherited(&key(&config.id, index))?,
            );
            if socket.local_addr()?.as_pathname() != Some(path) {
                return Err(io::Error::other(
                    "inherited Unix listener does not match its declaration",
                ));
            }
            Ok((tokio::net::UnixListener::from_std(socket)?, None))
        }
        RunMode::Standalone => {
            let socket = super::bind(path, settings)?;
            let owned = super::SocketFile::owned(path.to_path_buf())?;
            Ok((tokio::net::UnixListener::from_std(socket)?, Some(owned)))
        }
    }
}
