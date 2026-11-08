//! Explicit descriptor ownership across worker re-exec. No transport policy.

use std::collections::BTreeMap;
use std::io;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::{AppState, registry};

/// A resource whose owner retains the original descriptor and its lifetime.
/// Preparing a child duplicates the descriptor; it never transfers ownership
/// of the original. Implementations may prepare lazily before the first spawn.
pub trait SharedDescriptor: std::any::Any + Send + Sync + 'static {
    fn duplicate(&self) -> io::Result<OwnedFd>;
}

#[derive(Default)]
pub struct ProcessResources {
    shared: Mutex<BTreeMap<Box<str>, Arc<dyn SharedDescriptor>>>,
    sealed: AtomicBool,
    inherited: Mutex<BTreeMap<Box<str>, OwnedFd>>,
}

impl ProcessResources {
    pub fn register(state: &AppState) -> registry::Result<Arc<Self>> {
        if let Some(resources) = state.registry_ref().resolve::<Self>() {
            return Ok(resources);
        }
        let resources = Arc::new(Self::default());
        state.registry_ref().insert(resources.clone())?;
        Ok(resources)
    }

    pub fn share(
        &self,
        name: impl Into<Box<str>>,
        resource: Arc<dyn SharedDescriptor>,
    ) -> io::Result<()> {
        let name = name.into();
        let mut shared = self
            .shared
            .lock()
            .map_err(|_| io::Error::other("process resource lock poisoned"))?;
        if shared.contains_key(name.as_ref()) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("process resource '{name}' already registered"),
            ));
        }
        if self.sealed.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "shared descriptors are sealed for worker launch; restart the process family to add one",
            ));
        }
        shared.insert(name, resource);
        Ok(())
    }

    pub fn shared(
        &self,
        name: &str,
    ) -> Option<Arc<dyn SharedDescriptor>> {
        self.shared.lock().expect("process resources").get(name).cloned()
    }

    /// Retire this owner's retained copy; active generations keep theirs.
    pub fn unshare(
        &self,
        name: &str,
    ) {
        self.shared.lock().expect("process resources").remove(name);
        self.inherited.lock().expect("inherited resources").remove(name);
    }

    /// Called by the process owner after configuration, before child spawn.
    pub fn for_child(&self) -> io::Result<ChildDescriptors> {
        let shared = self
            .shared
            .lock()
            .map_err(|_| io::Error::other("process resource lock poisoned"))?;
        self.sealed.store(true, Ordering::Release);
        let descriptors = shared
            .iter()
            .map(|(name, resource)| Ok((name.clone(), resource.duplicate()?)))
            .collect::<io::Result<_>>()?;
        Ok(ChildDescriptors { descriptors })
    }

    pub fn inherit(
        &self,
        descriptors: Vec<(Box<str>, OwnedFd)>,
    ) -> io::Result<()> {
        let mut inherited = self.inherited.lock().map_err(|_| {
            io::Error::other("inherited resource lock poisoned")
        })?;
        let mut names = std::collections::BTreeSet::new();
        for (name, _) in &descriptors {
            if inherited.contains_key(name.as_ref())
                || !names.insert(name.as_ref())
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "duplicate inherited resource",
                ));
            }
        }
        inherited.extend(descriptors);
        Ok(())
    }

    /// Each serving generation owns its duplicate. Closing it never closes
    /// the retained descriptor or a sibling generation's descriptor.
    pub fn inherited(
        &self,
        name: &str,
    ) -> io::Result<OwnedFd> {
        let inherited = self.inherited.lock().map_err(|_| {
            io::Error::other("inherited resource lock poisoned")
        })?;
        inherited
            .get(name)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("inherited resource '{name}' is missing"),
                )
            })?
            .as_fd()
            .try_clone_to_owned()
    }
}

/// Spawn-only duplicates, kept alive until `Command::spawn` returns.
pub struct ChildDescriptors {
    descriptors: Vec<(Box<str>, OwnedFd)>,
}

impl ChildDescriptors {
    pub fn manifest(&self) -> Vec<(Box<str>, i32)> {
        self.descriptors
            .iter()
            .map(|(name, fd)| (name.clone(), fd.as_raw_fd()))
            .collect()
    }

    /// Only these descriptors survive exec. Originals remain CLOEXEC in the
    /// parent; the child performs only async-signal-safe fcntl operations.
    pub fn attach(
        self,
        command: &mut Command,
    ) {
        use std::os::unix::process::CommandExt;
        if self.descriptors.is_empty() {
            return;
        }
        // SAFETY: the callback allocates nothing and touches only owned FDs.
        unsafe {
            command.pre_exec(move || {
                for (_, fd) in &self.descriptors {
                    let raw = fd.as_raw_fd();
                    let flags = libc::fcntl(raw, libc::F_GETFD);
                    if flags < 0
                        || libc::fcntl(
                            raw,
                            libc::F_SETFD,
                            flags & !libc::FD_CLOEXEC,
                        ) < 0
                    {
                        return Err(io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
    }
}

#[cfg(test)]
#[path = "../tests/process.rs"]
mod tests;
