//! Exact child ownership; the production supervisor owns each worker family.
use super::{
    Result,
    config::Launch,
    wire::{Channel, Command, Identity, Reply},
};
use claviron::sys::process::{ProcessHandle, identity};
use std::process::{Child, Command as Process, Stdio};
use tokio::net::UnixListener;

pub struct Peer {
    pub identity: Identity,
    pub channel: Channel,
}
pub struct Family {
    pub launch: Launch,
    pub child: Child,
    pub process: ProcessHandle,
    pub peers: Vec<Peer>,
    joined: bool,
}
impl Family {
    pub fn spawn(launch: Launch) -> Result<Self> {
        let mut child = Process::new(std::env::current_exe()?)
            .arg("--harness-config")
            .arg(&launch.directory)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;
        let opened = identity(child.id())
            .ok_or_else(|| {
                std::io::Error::other("master exited before identity capture")
            })
            .and_then(ProcessHandle::open);
        match opened {
            Ok(process) => Ok(Self {
                launch,
                child,
                process,
                peers: Vec::new(),
                joined: false,
            }),
            Err(error) => {
                // The unreaped Child still reserves this PID, even if opening a
                // platform exit notifier failed. Ask its master to drain.
                if child.try_wait()?.is_none() {
                    // SAFETY: this is the exact unreaped child just spawned above.
                    unsafe {
                        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
                    }
                }
                let _ = child.wait();
                Err(error.into())
            }
        }
    }
    pub async fn ready(
        &mut self,
        listener: &UnixListener,
        workers: u8,
    ) -> Result<()> {
        while self.peers.len() < usize::from(workers) + 1 {
            let (stream, _) = tokio::select! {
                accepted = listener.accept() => accepted?,
                exited = self.process.wait() => { exited?; return Err(format!("{:?} master exited during startup", self.launch.side).into()); }
            };
            let mut channel = Channel::new(stream);
            let reply = tokio::select! {
                reply = channel.receive::<Reply>() => reply?,
                exited = self.process.wait() => { exited?; return Err("master exited before readiness".into()); }
            };
            let Reply::Ready(info) = reply else {
                return Err("missing readiness identity".into());
            };
            if info.side != self.launch.side
                || info.master_pid != self.child.id()
                || info.fleet != self.launch.fleet
            {
                return Err(
                    "readiness belongs to another process family".into()
                );
            }
            if info.worker.is_some_and(|worker| worker == 0 || worker > workers)
                || (info.worker.is_none() && info.pid != self.child.id())
                || self.peers.iter().any(|peer| {
                    peer.identity.worker == info.worker
                        || peer.identity.pid == info.pid
                })
            {
                return Err(
                    "duplicate/replaced worker or invalid supervisor identity"
                        .into(),
                );
            }
            self.peers.push(Peer { identity: info, channel });
        }
        self.peers.sort_by_key(|peer| peer.identity.worker);
        Ok(())
    }
    pub async fn shutdown(&mut self) -> Result<()> {
        if self.joined {
            return Ok(());
        }
        if self.child.try_wait()?.is_none() {
            let acknowledged = if let Some(master) = self
                .peers
                .iter_mut()
                .find(|peer| peer.identity.worker.is_none())
            {
                master.channel.ack(&Command::Shutdown).await.is_ok()
            } else {
                false
            };
            if !acknowledged {
                match self.process.signal(libc::SIGTERM) {
                    Ok(()) => {}
                    Err(error)
                        if error.raw_os_error() == Some(libc::ESRCH)
                            || error.kind() == std::io::ErrorKind::NotFound => {
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            self.process.wait().await?;
        }
        let status = self.child.wait()?;
        self.joined = true;
        if !status.success() {
            return Err(format!(
                "{:?} master exited with {status}",
                self.launch.side
            )
            .into());
        }
        Ok(())
    }
}
impl Drop for Family {
    fn drop(&mut self) {
        if !self.joined {
            // Only this exact owned child is signalled; the production master
            // owns worker drain. No timeout or SIGKILL is invented here.
            let _ = self.process.signal(libc::SIGTERM);
            let _ = self.child.wait();
        }
    }
}

pub async fn broadcast(
    families: &mut [Family],
    command: &Command,
) -> Result<Vec<(Identity, Reply)>> {
    futures_util::future::try_join_all(
        families.iter_mut().flat_map(|family| family.peers.iter_mut()).map(
            |peer| async move {
                Ok((peer.identity.clone(), peer.channel.ask(command).await?))
            },
        ),
    )
    .await
}

pub fn require_ack(replies: Vec<(Identity, Reply)>) -> Result<()> {
    for (_, reply) in replies {
        if !matches!(reply, Reply::Ack) {
            return Err("unexpected phase acknowledgement".into());
        }
    }
    Ok(())
}
