//! The caller's lifecycle: one [`ApplicationClient`] per destination fleet this
//! worker reaches, and the provider that owns them.
//!
//! Applications in the same destination fleet share one client, lane and reader.
//! Different destination fleets have independent clients. Each
//! is joined by the first request that needs it: an application nobody routes
//! to is never mapped, and a host with no application route maps nothing.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use claviron_runtime::{
    AppState, Bootable, Component, Finalizable, FleetIncarnation, FleetName,
    ProcessEventBus, registry,
};

use super::client::ApplicationClient;
use super::{
    Access, ApplicationLinkSetup, ApplicationSegment, ControlCommand,
    ControlTarget, EDGE_RUNTIME, Error, Peer, Peers, Result,
};

/// One live or configured Link target. Link reachability says nothing about
/// the owner process outside the Link directory.
#[derive(Clone, Debug)]
pub struct LinkApplication {
    pub fleet: FleetName,
    pub name: String,
    pub runtime: Option<String>,
    pub peers: Vec<Peer>,
    /// Complete application snapshot rows reported by connected runner lanes.
    pub reports: Vec<Vec<Vec<u8>>>,
}

/// Reaches applications by name over shared memory, for as long as this
/// worker lives.
#[derive(Default)]
pub struct ApplicationService {
    worker: bool,
    /// What this worker's own lane advertises. Read from the instance at
    /// boot, because a lane an operator reads should say which process it
    /// belongs to.
    identity: Mutex<String>,
    incarnation: AtomicU64,
    /// Where application processes coming and going are reported: this
    /// process's local bus, taken at boot.
    events: Mutex<Option<ProcessEventBus>>,
    /// How application events cross each fleet this worker reaches, when
    /// this process has an application ring.
    link: Mutex<Option<ApplicationLinkSetup>>,
    clients: Mutex<HashMap<FleetName, Arc<ApplicationClient>>>,
    targets: Mutex<BTreeSet<(FleetName, String)>>,
    /// Route destinations stated as waiting for their owner, until the first
    /// join of their fleet adopts them. One process builds a route once per
    /// listener pipeline; the destination is stated once.
    waiting: Mutex<BTreeSet<(FleetName, Arc<str>)>>,
    /// Explicit access only for selected destination Links, never the host fleet.
    access: Mutex<BTreeMap<FleetName, Access>>,
}

impl ApplicationService {
    pub(crate) fn for_state(state: &AppState) -> Self {
        Self { worker: state.is_worker(), ..Self::default() }
    }

    /// Register immutable access before this destination's first client opens.
    /// This maps no SHM and changes no credentials or resource permissions.
    pub fn register_access(
        &self,
        fleet: FleetName,
        access: Access,
    ) -> Result<()> {
        // Match client() lock order, so registering cannot race a default join.
        let clients = self
            .clients
            .lock()
            .map_err(|_| Error::Malformed("application client lock"))?;
        if clients.contains_key(&fleet) {
            return Err(Error::Malformed(
                "Link access must be registered before its client opens",
            ));
        }
        let mut policies = self
            .access
            .lock()
            .map_err(|_| Error::Malformed("link access lock"))?;
        if policies.get(&fleet).is_some_and(|existing| existing != &access) {
            return Err(Error::Malformed(
                "Link access is already registered differently",
            ));
        }
        policies.insert(fleet, access);
        Ok(())
    }

    fn access_for(
        &self,
        fleet: FleetName,
    ) -> Result<Access> {
        Ok(self
            .access
            .lock()
            .map_err(|_| Error::Malformed("link access lock"))?
            .get(&fleet)
            .cloned()
            .unwrap_or_default())
    }
    /// The supervisor calls this only after observing the exact process exit.
    /// Management maps existing destinations without joining a caller lane.
    pub fn process_exited(
        &self,
        own_fleet: &str,
        pid: u32,
        incarnation: u64,
    ) {
        let mut fleets = BTreeSet::from([own_fleet.to_owned()]);
        fleets.extend(
            self.targets
                .lock()
                .expect("application target lock")
                .iter()
                .map(|(fleet, _)| fleet.as_str().to_owned()),
        );
        fleets.extend(
            self.clients
                .lock()
                .expect("application client lock")
                .keys()
                .map(|fleet| fleet.as_str().to_owned()),
        );
        for fleet in fleets {
            let result = self
                .access_for(FleetName::new(&fleet))
                .and_then(|access| ApplicationSegment::adopt(&fleet, &access))
                .and_then(|segment| match segment {
                    Some(segment) => segment.process_exited(pid, incarnation),
                    None => Ok(()),
                });
            if let Err(error) = result {
                tracing::error!(%fleet, pid, incarnation, %error, "could not release exited process Link streams");
            }
        }
    }

    /// Record an edge route so its Link target remains visible while no lane
    /// is connected. This is routing identity, not process ownership.
    pub fn track_target(
        &self,
        fleet: FleetName,
        application: &str,
    ) {
        self.targets
            .lock()
            .expect("application target lock")
            .insert((fleet, application.to_owned()));
    }

    /// State a missing destination once per serving process.
    pub fn destination_absent(
        &self,
        fleet: FleetName,
        application: &Arc<str>,
    ) {
        let first = self
            .waiting
            .lock()
            .expect("application waiting lock")
            .insert((fleet, Arc::clone(application)));
        if first {
            log_destination_absent(self.worker, fleet, application);
        }
    }

    /// State configured names missing from one fleet at master startup.
    /// Read the current directory without joining a lane or keeping a watch.
    pub fn state_absent_destinations<'a>(
        &self,
        fleet: FleetName,
        applications: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        let peers = ApplicationSegment::adopt(
            fleet.as_str(),
            &self.access_for(fleet)?,
        )?
        .map(|segment| {
            Peers::new(fleet, |role| role != EDGE_RUNTIME).read(segment.inbox())
        })
        .unwrap_or_default();
        for application in applications {
            if !peers.iter().any(|peer| peer.name == application) {
                log_destination_absent(false, fleet, application);
            }
        }
        Ok(())
    }

    /// Read the current Link directories of every destination fleet this
    /// worker has joined, including applications without an edge route.
    pub fn link_applications(&self) -> Result<Vec<LinkApplication>> {
        let targets = self
            .targets
            .lock()
            .map_err(|_| Error::Malformed("application target lock"))?
            .clone();
        for fleet in
            targets.iter().map(|(fleet, _)| *fleet).collect::<BTreeSet<_>>()
        {
            let joined = self
                .clients
                .lock()
                .map_err(|_| Error::Malformed("application client lock"))?
                .contains_key(&fleet);
            if !joined
                && ApplicationSegment::adopt(
                    fleet.as_str(),
                    &self.access_for(fleet)?,
                )?
                .is_some()
            {
                self.client(fleet)?;
            }
        }
        let clients: Vec<_> = self
            .clients
            .lock()
            .map_err(|_| Error::Malformed("application client lock"))?
            .iter()
            .map(|(fleet, client)| (*fleet, Arc::clone(client)))
            .collect();
        let mut found: BTreeMap<(FleetName, String), LinkApplication> = targets
            .iter()
            .map(|(fleet, name)| {
                (
                    (*fleet, name.clone()),
                    LinkApplication {
                        fleet: *fleet,
                        name: name.clone(),
                        runtime: None,
                        peers: Vec::new(),
                        reports: Vec::new(),
                    },
                )
            })
            .collect();
        for (fleet, client) in clients {
            for (name, runtime) in client.known_applications() {
                let entry =
                    found.entry((fleet, name.clone())).or_insert_with(|| {
                        LinkApplication {
                            fleet,
                            name,
                            runtime: None,
                            peers: Vec::new(),
                            reports: Vec::new(),
                        }
                    });
                entry.runtime = Some(runtime);
            }
            for peer in client.applications() {
                let entry = found
                    .entry((fleet, peer.name.clone()))
                    .or_insert_with(|| LinkApplication {
                        fleet,
                        name: peer.name.clone(),
                        runtime: None,
                        peers: Vec::new(),
                        reports: Vec::new(),
                    });
                if let Some(report) = client.application_state(&peer) {
                    entry.reports.push(report);
                }
                entry.runtime = Some(peer.role.clone());
                entry.peers.push(peer);
            }
        }
        Ok(found.into_values().collect())
    }

    /// A successful return means inbox acceptance, not handler execution.
    pub fn forward_command(
        &self,
        fleet: FleetName,
        application: &str,
        name: &str,
        payload: Vec<u8>,
        target: Option<ControlTarget>,
    ) -> Result<()> {
        if name.is_empty()
            || !name.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(byte, b'-' | b'_' | b'.')
            })
        {
            return Err(Error::Malformed("invalid Link command name"));
        }
        let client = self.client(fleet)?;
        client.command(&ControlCommand {
            application: application.to_owned(),
            name: name.to_owned(),
            payload,
            target,
            origin: None,
        })
    }

    /// The client for `fleet_name`, joining it if this is the first
    /// request for it.
    ///
    /// The lock is held for a lookup, and for the join the first time: every
    /// caller takes a clone of the `Arc` and releases it before anything is
    /// sent.
    pub fn client(
        &self,
        fleet_name: FleetName,
    ) -> Result<Arc<ApplicationClient>> {
        let mut held = self
            .clients
            .lock()
            .map_err(|_| Error::Malformed("application client lock"))?;
        if let Some(client) = held.get(&fleet_name) {
            return Ok(Arc::clone(client));
        }
        let identity = self
            .identity
            .lock()
            .map_err(|_| Error::Malformed("application identity"))?;
        let events = self
            .events
            .lock()
            .map_err(|_| Error::Malformed("application events"))?
            .clone();
        let link = self
            .link
            .lock()
            .map_err(|_| Error::Malformed("application link"))?
            .clone();
        // The destination's owner created it and declared its shape; a
        // caller adopts both and never creates a destination of its own.
        let segment = Arc::new(
            ApplicationSegment::adopt(
                fleet_name.as_str(),
                &self.access_for(fleet_name)?,
            )?
            .ok_or_else(|| Error::DestinationAbsent(fleet_name.to_string()))?,
        );
        let client = ApplicationClient::join_segment(
            segment,
            &identity,
            self.incarnation.load(Ordering::Acquire),
            events,
            link,
        )?;
        drop(identity);
        tracing::debug!(
            fleet = %fleet_name,
            lane = client.lane(),
            "🛰 edge caller lane joined Link fleet"
        );
        held.insert(fleet_name, Arc::clone(&client));
        // The destinations stated as waiting on this fleet have their owner.
        self.waiting.lock().expect("application waiting lock").retain(
            |(fleet, application)| {
                if *fleet != fleet_name {
                    return true;
                }
                tracing::debug!(
                    fleet_name = %fleet,
                    application = %application,
                    "the Link route's destination has its owner; adopted"
                );
                false
            },
        );
        Ok(client)
    }
}

fn log_destination_absent(
    worker: bool,
    fleet: FleetName,
    application: &str,
) {
    const MESSAGE: &str = "the Link route's destination has no owner yet; it is adopted once the owner creates it";
    if worker {
        tracing::debug!(fleet_name = %fleet, %application, "{MESSAGE}");
    } else {
        tracing::info!(fleet_name = %fleet, %application, "{MESSAGE}");
    }
}

impl Component<AppState> for ApplicationService {
    type Deps = ();

    fn name(&self) -> &'static str {
        "ApplicationService"
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<AppState>> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl Bootable<AppState> for ApplicationService {
    /// Learn who this process is, without joining anything.
    ///
    /// The join waits for a request that needs it; what cannot wait is the
    /// identity, because the incarnation is a fact the master hands this
    /// process and the instance name is boot geometry.
    async fn boot(
        &self,
        _deps: (),
        state: &AppState,
    ) -> registry::Result<()> {
        *self
            .identity
            .lock()
            .map_err(|_| registry::Error::msg("application identity"))? =
            state.name().to_owned();
        let incarnation = state
            .registry_ref()
            .resolve::<FleetIncarnation>()
            .map_or(FleetIncarnation::FIRST, |held| *held)
            .raw();
        self.incarnation.store(incarnation, Ordering::Release);
        *self
            .events
            .lock()
            .map_err(|_| registry::Error::msg("application events"))? =
            Some(state.events().bus().clone());
        *self
            .link
            .lock()
            .map_err(|_| registry::Error::msg("application link"))? =
            ApplicationLinkSetup::resolve(state);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Finalizable<AppState> for ApplicationService {
    /// Give up every lane after the runnables have drained, so an exchange
    /// in flight still has somewhere for its reply to land.
    async fn finalize(
        &self,
        _state: &AppState,
    ) -> registry::Result<()> {
        let clients = self
            .clients
            .lock()
            .map(|mut held| std::mem::take(&mut *held))
            .unwrap_or_default();
        for client in clients.into_values() {
            client.leave();
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/link/application/service.rs"]
mod tests;
