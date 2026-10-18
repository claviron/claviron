//! The caller's lifecycle: one [`TransportClient`] per destination fleet this
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

use super::client::TransportClient;
use super::{
    ApplicationLinkSetup, ControlCommand, ControlTarget, Error, Peer, Result,
    TransportSegment,
};

/// One live or configured Link target. Link reachability says nothing about
/// the owner process outside the Link directory.
#[derive(Clone, Debug)]
pub struct LinkedApplication {
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
pub struct TransportService {
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
    clients: Mutex<HashMap<FleetName, Arc<TransportClient>>>,
    targets: Mutex<BTreeSet<(FleetName, String)>>,
}

impl TransportService {
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
                .expect("transport target lock")
                .iter()
                .map(|(fleet, _)| fleet.as_str().to_owned()),
        );
        fleets.extend(
            self.clients
                .lock()
                .expect("transport client lock")
                .keys()
                .map(|fleet| fleet.as_str().to_owned()),
        );
        for fleet in fleets {
            let result = TransportSegment::existing_for_fleet(&fleet).and_then(
                |segment| match segment {
                    Some(segment) => segment.process_exited(pid, incarnation),
                    None => Ok(()),
                },
            );
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
            .expect("transport target lock")
            .insert((fleet, application.to_owned()));
    }

    /// Read the current Link directories of every destination fleet this
    /// worker has joined, including applications without an edge route.
    pub fn linked_applications(&self) -> Result<Vec<LinkedApplication>> {
        let targets = self
            .targets
            .lock()
            .map_err(|_| Error::Malformed("transport target lock"))?
            .clone();
        for fleet in
            targets.iter().map(|(fleet, _)| *fleet).collect::<BTreeSet<_>>()
        {
            let joined = self
                .clients
                .lock()
                .map_err(|_| Error::Malformed("transport client lock"))?
                .contains_key(&fleet);
            if !joined
                && TransportSegment::existing_for_fleet(fleet.as_str())?
                    .is_some()
            {
                self.client(fleet)?;
            }
        }
        let clients: Vec<_> = self
            .clients
            .lock()
            .map_err(|_| Error::Malformed("transport client lock"))?
            .iter()
            .map(|(fleet, client)| (*fleet, Arc::clone(client)))
            .collect();
        let mut found: BTreeMap<(FleetName, String), LinkedApplication> =
            targets
                .iter()
                .map(|(fleet, name)| {
                    (
                        (*fleet, name.clone()),
                        LinkedApplication {
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
                        LinkedApplication {
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
                    .or_insert_with(|| LinkedApplication {
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
    ) -> Result<Arc<TransportClient>> {
        let mut held = self
            .clients
            .lock()
            .map_err(|_| Error::Malformed("transport client lock"))?;
        if let Some(client) = held.get(&fleet_name) {
            return Ok(Arc::clone(client));
        }
        let identity = self
            .identity
            .lock()
            .map_err(|_| Error::Malformed("transport identity"))?;
        let events = self
            .events
            .lock()
            .map_err(|_| Error::Malformed("transport events"))?
            .clone();
        let link = self
            .link
            .lock()
            .map_err(|_| Error::Malformed("transport link"))?
            .clone();
        let client = TransportClient::join_observed(
            fleet_name.as_str(),
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
        Ok(client)
    }
}

impl Component<AppState> for TransportService {
    type Deps = ();

    fn name(&self) -> &'static str {
        "TransportService"
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<AppState>> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl Bootable<AppState> for TransportService {
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
            .map_err(|_| registry::Error::msg("transport identity"))? =
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
            .map_err(|_| registry::Error::msg("transport events"))? =
            Some(state.events().bus().clone());
        *self
            .link
            .lock()
            .map_err(|_| registry::Error::msg("transport link"))? =
            ApplicationLinkSetup::resolve(state);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Finalizable<AppState> for TransportService {
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
