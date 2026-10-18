use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::SecondsFormat;
use claviron_orbit::FleetProvider;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_pusher::PresenceMember;
use claviron_runtime::registry::{self, Component, Runnable, priority};
use claviron_runtime::{AppState, RunContext};

use crate::fleet::{FleetFanout, FleetPublication};
use crate::hooks::{Hooks, OccupancyChange, OccupancyKind};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ChannelIdentity {
    app_id: String,
    channel: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SubscriberIdentity {
    node_id: u16,
    socket_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransitionKind {
    Occupied,
    Vacated,
}

#[derive(Clone, Debug, PartialEq)]
struct Transition {
    kind: TransitionKind,
    identity: ChannelIdentity,
    member: Option<PresenceMember>,
    time: String,
}

#[derive(Default)]
struct Occupancy {
    channels: HashMap<
        ChannelIdentity,
        HashMap<SubscriberIdentity, Option<PresenceMember>>,
    >,
}

impl Occupancy {
    fn joined(
        &mut self,
        node_id: u16,
        app_id: String,
        channel: String,
        socket_id: String,
        member: Option<PresenceMember>,
    ) -> Option<Transition> {
        let identity = ChannelIdentity { app_id, channel };
        let subscribers = self.channels.entry(identity.clone()).or_default();
        let was_empty = subscribers.is_empty();
        if subscribers
            .insert(SubscriberIdentity { node_id, socket_id }, member.clone())
            .is_some()
        {
            return None;
        }
        was_empty.then_some(Transition {
            kind: TransitionKind::Occupied,
            identity,
            member,
            time: iso_time(),
        })
    }

    fn left(
        &mut self,
        node_id: u16,
        app_id: &str,
        channel: &str,
        socket_id: &str,
    ) -> Option<Transition> {
        let identity = ChannelIdentity {
            app_id: app_id.to_owned(),
            channel: channel.to_owned(),
        };
        let subscribers = self.channels.get_mut(&identity)?;
        let member = subscribers.remove(&SubscriberIdentity {
            node_id,
            socket_id: socket_id.to_owned(),
        })?;
        if !subscribers.is_empty() {
            return None;
        }
        self.channels.remove(&identity);
        Some(Transition {
            kind: TransitionKind::Vacated,
            identity,
            member,
            time: iso_time(),
        })
    }

    fn reset_node(
        &mut self,
        node_id: u16,
    ) -> Vec<Transition> {
        let identities = self.channels.keys().cloned().collect::<Vec<_>>();
        let mut transitions = Vec::new();
        for identity in identities {
            let Some(subscribers) = self.channels.get_mut(&identity) else {
                continue;
            };
            let removed = subscribers
                .extract_if(|subscriber, _| subscriber.node_id == node_id)
                .map(|(_, member)| member)
                .collect::<Vec<_>>();
            if removed.is_empty() || !subscribers.is_empty() {
                continue;
            }
            self.channels.remove(&identity);
            let member = (removed.len() == 1)
                .then(|| removed.into_iter().next().flatten())
                .flatten();
            transitions.push(Transition {
                kind: TransitionKind::Vacated,
                identity,
                member,
                time: iso_time(),
            });
        }
        transitions
    }
}

pub(crate) struct BroadcastOccupancyService(pub(crate) Hooks);

impl Component<AppState> for BroadcastOccupancyService {
    type Deps = (Arc<FleetProvider>,);

    fn name(&self) -> &'static str {
        "BroadcastOccupancyService"
    }

    fn run_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
}

/// Occupancy is decided once, master-side; a standalone process is its own
/// master.
impl Scoped for BroadcastOccupancyService {
    fn scope(&self) -> Scope {
        Scope::Master
    }
}

#[async_trait]
impl Runnable<AppState, (Arc<FleetProvider>,)> for BroadcastOccupancyService {
    async fn run(
        self: Arc<Self>,
        (fleet,): (Arc<FleetProvider>,),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let fleet = fleet.handle().map(FleetFanout::new).ok_or_else(|| {
            registry::Error::msg("Broadcast occupancy requires an Orbit fleet")
        })?;
        let mut cursor = fleet.cursor_at_head();
        let include_local = state.is_standalone();
        fleet.publish_subscription_reset().map_err(|error| {
            registry::Error::msg(format!(
                "Broadcast occupancy resync failed: {error}"
            ))
        })?;
        let mut occupancy = Occupancy::default();
        deliver(
            &state,
            &fleet,
            &mut cursor,
            &mut occupancy,
            include_local,
            &self.0,
        )
        .await;

        #[cfg(any(target_os = "linux", target_os = "freebsd"))]
        let event_fd = Some(
            fleet
                .event_fd()
                .and_then(|event_fd| {
                    // SAFETY: RingEventFd owns its stable descriptor until drop;
                    // the occupancy loop only drains it and never replaces it.
                    unsafe { tokio::io::unix::AsyncFd::register(event_fd) }
                        .map_err(std::io::Error::from)
                })
                .map_err(|error| {
                    registry::Error::msg(format!(
                        "Broadcast occupancy readiness failed: {error}"
                    ))
                })?,
        );
        #[cfg(target_os = "macos")]
        let event_fd = match fleet.event_fd() {
            Ok(event_fd) => Some(
                // SAFETY: RingEventFd owns its stable descriptor until drop;
                // the occupancy loop only drains it and never replaces it.
                unsafe { tokio::io::unix::AsyncFd::register(event_fd) }
                    .map_err(|error| {
                        registry::Error::msg(format!(
                            "Broadcast occupancy readiness failed: {error}"
                        ))
                    })?,
            ),
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
                None
            }
            Err(error) => {
                return Err(registry::Error::msg(format!(
                    "Broadcast occupancy readiness failed: {error}"
                )));
            }
        };

        #[cfg(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "macos"
        ))]
        if let Some(event_fd) = event_fd {
            loop {
                tokio::select! {
                    biased;
                    _ = context.cancelled() => break,
                    ready = event_fd.readable() => {
                        let mut ready = ready.map_err(|error| registry::Error::msg(format!("Broadcast occupancy readiness failed: {error}")))?;
                        event_fd.get_ref().drain().map_err(|error| registry::Error::msg(format!("Broadcast occupancy readiness drain failed: {error}")))?;
                        ready.clear_ready();
                        deliver(&state, &fleet, &mut cursor, &mut occupancy, include_local, &self.0).await;
                    }
                }
            }
            return Ok(());
        }

        let mut interval = tokio::time::interval(Duration::from_millis(10));
        interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => break,
                _ = interval.tick() => deliver(&state, &fleet, &mut cursor, &mut occupancy, include_local, &self.0).await
            }
        }
        Ok(())
    }
}

async fn deliver(
    state: &AppState,
    fleet: &FleetFanout,
    cursor: &mut orbitive::fleet::FleetLaneCursor,
    occupancy: &mut Occupancy,
    include_local: bool,
    hooks: &Hooks,
) {
    let inbound = if include_local {
        fleet.poll_including_local(cursor)
    } else {
        fleet.poll(cursor)
    };
    for inbound in inbound {
        let transitions = match inbound.publication {
            FleetPublication::SubscriptionJoined {
                app_id,
                channel,
                socket_id,
                member,
            } => occupancy
                .joined(inbound.source_node, app_id, channel, socket_id, member)
                .into_iter()
                .collect(),
            FleetPublication::SubscriptionLeft {
                app_id,
                channel,
                socket_id,
            } => occupancy
                .left(inbound.source_node, &app_id, &channel, &socket_id)
                .into_iter()
                .collect(),
            FleetPublication::SubscriptionReset => {
                occupancy.reset_node(inbound.source_node)
            }
            _ => continue,
        };
        for transition in transitions {
            emit(state, transition, hooks).await;
        }
    }
}

async fn emit(
    state: &AppState,
    transition: Transition,
    hooks: &Hooks,
) {
    let change = change(transition);
    super::event::emit(state, &change);
    hooks.dispatch(change).await;
}

fn change(transition: Transition) -> OccupancyChange {
    OccupancyChange {
        kind: match transition.kind {
            TransitionKind::Occupied => OccupancyKind::Occupied,
            TransitionKind::Vacated => OccupancyKind::Vacated,
        },
        app_id: transition.identity.app_id,
        channel: transition.identity.channel,
        time: transition.time,
        user_id: transition
            .member
            .as_ref()
            .map(|member| member.user_id.clone()),
        user_info: transition.member.map(|member| member.user_info),
    }
}

fn iso_time() -> String {
    chrono::Local::now().to_rfc3339_opts(SecondsFormat::Micros, false)
}

#[cfg(test)]
#[path = "../../../tests/occupancy.rs"]
mod tests;
