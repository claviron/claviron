//! Local application observations of the peers this process reaches.
//!
//! The transport already observes the opposite side's lane directory. Turn
//! those observations into guest events here, without a fleet-wide question,
//! reply or publication. Each process sees only its own direct peers; joining
//! another link cannot introduce those peers to unrelated applications.
//!
//! A departure is not announced by the one leaving — it may already be gone.
//! The side that sees it go says so, locally, to its own application: an
//! application process hears `claviron.edge.left.v1`, an edge hears
//! `claviron.application.left.v1`. Gone means the last lane serving that name
//! went: an edge does not know what runs behind an application name, and the
//! application does not know how many workers an edge has, so neither hears
//! about single lanes — those stay in the runtime's own events and logs.
//! Delivered in this process only, never published, so each application
//! instance hears each departure once.

use claviron_orbit::ApplicationEventRegistry;
use claviron_orbit::FleetEventCodec;
use claviron_orbit::transport::{
    ApplicationConnected, ApplicationDisconnected, EdgeConnected,
    EdgeDisconnected, Peer,
};
use claviron_runtime::{AppState, registry};
use tracing::warn;

use crate::{
    APPLICATION_EVENT_TOPIC, ApplicationEvent, EventArrayKey, EventValue,
};

pub const EDGE_CONNECTED_TOPIC: &str = "claviron.edge.connected.v1";
pub const APPLICATION_CONNECTED_TOPIC: &str =
    "claviron.application.connected.v1";
pub const EDGE_LEFT_TOPIC: &str = "claviron.edge.left.v1";
pub const APPLICATION_LEFT_TOPIC: &str = "claviron.application.left.v1";

pub(crate) fn register(state: &AppState) -> registry::Result<()> {
    // Use the existing codec's inbound delivery path so these local notices
    // cannot be republished by the ordinary application-event bridge.
    claviron_orbit::register(state)?;
    if let Some(events) =
        state.registry_ref().resolve::<ApplicationEventRegistry>()
    {
        events.register_bridge::<ApplicationEvent>(state)?;
    }
    state.events().listen::<EdgeConnected, _>(
        |state, connected| async move {
            let mut fields = introduction(&connected.edge);
            fields.push(field("application", text(&connected.application)));
            deliver_here(&state, EDGE_CONNECTED_TOPIC, fields);
            Ok(())
        },
    )?;

    state.events().listen::<ApplicationConnected, _>(
        |state, connected| async move {
            let mut fields = introduction(&connected.application);
            fields.push(field("edge", text(&connected.edge)));
            deliver_here(&state, APPLICATION_CONNECTED_TOPIC, fields);
            Ok(())
        },
    )?;

    state.events().listen::<EdgeDisconnected, _>(|state, left| async move {
        if left.last {
            let mut fields = named(&left.edge);
            fields.push(field("application", text(&left.application)));
            deliver_here(&state, EDGE_LEFT_TOPIC, fields);
        }
        Ok(())
    })?;

    state.events().listen::<ApplicationDisconnected, _>(
        |state, left| async move {
            if left.last {
                let mut fields = named(&left.application);
                fields.push(field("edge", text(&left.edge)));
                deliver_here(&state, APPLICATION_LEFT_TOPIC, fields);
            }
            Ok(())
        },
    )?;
    Ok(())
}

/// Facts the directly observed lane actually supplies, not a remote query.
fn introduction(peer: &Peer) -> Vec<(EventArrayKey, EventValue)> {
    let mut fields = named(peer);
    fields.push(field("role", text(&peer.role)));
    fields.push(field(
        "pid",
        peer.pid
            .map_or(EventValue::Null, |pid| EventValue::Int(i64::from(pid))),
    ));
    fields.push(field("lane", EventValue::Int(peer.lane as i64)));
    fields
}

/// What went, by name only: what serves a name is not this side's business.
fn named(peer: &Peer) -> Vec<(EventArrayKey, EventValue)> {
    vec![
        field("name", text(&peer.name)),
        field("fleet", text(peer.fleet.as_str())),
    ]
}

/// To this process's own application only: never into the ring.
fn deliver_here(
    state: &AppState,
    topic: &str,
    fields: Vec<(EventArrayKey, EventValue)>,
) {
    let event = match ApplicationEvent::new(topic, EventValue::Array(fields)) {
        Ok(event) => event,
        Err(error) => {
            warn!(topic, "link observation rejected: {error}");
            return;
        }
    };
    match state.registry_ref().resolve::<ApplicationEventRegistry>() {
        Some(registry) => {
            if let Err(error) = registry.deliver(
                state.events().bus(),
                APPLICATION_EVENT_TOPIC,
                &event.encode_orbit(),
            ) {
                warn!(
                    topic,
                    "link observation could not be delivered: {error}"
                );
            }
        }
        // No ring: a plain emit already stays in this process.
        None => state.events().emit(event),
    }
}

fn field(
    name: &str,
    value: EventValue,
) -> (EventArrayKey, EventValue) {
    (EventArrayKey::String(name.to_owned()), value)
}

fn text(value: &str) -> EventValue {
    EventValue::String(value.as_bytes().to_vec())
}

#[cfg(test)]
#[path = "../tests/link.rs"]
mod tests;
