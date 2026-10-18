//! Who is on the other side of a transport segment, read from its lane
//! directory.
//!
//! A connect or disconnect is not carried anywhere. The lane directory is
//! current state, kept in the inbox table, and every process that wants to
//! know derives the change itself: a worker from the lane it already reads, a
//! master from a directory watcher. The wake only says when to look again;
//! [`Peers::update`] turns two looks into what came and what went. Nothing is
//! published to the fleet for it, so every observing process sees a change
//! once, and nobody has to be chosen to announce it.

use claviron_runtime::FleetName;
use claviron_runtime::events::Event;

use super::Inbox;

/// One claimed lane, as the directory names it.
///
/// The whole identity is the key. A process that restarts usually takes the
/// same lane back under the same name, and its release and claim can land
/// between two looks as one directory generation; only the incarnation and
/// pid tell that apart, so only a comparison of all of it turns that into a
/// disconnect and a connect.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Peer {
    pub fleet: FleetName,
    /// The name the lane serves; an edge's lane carries its instance name.
    pub name: String,
    /// What holds the lane: `edge`, `php`, `asgi`.
    pub role: String,
    pub lane: usize,
    pub incarnation: u64,
    pub pid: Option<u32>,
}

/// The peers last seen, and the difference to the next look.
pub struct Peers {
    fleet: FleetName,
    wanted: fn(&str) -> bool,
    seen: Vec<Peer>,
}

impl Peers {
    /// Peers whose role `wanted` accepts, none seen yet: the first
    /// [`Self::update`] reports every one already there as come.
    pub fn new(
        fleet: FleetName,
        wanted: fn(&str) -> bool,
    ) -> Self {
        Self { fleet, wanted, seen: Vec::new() }
    }

    /// Read the directory and return who came and who went since the last
    /// look.
    pub fn update(
        &mut self,
        inbox: &Inbox,
    ) -> (Vec<Peer>, Vec<Peer>) {
        let now = self.read(inbox);
        let came = now
            .iter()
            .filter(|peer| !self.seen.contains(peer))
            .cloned()
            .collect();
        let went = self
            .seen
            .iter()
            .filter(|peer| !now.contains(peer))
            .cloned()
            .collect();
        self.seen = now;
        (came, went)
    }

    /// Whether any lane seen at the last look serves `name`.
    pub fn serves(
        &self,
        name: &str,
    ) -> bool {
        self.seen.iter().any(|peer| peer.name == name)
    }

    fn read(
        &self,
        inbox: &Inbox,
    ) -> Vec<Peer> {
        (0..inbox.lanes())
            .filter_map(|lane| {
                if inbox.consumers(lane).ok()? == 0 {
                    return None;
                }
                let role = inbox.role(lane).ok()??;
                if !(self.wanted)(&role) {
                    return None;
                }
                Some(Peer {
                    fleet: self.fleet,
                    name: inbox.service(lane).ok()??,
                    role,
                    lane,
                    incarnation: inbox.incarnation(lane).ok()?,
                    pid: inbox.owner(lane).ok()?,
                })
            })
            .collect()
    }
}

/// An edge took a lane in this application's fleet: it can now write
/// exchanges to this application. Process-local; never published.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeConnected {
    pub edge: Peer,
    /// The application this process serves.
    pub application: String,
}

impl Event for EdgeConnected {}

/// An edge's lane left this application's fleet: released on a clean stop,
/// or taken back from a process that is gone. Process-local; never
/// published.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeDisconnected {
    pub edge: Peer,
    /// The application this process serves.
    pub application: String,
    /// No lane serving this edge's name is left: the edge, not one of its
    /// workers, is gone.
    pub last: bool,
}

impl Event for EdgeDisconnected {}

/// An application process took a lane in a fleet this edge reaches: its name
/// is now a target. Process-local; never published.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationConnected {
    pub application: Peer,
    /// The edge this process is: its instance name, which its lane carries.
    pub edge: String,
}

impl Event for ApplicationConnected {}

/// An application process's lane left a fleet this edge reaches: released on
/// a clean stop, or taken back from a process that is gone. Process-local;
/// never published.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationDisconnected {
    pub application: Peer,
    /// The edge this process is: its instance name, which its lane carries.
    pub edge: String,
    /// No lane serving this application's name is left: the application,
    /// not one of its workers, is gone.
    pub last: bool,
}

impl Event for ApplicationDisconnected {}

/// This process took a lane in a link segment and bound its bridge: it can
/// now be reached, and what it publishes crosses. Process-local; never
/// published. It is a transport-readiness observation, not a request for
/// fleet-wide introductions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkJoined {
    pub fleet: FleetName,
    pub lane: usize,
    /// What this process's lane says it is: `edge`, `php`, `asgi`.
    pub role: String,
}

impl Event for LinkJoined {}

#[cfg(test)]
#[path = "../../tests/transport/peer.rs"]
mod tests;
