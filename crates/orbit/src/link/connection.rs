use std::sync::Arc;

use orbitive::events::{EventLayout, FleetEventBus};
use orbitive::link::{InboxReceiver, Joined};

use super::{
    Access, Error, EventConnection, LinkSegment, LinkSpec, Received, Result,
    Routing,
};

/// One named Link membership and its exclusive inbox reader.
///
/// This opens no HTTP body table or event ring and installs no signal handler
/// or background task. The caller owns protocol dispatch and accepted work.
/// `incarnation` identifies this execution, not a stable service identity.
/// Open inside a Tokio runtime with its IO driver enabled.
pub struct Connection {
    segment: Arc<LinkSegment>,
    joined: Joined,
    receiver: Option<InboxReceiver>,
    withdrawn: bool,
}

impl Connection {
    pub fn open(
        fleet: &str,
        spec: LinkSpec,
        name: &str,
        role: &str,
        incarnation: u64,
    ) -> Result<Self> {
        Self::open_with_access(
            fleet,
            spec,
            name,
            role,
            incarnation,
            &Access::OwnerOnly,
        )
    }

    /// Explicit per-Link access. Other fleet resources retain their own policy.
    pub fn open_with_access(
        fleet: &str,
        spec: LinkSpec,
        name: &str,
        role: &str,
        incarnation: u64,
        access: &Access,
    ) -> Result<Self> {
        tokio::runtime::Handle::try_current().map_err(|_| {
            Error::Malformed("a Link inbox reader requires a Tokio runtime")
        })?;
        let segment = Arc::new(access.open(fleet, spec)?);
        let joined = segment.join(name, role, incarnation)?;
        // Establish the owner before binding: a failed receiver bind must
        // release the lane before its kernel hold is dropped too.
        let mut connection =
            Self { segment, joined, receiver: None, withdrawn: false };
        connection.receiver =
            Some(connection.segment.receiver(connection.joined.lane)?);
        Ok(connection)
    }

    pub fn lane(&self) -> usize {
        self.joined.lane
    }

    /// The directory and bounded inbox, for caller-owned routing/protocols.
    pub fn segment(&self) -> &Arc<LinkSegment> {
        &self.segment
    }

    /// Exact dead incarnations recovered while this connection joined.
    pub fn reclaimed(&self) -> &[super::Reclaimed] {
        &self.joined.reclaimed
    }

    /// Read one frame or directory change. Cancellation consumes neither.
    pub async fn next(
        &mut self,
        frame: &mut Vec<u8>,
    ) -> Result<Received> {
        let receiver = self.receiver.as_mut().ok_or(Error::Malformed(
            "the Link connection has withdrawn from admission",
        ))?;
        receiver.next(frame).await
    }

    /// Stop new admission while the caller finishes its already accepted work.
    /// The kernel ownership hold remains until this connection is dropped.
    pub fn withdraw(&mut self) -> Result<()> {
        if !self.withdrawn {
            self.segment.inbox().release_lane(self.joined.lane)?;
            self.withdrawn = true;
            self.receiver.take();
        }
        Ok(())
    }

    /// Bind a caller-owned event bus to this connection. `Routing::Topic`
    /// carries plain topics; an envelope owner may supply `Routing::Key`.
    /// No `ApplicationEvent` codec or Claviron application identity is needed.
    pub fn events<L: EventLayout>(
        self,
        bus: Arc<FleetEventBus<L>>,
        far: fn(&str) -> bool,
        routing: Routing,
    ) -> Result<EventConnection<L>> {
        EventConnection::new(self, bus, far, routing)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Err(error) = self.withdraw() {
            tracing::warn!(%error, "could not withdraw owned Orbit Link lane");
        }
        // Drop the reader before `joined` releases the kernel hold, so a
        // successor can bind its reader without racing our old wake socket.
        self.receiver.take();
    }
}

#[cfg(test)]
#[path = "../../tests/link/connection.rs"]
mod tests;
