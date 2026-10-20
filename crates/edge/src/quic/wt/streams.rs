//! Bounded session input; delivery never awaits an application receiver.

use std::collections::HashMap;
use std::num::NonZeroUsize;

use h3_webtransport::SessionId;
use tokio::sync::mpsc;

pub(crate) struct SessionStreams<T> {
    capacity: NonZeroUsize,
    sessions: HashMap<SessionId, mpsc::Sender<T>>,
}

impl<T> SessionStreams<T> {
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self { capacity, sessions: HashMap::new() }
    }

    pub(crate) fn register(
        &mut self,
        id: SessionId,
    ) -> mpsc::Receiver<T> {
        let (sender, receiver) = mpsc::channel(self.capacity.get());
        self.sessions.insert(id, sender);
        receiver
    }

    /// Unknown, ended, or saturated sessions refuse this stream rather than
    /// blocking the shared H3 driver or retaining unbounded pending work.
    pub(crate) fn route(
        &mut self,
        id: SessionId,
        stream: T,
    ) -> Result<(), T> {
        let Some(sender) = self.sessions.get(&id) else { return Err(stream) };
        match sender.try_send(stream) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(stream)) => Err(stream),
            Err(mpsc::error::TrySendError::Closed(stream)) => {
                self.remove(id);
                Err(stream)
            }
        }
    }

    pub(crate) fn remove(
        &mut self,
        id: SessionId,
    ) {
        self.sessions.remove(&id);
    }

    pub(crate) fn close(&mut self) {
        self.sessions.clear();
    }

    pub(crate) fn prune_closed(&mut self) {
        self.sessions.retain(|_, sender| !sender.is_closed());
    }
}

#[cfg(test)]
#[path = "../../../tests/quic/wt/streams.rs"]
mod tests;
