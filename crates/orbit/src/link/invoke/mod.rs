//! Bounded command/result calls over a service-owned Link geometry.
//!
//! No HTTP types, ring publication, implicit retry or operation timeout. One
//! driver owns the connection's reader, optionally alongside its event bridge.
//! Group members are trusted SHM writers, not authenticated remote principals.

mod driver;
mod frame;
mod state;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::sync::{Notify, oneshot};

use super::{Connection, EventConnection, LinkSegment};
use crate::invoke::{CallError as Error, CallResult as Result, Request, Route};

pub use driver::Driver;
use frame::{Body, Frame};
use state::State;

impl From<super::Error> for Error {
    fn from(error: super::Error) -> Self {
        match error {
            super::Error::InboxFull => Self::Busy,
            super::Error::FrameTooLarge => Self::PayloadTooLarge,
            super::Error::Stream(orbitive::stream::Error::Full { .. }) => {
                Self::Busy
            }
            other => Self::Transport(other.to_string()),
        }
    }
}

/// Endpoint generation, not proof of identity or authorization.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Peer {
    pub lane: u16,
    pub incarnation: u64,
}

/// Operator-owned bounds, independent of HTTP and the invocation ring.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub pending_calls: usize,
    pub accepted_calls: usize,
    pub operation_bytes: usize,
    pub request_bytes: usize,
    pub response_bytes: usize,
}

impl Limits {
    pub fn validate(
        &self,
        frame_bytes: usize,
    ) -> Result<()> {
        if self.pending_calls == 0 || self.accepted_calls == 0 {
            return Err(Error::InvalidConfig(
                "call capacities must be positive",
            ));
        }
        if self.operation_bytes == 0 || self.operation_bytes > u16::MAX as usize
        {
            return Err(Error::InvalidConfig(
                "operation bound must fit a positive u16",
            ));
        }
        let available = frame_bytes.checked_sub(frame::HEADER).ok_or(
            Error::InvalidConfig("Link frame cannot hold an invoke header"),
        )?;
        if self
            .operation_bytes
            .checked_add(self.request_bytes)
            .is_none_or(|n| n > available)
            || self.response_bytes > u32::MAX as usize - frame::HEADER
        {
            return Err(Error::InvalidConfig(
                "invoke bounds exceed Link frame capacity",
            ));
        }
        Ok(())
    }
}

/// Request delivered after validating its exact destination generation.
pub struct Call {
    pub peer: Peer,
    pub request: Request,
}

struct Shared {
    segment: Arc<LinkSegment>,
    bodies: orbitive::link::LinkBodies,
    local: Peer,
    limits: Limits,
    state: Mutex<State>,
    changed: Notify,
}

impl Shared {
    fn live(
        &self,
        peer: Peer,
    ) -> bool {
        let inbox = self.segment.inbox();
        let lane = usize::from(peer.lane);
        matches!(inbox.incarnation(lane), Ok(id) if id == peer.incarnation)
            && matches!(inbox.consumers(lane), Ok(n) if n > 0)
    }

    fn send(
        &self,
        frame: &Frame,
    ) -> Result<()> {
        if !self.live(frame.to) {
            return Err(Error::Unavailable);
        }
        self.segment
            .inbox()
            .write(usize::from(frame.to.lane), &frame.encode()?)?;
        // Receivers also verify both stamps: a concurrent replacement can
        // never execute a request addressed to the previous owner.
        Ok(())
    }
}

/// A cheap caller handle. The host must drive its companion `Driver`.
#[derive(Clone)]
pub struct Handle(Arc<Shared>);

impl Handle {
    /// Bind one logical service to this Link. Multiple live matches are an
    /// error; this API introduces no implicit round-robin or failover policy.
    pub fn route(
        &self,
        service: impl Into<String>,
    ) -> Result<Arc<dyn Route>> {
        let service = service.into();
        if service.is_empty() || service.len() > orbitive::link::LANE_NAME_MAX {
            return Err(Error::InvalidConfig("invalid Link service name"));
        }
        Ok(Arc::new(LinkRoute { shared: self.0.clone(), service }))
    }
}

struct LinkRoute {
    shared: Arc<Shared>,
    service: String,
}

struct Waiting {
    shared: Arc<Shared>,
    sequence: u64,
}

impl Drop for Waiting {
    fn drop(&mut self) {
        self.shared
            .state
            .lock()
            .expect("invoke state poisoned")
            .pending
            .remove(&self.sequence);
        self.shared.changed.notify_one();
    }
}

#[async_trait]
impl Route for LinkRoute {
    async fn call(
        &self,
        request: Request,
    ) -> Result<Vec<u8>> {
        let limits = self.shared.limits;
        if request.operation.is_empty()
            || request.operation.len() > limits.operation_bytes
            || request.payload.len() > limits.request_bytes
        {
            return Err(Error::PayloadTooLarge);
        }
        let inbox = self.shared.segment.inbox();
        let candidates: Vec<_> = inbox
            .serving(&self.service)
            .into_iter()
            .filter(|lane| matches!(inbox.consumers(*lane), Ok(n) if n > 0))
            .collect();
        let lane = match candidates.as_slice() {
            [] => return Err(Error::Unavailable),
            [lane] => *lane,
            _ => return Err(Error::AmbiguousTarget),
        };
        let peer =
            Peer { lane: lane as u16, incarnation: inbox.incarnation(lane)? };
        let (sender, mut receiver) = oneshot::channel();
        let sequence;
        let endpoint;
        {
            let mut state =
                self.shared.state.lock().expect("invoke state poisoned");
            if !state.running {
                return Err(Error::Closed);
            }
            if state.pending.len() >= limits.pending_calls {
                return Err(Error::Busy);
            }
            let (stream, ticket) = self.shared.bodies.create()?;
            endpoint = stream;
            sequence = state.insert(peer, sender, limits.pending_calls)?;
            let frame = Frame {
                from: self.shared.local,
                to: peer,
                sequence,
                reply_limit: limits.response_bytes as u32,
                ticket: Some(ticket),
                body: Body::Request {
                    operation: request.operation,
                    payload: request.payload,
                },
            };
            if let Err(error) = self.shared.send(&frame) {
                state.pending.remove(&sequence);
                return Err(error);
            }
        }
        let _waiting = Waiting { shared: self.shared.clone(), sequence };
        // The return channel was reserved before publication. Results never
        // contend for inbox space with events or unrelated calls.
        let (mut reader, mut writer) = endpoint.split();
        writer.finish().map_err(|_| Error::OutcomeUnknown)?;
        drop(writer);
        let response = async {
            let length =
                reader.read_u32_le().await.map_err(|_| Error::OutcomeUnknown)?
                    as usize;
            if length < frame::HEADER
                || length > frame::HEADER + limits.response_bytes
            {
                return Err(Error::PayloadTooLarge);
            }
            let mut bytes = vec![0; length];
            reader
                .read_exact(&mut bytes)
                .await
                .map_err(|_| Error::OutcomeUnknown)?;
            let result = Frame::decode(&bytes)?;
            if result.from != peer
                || result.to != self.shared.local
                || result.sequence != sequence
                || result.ticket.is_some()
            {
                return Err(Error::Protocol("result correlation"));
            }
            match result.body {
                Body::Reply(result) => result.map_err(Error::Remote),
                _ => Err(Error::Protocol("expected result")),
            }
        };
        tokio::select! {
            biased;
            result = response => {
                // A fully validated reserved result wins over a concurrent
                // directory-loss notice, even if that notice removed pending.
                let direct = self.shared.state.lock().expect("invoke state poisoned").complete(sequence, peer, result);
                match direct { Some(result) => result, None => receiver.await.unwrap_or(Err(Error::OutcomeUnknown)) }
            }
            result = &mut receiver => result.unwrap_or(Err(Error::OutcomeUnknown)),
        }
    }
}

impl Connection {
    /// Consume this connection's only reader. Starts no task and opens no
    /// event ring, HTTP body table or second Link.
    pub fn invocations(
        self,
        limits: Limits,
    ) -> Result<(Handle, Driver)> {
        Driver::new(self, None, limits)
    }
}

impl<L: orbitive::events::EventLayout> EventConnection<L> {
    /// Add calls to the existing event connection, retaining one inbox reader.
    pub fn invocations(
        self,
        limits: Limits,
    ) -> Result<(Handle, Driver<L>)> {
        let Self { connection, bridge, bus: _, interest } = self;
        Driver::new(
            connection,
            Some(driver::Events { bridge, interest }),
            limits,
        )
    }
}

#[cfg(test)]
#[path = "../../../tests/link/invoke/mod.rs"]
mod tests;
