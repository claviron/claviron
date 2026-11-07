//! An attempt to open a session, as it passes through a site.
//!
//! A door that recognizes a WebSocket, a CONNECT or a WebTransport request puts
//! an [`Offer`] in the request's extensions and dispatches it like any other.
//! Whatever stands in front of the owner applies to it the same way, and the
//! owner of the selected route answers HTTP and sessions alike. An ordinary
//! request carries no offer and pays nothing for this.
//!
//! Accepting has two steps because a session's I/O does not exist until the
//! handshake's response has gone out: awaiting it while answering would never
//! finish. [`Claim::accept_with`] returns a response proposing the session and
//! keeps the owner's callback. The door then [finishes](Offer::finish) with the
//! response that came back through the reply hooks. A proposal that survived
//! them commits the handshake and runs the callback under the door's session
//! owner; anything else is a refusal, so no status can manufacture a session.

use std::any::Any;
use std::future::Future;
use std::sync::{Arc, Mutex};

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use http::StatusCode;

use crate::{RouteResponse, empty_body};

/// What is being opened, whichever protocol carries it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketKind {
    WebSocket,
    /// A raw tunnel: bytes, with no WebSocket framing.
    Connect,
    WebTransport,
}

/// What carries the attempt to this process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Carrier {
    H1,
    H2,
    H3,
    /// Whole messages over Orbit Link, from an edge that speaks the protocol.
    Orbit,
}

/// The facts of an attempt, for anything that decides about it without taking
/// it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OfferInfo {
    pub kind: SocketKind,
    pub carrier: Carrier,
}

/// A session's I/O, once its handshake has been committed.
pub enum SocketIo {
    /// The connection itself, or an HTTP/2 stream, taken over as bytes.
    Stream(hyper::upgrade::Upgraded),
    /// What only the door that made it can describe: a WebTransport session, or
    /// the messages of an Orbit exchange. The offer's [`OfferInfo`] says which.
    Native(Box<dyn Any + Send>),
}

/// The work of an accepted session, under the door's session owner.
pub type Session = BoxFuture<'static, ()>;
/// What an owner runs with the session's I/O.
pub type Deferred = Box<dyn FnOnce(SocketIo) -> Session + Send>;

/// How one door completes the attempt it recognized.
pub trait Door: Send + 'static {
    /// Commit the handshake on the proposal, hand `deferred` to the session
    /// owner, and return the response to send. A proposal that can no longer be
    /// committed returns a failure and drops `deferred`.
    fn commit(
        self: Box<Self>,
        proposal: RouteResponse,
        deferred: Deferred,
    ) -> RouteResponse;

    /// Answer with a refusal. [`refusal`] gives the status to use.
    fn refuse(
        self: Box<Self>,
        response: RouteResponse,
    ) -> RouteResponse;
}

enum Slot {
    Waiting(Box<dyn Door>),
    Claimed(Box<dyn Door>),
    Proposed(Box<dyn Door>, Deferred),
    Finished,
}

struct Attempt {
    info: OfferInfo,
    slot: Mutex<Slot>,
}

/// One pending attempt. Extensions must be cloneable, so this is a handle:
/// every clone shares one single-use slot, and none of them is a second grant.
#[derive(Clone)]
pub struct Offer {
    attempt: Arc<Attempt>,
}

/// The exclusive right to answer the attempt. Dropping it without accepting
/// leaves a refusal: no other owner can claim the attempt afterwards.
pub struct Claim {
    attempt: Arc<Attempt>,
}

/// Marks a proposing response, tying it to the attempt it was made for. A bare
/// `101` or `200` has none and proposes nothing.
#[derive(Clone)]
struct Receipt(Arc<Attempt>);

impl Offer {
    pub fn new(
        info: OfferInfo,
        door: impl Door,
    ) -> Self {
        Self {
            attempt: Arc::new(Attempt {
                info,
                slot: Mutex::new(Slot::Waiting(Box::new(door))),
            }),
        }
    }

    pub fn info(&self) -> OfferInfo {
        self.attempt.info
    }

    /// Take the attempt, once. `None` when an owner already has it.
    pub fn claim(&self) -> Option<Claim> {
        let mut slot = self.attempt.slot.lock().expect("socket offer");
        match std::mem::replace(&mut *slot, Slot::Finished) {
            Slot::Waiting(door) => {
                *slot = Slot::Claimed(door);
                Some(Claim { attempt: Arc::clone(&self.attempt) })
            }
            other => {
                *slot = other;
                None
            }
        }
    }

    /// End the attempt with the response that came back from the plan: commit
    /// it if this is still a proposal for this attempt, refuse it otherwise.
    pub fn finish(
        &self,
        mut response: RouteResponse,
    ) -> RouteResponse {
        let proposed = response
            .extensions_mut()
            .remove::<Receipt>()
            .is_some_and(|receipt| Arc::ptr_eq(&receipt.0, &self.attempt));
        let taken = std::mem::replace(
            &mut *self.attempt.slot.lock().expect("socket offer"),
            Slot::Finished,
        );
        match taken {
            Slot::Proposed(door, deferred) if proposed => {
                door.commit(response, deferred)
            }
            Slot::Waiting(door)
            | Slot::Claimed(door)
            | Slot::Proposed(door, _) => door.refuse(response),
            Slot::Finished => refusal(response),
        }
    }
}

impl Claim {
    pub fn info(&self) -> OfferInfo {
        self.attempt.info
    }

    /// Propose the session. `session` runs with its I/O after the handshake is
    /// committed, not before; nothing is committed or detached by this call.
    /// Headers set on the returned response cross to the handshake, except what
    /// the protocol itself decides.
    pub fn accept_with<F, Fut>(
        self,
        session: F,
    ) -> RouteResponse
    where
        F: FnOnce(SocketIo) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let deferred: Deferred = Box::new(move |io| session(io).boxed());
        let mut slot = self.attempt.slot.lock().expect("socket offer");
        if let Slot::Claimed(door) =
            std::mem::replace(&mut *slot, Slot::Finished)
        {
            *slot = Slot::Proposed(door, deferred);
        }
        drop(slot);
        let mut response = http::Response::new(empty_body());
        *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        response.extensions_mut().insert(Receipt(Arc::clone(&self.attempt)));
        response
    }
}

/// The response refusing an attempt: what was answered, unless that reads as
/// success. A redirect or an error stands; a file, a router's `200` or a bare
/// `101` is not a session and must not look like one.
pub fn refusal(mut response: RouteResponse) -> RouteResponse {
    let status = response.status();
    if status.is_success() || status.is_informational() {
        *response.status_mut() = StatusCode::FORBIDDEN;
        *response.body_mut() = empty_body();
        response.headers_mut().clear();
    }
    response
}

#[cfg(test)]
#[path = "../tests/socket.rs"]
mod tests;
