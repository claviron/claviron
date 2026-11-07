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
use std::sync::atomic::{AtomicBool, Ordering};
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

    /// Whether an owner has taken this attempt.
    pub fn claimed(&self) -> bool {
        !matches!(
            *self.attempt.slot.lock().expect("socket offer"),
            Slot::Waiting(_)
        )
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

    /// Say the attempt reached its owner, under an [`Admission`], whose door
    /// commits nothing.
    pub fn reach(self) -> RouteResponse {
        self.accept_with(|_| async {})
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

/// Whether a session attempt gets through a plan, where what commits it is not
/// this response: an edge's Link acknowledgment, or a WebTransport session the
/// transport answers itself.
pub struct Admission {
    offer: Offer,
    reached: Arc<AtomicBool>,
}

/// Commits nothing; it only records that the plan let the attempt through.
struct AdmissionDoor(Arc<AtomicBool>);

impl Door for AdmissionDoor {
    fn commit(
        self: Box<Self>,
        proposal: RouteResponse,
        _: Deferred,
    ) -> RouteResponse {
        self.0.store(true, Ordering::Relaxed);
        proposal
    }

    fn refuse(
        self: Box<Self>,
        response: RouteResponse,
    ) -> RouteResponse {
        refusal(response)
    }
}

impl Admission {
    pub fn new(info: OfferInfo) -> Self {
        let reached = Arc::new(AtomicBool::new(false));
        let offer = Offer::new(info, AdmissionDoor(Arc::clone(&reached)));
        Self { offer, reached }
    }

    /// What goes in the extensions of the request that runs through the plan.
    pub fn offer(&self) -> Offer {
        self.offer.clone()
    }

    /// What the plan refused the attempt with. `None` when it reached its owner.
    pub fn refused(
        self,
        response: RouteResponse,
    ) -> Option<RouteResponse> {
        let response = self.offer.finish(response);
        (!self.reached.load(Ordering::Relaxed)).then_some(response)
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

/// The handshake an HTTP request asks for. The door owns it: an owner's headers
/// cross, the protocol's own never do.
#[derive(Clone, Debug)]
pub enum Handshake {
    /// An `Upgrade`, answered `101` with this key's accept value.
    H1(String),
    /// An extended CONNECT, answered `200`.
    H2,
}

impl Handshake {
    /// What this request asks for. `Ok(None)` is an ordinary request, including
    /// an `Upgrade` to anything but a WebSocket, which HTTP lets a server ignore.
    /// An `Err` is a WebSocket request that is not a valid one.
    pub fn from_request<B>(
        request: &http::Request<B>
    ) -> Result<Option<Self>, StatusCode> {
        let h2 = request.version() == http::Version::HTTP_2
            && request.method() == http::Method::CONNECT
            && request
                .extensions()
                .get::<hyper::ext::Protocol>()
                .is_some_and(|protocol| protocol.as_str() == "websocket");
        let h1 = crate::upgrade::is_h1_header_upgrade(request, "websocket");
        if !h1 && !h2 {
            return Ok(None);
        }
        if request
            .headers()
            .get(http::header::SEC_WEBSOCKET_VERSION)
            .is_none_or(|version| version != "13")
        {
            return Err(StatusCode::BAD_REQUEST);
        }
        if h2 {
            return Ok(Some(Self::H2));
        }
        let key = request
            .headers()
            .get(http::header::SEC_WEBSOCKET_KEY)
            .and_then(|key| key.to_str().ok())
            .ok_or(StatusCode::BAD_REQUEST)?;
        Ok(Some(Self::H1(key.to_owned())))
    }

    /// The proposal's status and framing become this handshake's own.
    fn commit(
        &self,
        response: &mut RouteResponse,
    ) {
        let headers = response.headers_mut();
        for name in [
            http::header::CONNECTION,
            http::header::UPGRADE,
            http::header::TRANSFER_ENCODING,
            http::header::CONTENT_LENGTH,
            http::header::SEC_WEBSOCKET_ACCEPT,
        ] {
            headers.remove(name);
        }
        headers.remove("keep-alive");
        headers.remove("proxy-connection");
        let status = match self {
            Self::H1(key) => {
                headers.insert(
                    http::header::UPGRADE,
                    http::HeaderValue::from_static("websocket"),
                );
                headers.insert(
                    http::header::CONNECTION,
                    http::HeaderValue::from_static("Upgrade"),
                );
                if let Ok(accept) = http::HeaderValue::from_str(
                    &crate::upgrade::websocket_accept_key(key),
                ) {
                    headers.insert(http::header::SEC_WEBSOCKET_ACCEPT, accept);
                }
                StatusCode::SWITCHING_PROTOCOLS
            }
            Self::H2 => StatusCode::OK,
        };
        *response.status_mut() = status;
    }
}

/// An HTTP/1.1 upgrade or an HTTP/2 extended CONNECT, on a connection this
/// process holds.
struct HttpDoor {
    on_upgrade: hyper::upgrade::OnUpgrade,
    sessions: crate::session::SessionSpawner,
    handshake: Handshake,
}

impl Door for HttpDoor {
    fn commit(
        self: Box<Self>,
        mut proposal: RouteResponse,
        deferred: Deferred,
    ) -> RouteResponse {
        let Self { on_upgrade, sessions, handshake } = *self;
        let shutdown = sessions.clone();
        let session = async move {
            tokio::select! {
                biased;
                () = shutdown.wait_graceful_shutdown() => {}
                upgraded = on_upgrade => match upgraded {
                    Ok(upgraded) => deferred(SocketIo::Stream(upgraded)).await,
                    Err(error) => {
                        tracing::debug!("session upgrade failed: {error}");
                    }
                },
            }
        };
        // Held by the owner that joins it at shutdown before the handshake is
        // answered, so an accepted session cannot be detached from it.
        if let Err(error) = sessions.spawn(session) {
            tracing::debug!("session was not started: {error}");
            return crate::err_status(StatusCode::SERVICE_UNAVAILABLE);
        }
        handshake.commit(&mut proposal);
        proposal
    }

    fn refuse(
        self: Box<Self>,
        response: RouteResponse,
    ) -> RouteResponse {
        refusal(response)
    }
}

/// Recognize a session attempt on a request this connection received, and put
/// its offer in the request's extensions. An ordinary request is left untouched
/// and allocates nothing.
pub fn attach<B>(
    request: &mut http::Request<B>,
    sessions: &crate::session::SessionSpawner,
) -> Result<Option<Offer>, StatusCode> {
    if request.method() != http::Method::CONNECT
        && !request.headers().contains_key(http::header::UPGRADE)
    {
        return Ok(None);
    }
    let Some(handshake) = Handshake::from_request(request)? else {
        return Ok(None);
    };
    let Some(on_upgrade) =
        request.extensions().get::<hyper::upgrade::OnUpgrade>().cloned()
    else {
        return Ok(None);
    };
    let carrier = match handshake {
        Handshake::H1(_) => Carrier::H1,
        Handshake::H2 => Carrier::H2,
    };
    let offer = Offer::new(
        OfferInfo { kind: SocketKind::WebSocket, carrier },
        HttpDoor { on_upgrade, sessions: sessions.clone(), handshake },
    );
    request.extensions_mut().insert(offer.clone());
    Ok(Some(offer))
}

#[cfg(test)]
#[path = "../tests/socket.rs"]
mod tests;
