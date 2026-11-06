//! Work done once while a connection is accepted, for its requests to read.
//!
//! Some of what a request needs is already decided when the handshake ends:
//! who the peer is, which host was selected, what follows from the two. An
//! accept layer does that work once, there, and leaves its result in the
//! connection's [`ConnState`](claviron_http::ext::ConnState). Every request
//! of the connection then borrows it through `HostContext::conn_state`, with
//! no lookup, lock or allocation of its own.
//!
//! Layers are registered before the first publication and selected per host
//! and protocol family while the host table is prepared. A host no layer
//! selected carries none: accepting its connections runs nothing and
//! allocates nothing.
//!
//! A layer runs when the connection's host is known, which is after the TLS
//! or QUIC handshake. A cleartext connection has no host until a request
//! names one, and may name another with the next; nothing host-bound can be
//! prepared for it, and no layer runs.

use std::sync::Arc;

use claviron_http::ext::{ConnCtx, ConnStateBuilder, HostKey, HostName};

use crate::Site;

/// A connection whose facts are complete: its handshake is over and its host
/// selected, and no request has been read from it.
///
/// More facts of the handshake can be lent here later without changing a
/// layer that does not read them.
#[non_exhaustive]
pub struct Accepted<'a> {
    pub connection: &'a ConnCtx,
    /// The name the peer asked for.
    pub hostname: &'a HostName,
    /// The canonical host that name resolved to.
    pub host: &'a HostKey,
    pub config: &'a Site,
}

impl<'a> Accepted<'a> {
    /// The facts a transport completed for one connection. A fact added later
    /// is given a default here, so a transport that cannot supply it keeps
    /// compiling.
    pub fn new(
        connection: &'a ConnCtx,
        hostname: &'a HostName,
        host: &'a HostKey,
        config: &'a Site,
    ) -> Self {
        Self { connection, hostname, host, config }
    }
}

/// Prepares connection-scoped values, once, at acceptance.
///
/// It decides nothing about the connection: it cannot refuse it or delay it.
/// It runs on the accepting task before the first request, so it does only
/// what the facts in hand allow without waiting.
pub trait AcceptLayer: Send + Sync + 'static {
    fn accept(
        &self,
        accepted: &Accepted<'_>,
        state: &mut ConnStateBuilder,
    );
}

impl<F> AcceptLayer for F
where
    F: for<'a> Fn(&Accepted<'a>, &mut ConnStateBuilder) + Send + Sync + 'static,
{
    fn accept(
        &self,
        accepted: &Accepted<'_>,
        state: &mut ConnStateBuilder,
    ) {
        self(accepted, state)
    }
}

/// The layers selected for one host on one protocol family.
pub type AcceptLayers = Arc<[Arc<dyn AcceptLayer>]>;

/// Run a host's layers on one of its connections.
pub fn accept(
    layers: Option<&AcceptLayers>,
    accepted: &Accepted<'_>,
) -> claviron_http::ext::ConnState {
    let Some(layers) = layers else {
        return Default::default();
    };
    let mut state = ConnStateBuilder::default();
    for layer in layers.iter() {
        layer.accept(accepted, &mut state);
    }
    state.build()
}
