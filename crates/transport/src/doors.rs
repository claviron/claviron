//! The sockets of one owner: TCP and Unix, HTTP/1 and HTTP/2, TLS on TCP.
//!
//! Each address becomes a [`Single`](crate::tcp::plain::Single) listener
//! declared the way `http.yaml` declares one, so its defaults are the same
//! ones. The doors share a lifetime and an owner and differ only in where they
//! accept. There is no HTTP/3 here: this is for one application reached over a
//! loopback, a private network or a Unix socket; a QUIC listener is
//! [`quic::Single`](crate::quic::Single)'s.
//!
//! Shutdown stops accepting, asks every connection to finish, and joins them
//! and the sessions they started, with no deadline of its own.

use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use claviron_http::ListenerId;
use claviron_http::config::{
    H1Override, Limit, ListenerConfig, ListenerDefinition, ListenerExecution,
    ListenerOverrides, Override, RequestPolicyOverride, TcpConfig, TcpOverride,
};
use claviron_http::listener::HttpProtocol;
use claviron_runtime::registry::{Error, Result};
use claviron_runtime::{ExecutionSpec, RunContext, RunMode};

use crate::owner::Owner;
use crate::tcp::plain;
#[cfg(feature = "tls")]
use crate::tcp::secure;
#[cfg(feature = "tls")]
use crate::tcp::tls::Tls;

/// Where a listener listens.
#[derive(Clone, Debug)]
pub enum Bind {
    Tcp(SocketAddr),
    Uds(PathBuf),
}

impl Bind {
    fn describe(
        &self,
        secure: bool,
    ) -> String {
        match self {
            Self::Tcp(address) if secure => format!("https://{address}"),
            Self::Tcp(address) => format!("http://{address}"),
            Self::Uds(path) => format!("unix:{}", path.display()),
        }
    }
}

/// Which HTTP versions the sockets accept. Naming neither is the same as
/// naming both: whatever the client speaks.
#[derive(Clone, Copy, Debug)]
pub struct Protocols {
    pub h1: bool,
    pub h2: bool,
}

impl Default for Protocols {
    fn default() -> Self {
        Self { h1: true, h2: true }
    }
}

impl Protocols {
    pub fn resolve(
        h1: bool,
        h2: bool,
    ) -> Self {
        if h1 || h2 { Self { h1, h2 } } else { Self::default() }
    }

    #[cfg(feature = "tls")]
    fn alpn(self) -> Vec<&'static [u8]> {
        let mut alpn: Vec<&'static [u8]> = Vec::new();
        if self.h2 {
            alpn.push(b"h2");
        }
        if self.h1 {
            alpn.push(b"http/1.1");
        }
        alpn
    }

    /// In preference order, as ALPN offers them.
    #[cfg(feature = "tls")]
    fn accepted(self) -> Vec<HttpProtocol> {
        let mut protocols = Vec::new();
        if self.h2 {
            protocols.push(HttpProtocol::H2);
        }
        if self.h1 {
            protocols.push(HttpProtocol::H1);
        }
        protocols
    }
}

/// Every socket one owner answers on.
pub struct Doors {
    binds: Vec<Bind>,
    settings: Settings,
}

/// What all of an owner's doors share.
#[derive(Clone)]
struct Settings {
    protocols: Protocols,
    tcp_backlog: NonZeroU32,
    listener: ListenerOverrides,
    #[cfg(feature = "tls")]
    tls: Option<Tls>,
}

impl Doors {
    pub fn new(
        binds: Vec<Bind>,
        protocols: Protocols,
    ) -> Self {
        Self {
            binds,
            settings: Settings {
                protocols,
                tcp_backlog: TcpConfig::default().backlog,
                listener: ListenerOverrides::default(),
                #[cfg(feature = "tls")]
                tls: None,
            },
        }
    }

    /// Set the pending TCP connection queue before serving. The default comes
    /// from `TcpConfig`; the OS may cap the requested value. Values above the
    /// socket API range fail binding, rather than wrapping.
    pub fn tcp_backlog(
        mut self,
        backlog: NonZeroU32,
    ) -> Self {
        self.settings.tcp_backlog = backlog;
        self
    }

    /// What a listener in `http.yaml` could say, for every door here: execution,
    /// limits, and the TCP, HTTP/1 and HTTP/2 settings. Requests and responses
    /// are the owner's policy, and the driver applies none.
    pub fn listener(
        mut self,
        listener: ListenerOverrides,
    ) -> Self {
        self.settings.listener = listener;
        self
    }

    /// Serve TLS on the TCP sockets. A Unix socket ignores it: its boundary is
    /// the filesystem's already.
    #[cfg(feature = "tls")]
    pub fn tls(
        mut self,
        tls: Option<Tls>,
    ) -> Self {
        self.settings.tls = tls;
        self
    }

    pub fn binds(&self) -> &[Bind] {
        &self.binds
    }

    /// One door per address, each with a name of its own. Opens nothing.
    pub fn doors(&self) -> Vec<Door> {
        let (mut tcp, mut uds) = (0, 0);
        self.binds
            .iter()
            .map(|bind| {
                let (kind, count) = match bind {
                    Bind::Tcp(_) => ("tcp", &mut tcp),
                    Bind::Uds(_) => ("uds", &mut uds),
                };
                *count += 1;
                let name = match *count {
                    1 => format!("runner-{kind}"),
                    n => format!("runner-{kind}-{n}"),
                };
                Door {
                    id: name.parse().expect("a door's name is a listener id"),
                    bind: bind.clone(),
                    settings: self.settings.clone(),
                }
            })
            .collect()
    }
}

/// One socket of one owner: it binds, accepts and drains on its own, under its
/// own listener identity.
pub struct Door {
    id: ListenerId,
    bind: Bind,
    settings: Settings,
}

impl Door {
    pub fn id(&self) -> &ListenerId {
        &self.id
    }

    /// Where this door runs in each serving process.
    pub fn execution_spec(&self) -> ExecutionSpec {
        self.settings
            .listener
            .execution
            .resolve(&ListenerExecution::default())
            .for_listener(&self.id)
    }

    /// A listener for this door. No deadline unless one was declared: a slow
    /// client's header read is the proxy's or the operator's to bound. It never
    /// bounds a body: the driver applies no request policy, that is the owner's.
    fn listener(
        &self,
        secure: bool,
    ) -> Result<Arc<ListenerConfig>> {
        let address = match &self.bind {
            Bind::Tcp(address) => *address,
            Bind::Uds(_) => SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        };
        #[cfg(feature = "tls")]
        let protocols = if secure {
            self.settings.protocols.accepted()
        } else {
            vec![HttpProtocol::H1]
        };
        #[cfg(not(feature = "tls"))]
        let protocols = vec![HttpProtocol::H1];
        let serves_h1 = protocols.contains(&HttpProtocol::H1);
        let mut definition = ListenerDefinition::tcp(address, protocols);
        definition.tls = Override::Value(secure);
        definition.tcp = Override::Value(TcpOverride {
            backlog: Override::Value(self.settings.tcp_backlog),
            ..Default::default()
        });
        if serves_h1
            && self.settings.listener.h1.header_read_timeout.is_inherit()
        {
            definition.h1 = Override::Value(H1Override {
                header_read_timeout: Override::Value(None),
                ..Default::default()
            });
        }
        definition.requests = RequestPolicyOverride {
            max_body_size: Some(Limit::Off),
            body_read_timeout: Some(Limit::Off),
            ..Default::default()
        };
        definition
            .resolve(self.id.clone(), &self.settings.listener)
            .map(Arc::new)
            .map_err(failure)
    }

    /// The owner behind this door in cleartext. Cleartext has no ALPN: HTTP/2
    /// is by prior knowledge, beside HTTP/1.1 unless only HTTP/2 was named.
    pub fn cleartext<O: Owner>(
        &self,
        owner: Arc<O>,
    ) -> Result<Arc<plain::Single<O>>> {
        let single = plain::Single::shared(self.listener(false)?, owner)?;
        let protocols = self.settings.protocols;
        Ok(Arc::new(if protocols.h2 {
            single.prior_knowledge(protocols.h1)
        } else {
            single
        }))
    }

    /// Accept until the context ends, then drain.
    pub async fn serve<O: Owner>(
        &self,
        owner: Arc<O>,
        context: &RunContext,
        run_mode: RunMode,
    ) -> Result<()> {
        match &self.bind {
            bind @ Bind::Tcp(_) => {
                // Read the certificate before the socket opens: a bad path or
                // a key that does not match is a boot failure, not a failed
                // handshake later.
                #[cfg(feature = "tls")]
                if let Some(tls) = &self.settings.tls {
                    let tls = tls
                        .config(&self.settings.protocols.alpn())
                        .map_err(failure)?;
                    let snapshot = Arc::new(secure::Single::shared(
                        self.listener(true)?,
                        tls,
                        owner,
                    )?);
                    tracing::info!("listening on {}", bind.describe(true));
                    return serve_secure(snapshot, context, run_mode).await;
                }
                let snapshot = self.cleartext(owner)?;
                tracing::info!("listening on {}", bind.describe(false));
                serve_plain(snapshot, context, run_mode).await
            }
            bind @ Bind::Uds(path) => {
                tracing::info!("listening on {}", bind.describe(false));
                #[cfg(unix)]
                {
                    crate::unix::serve(path, self.cleartext(owner)?, context)
                        .await
                }
                #[cfg(not(unix))]
                Err(failure(format!(
                    "unix:{}: this platform has no Unix sockets",
                    path.display()
                )))
            }
        }
    }
}

async fn serve_plain<O: Owner>(
    snapshot: Arc<plain::Single<O>>,
    context: &RunContext,
    run_mode: RunMode,
) -> Result<()> {
    let snapshots = ArcSwapOption::from(Some(snapshot.clone()));
    crate::tcp::listener::serve(
        &snapshots,
        snapshot,
        context,
        run_mode,
        |snapshot, stream, peer, scope| {
            plain::serve(stream, peer, snapshot, scope)
        },
    )
    .await
}

#[cfg(feature = "tls")]
async fn serve_secure<O: Owner>(
    snapshot: Arc<secure::Single<O>>,
    context: &RunContext,
    run_mode: RunMode,
) -> Result<()> {
    let snapshots = ArcSwapOption::from(Some(snapshot.clone()));
    crate::tcp::listener::serve(
        &snapshots,
        snapshot,
        context,
        run_mode,
        |snapshot, stream, peer, scope| {
            secure::serve(stream, peer, snapshot, scope)
        },
    )
    .await
}

fn failure(error: impl std::fmt::Display) -> Error {
    Error::msg(error.to_string())
}

#[cfg(test)]
#[path = "../tests/doors.rs"]
mod tests;
