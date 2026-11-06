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

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use claviron_http::config::{HttpSnapshot, ListenerConfig, TcpConfig};
use claviron_runtime::registry::{Error, Result};
use claviron_runtime::{RunContext, RunMode};
use tokio::task::JoinSet;

use crate::owner::Owner;
use crate::tcp::plain;
#[cfg(feature = "tls")]
use crate::tcp::secure;
#[cfg(feature = "tls")]
use crate::tcp::tls::Tls;

const LISTENER: &str = "runner";

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

    #[cfg(feature = "tls")]
    fn declared(self) -> &'static str {
        match (self.h1, self.h2) {
            (true, false) => "[h1]",
            (false, true) => "[h2]",
            _ => "[h2, h1]",
        }
    }
}

/// Every socket one owner answers on.
pub struct Doors {
    binds: Vec<Bind>,
    protocols: Protocols,
    tcp_backlog: NonZeroU32,
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
            protocols,
            tcp_backlog: TcpConfig::default().backlog,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    /// Set the pending TCP connection queue before serving. The default comes
    /// from `TcpConfig`; the OS may cap the requested value. Values above the
    /// socket API range fail binding, rather than wrapping.
    pub fn tcp_backlog(
        mut self,
        backlog: NonZeroU32,
    ) -> Self {
        self.tcp_backlog = backlog;
        self
    }

    /// Serve TLS on the TCP sockets. A Unix socket ignores it: its boundary is
    /// the filesystem's already.
    #[cfg(feature = "tls")]
    pub fn tls(
        mut self,
        tls: Option<Tls>,
    ) -> Self {
        self.tls = tls;
        self
    }

    pub fn binds(&self) -> &[Bind] {
        &self.binds
    }

    /// A listener for one address. No deadline of its own: a slow client's
    /// header read is the proxy's or the operator's to bound.
    fn listener(
        &self,
        address: Option<SocketAddr>,
        secure: bool,
    ) -> Result<Arc<ListenerConfig>> {
        let listen = address.map_or_else(
            || "127.0.0.1:0".to_owned(),
            |address| address.to_string(),
        );
        #[cfg(feature = "tls")]
        let protocols = if secure { self.protocols.declared() } else { "h1" };
        #[cfg(not(feature = "tls"))]
        let protocols = "h1";
        let backlog = self.tcp_backlog;
        let document = format!(
            "listeners:\n  {LISTENER}:\n    transport: tcp\n    tls: {secure}\n    protocols: {protocols}\n    listen: '{listen}'\n    tcp: {{backlog: {backlog}}}\n    requests: {{max_body_size: off, body_read_timeout: off}}\n    h1: {{header_read_timeout: null}}\n",
        );
        let snapshot: HttpSnapshot = serde_yaml::from_str(&document)
            .map_err(|error| Error::msg(error.to_string()))?;
        snapshot
            .listeners
            .into_values()
            .next()
            .ok_or_else(|| Error::msg("the listener was not declared"))
    }

    /// The owner behind one cleartext address, or a Unix socket without one.
    /// Cleartext has no ALPN: HTTP/2 is by prior knowledge, beside HTTP/1.1
    /// unless only HTTP/2 was named.
    pub fn cleartext<O: Owner>(
        &self,
        address: Option<SocketAddr>,
        owner: Arc<O>,
    ) -> Result<Arc<plain::Single<O>>> {
        let single =
            plain::Single::shared(self.listener(address, false)?, owner)?;
        Ok(Arc::new(if self.protocols.h2 {
            single.prior_knowledge(self.protocols.h1)
        } else {
            single
        }))
    }

    /// Accept on every door until the context ends. One socket that cannot be
    /// bound ends the others: a boot failure, rather than a process that half
    /// serves.
    pub async fn serve<O: Owner>(
        &self,
        owner: Arc<O>,
        context: &RunContext,
        run_mode: RunMode,
    ) -> Result<()> {
        // Read the certificate before any socket opens: a bad path or a key
        // that does not match is a boot failure, not a failed handshake later.
        #[cfg(feature = "tls")]
        let tls = self
            .tls
            .as_ref()
            .map(|tls| tls.config(&self.protocols.alpn()))
            .transpose()
            .map_err(failure)?;
        #[cfg(feature = "tls")]
        let secure = tls.is_some();
        #[cfg(not(feature = "tls"))]
        let secure = false;

        let owned = context.child();
        let mut serving = JoinSet::new();
        for bind in &self.binds {
            let context = owned.child();
            match bind {
                Bind::Tcp(address) => {
                    tracing::info!("listening on {}", bind.describe(secure));
                    #[cfg(feature = "tls")]
                    if let Some(tls) = &tls {
                        let snapshot = Arc::new(secure::Single::shared(
                            self.listener(Some(*address), true)?,
                            tls.clone(),
                            owner.clone(),
                        )?);
                        serving.spawn(serve_secure(snapshot, context, run_mode));
                        continue;
                    }
                    let snapshot =
                        self.cleartext(Some(*address), owner.clone())?;
                    serving.spawn(serve_plain(snapshot, context, run_mode));
                }
                Bind::Uds(path) => {
                    tracing::info!("listening on {}", bind.describe(false));
                    #[cfg(unix)]
                    {
                        let snapshot = self.cleartext(None, owner.clone())?;
                        let path = path.clone();
                        serving.spawn(async move {
                            crate::unix::serve(&path, snapshot, &context).await
                        });
                    }
                    #[cfg(not(unix))]
                    return Err(failure(format!(
                        "unix:{}: this platform has no Unix sockets",
                        path.display()
                    )));
                }
            }
        }

        let mut failed = None;
        while let Some(finished) = serving.join_next().await {
            let result = finished.map_err(failure).and_then(|result| result);
            if let Err(error) = result {
                owned.cancellation_token().cancel();
                failed.get_or_insert(error);
            }
        }
        failed.map_or(Ok(()), Err)
    }
}

async fn serve_plain<O: Owner>(
    snapshot: Arc<plain::Single<O>>,
    context: RunContext,
    run_mode: RunMode,
) -> Result<()> {
    let snapshots = ArcSwapOption::from(Some(snapshot.clone()));
    crate::tcp::listener::serve(
        &snapshots,
        snapshot,
        &context,
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
    context: RunContext,
    run_mode: RunMode,
) -> Result<()> {
    let snapshots = ArcSwapOption::from(Some(snapshot.clone()));
    crate::tcp::listener::serve(
        &snapshots,
        snapshot,
        &context,
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
