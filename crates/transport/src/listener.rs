//! Named listener generations, independent of their request owner.

use std::fmt;
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use claviron_http::config::{ListenerConfig, ListenerSettings, TcpSecurity};
use claviron_http::listener::{HttpProtocol, ListenerTransport};
use claviron_runtime::{
    AppState, ExecutionSpec, Reloadable, Resolve, RunContext, Runnable,
    RunnableInstance, ServiceStartPolicy, execution::execution_spec, registry,
};

/// State one listener in the log: its id, transport, addresses, protocols,
/// whether it speaks TLS and its execution placement. One line per listener
/// and the same line for every transport.
///
/// The process that owns the fact states it at `info`: a standalone process
/// once it is bound, a supervised master once its workers booted
/// ([`state_declared`]). A worker binds the same as each of its siblings and
/// states it at `debug`. Stopping is not stated here: the process's shutdown
/// line covers it.
pub fn state_accepting<A: fmt::Display>(
    config: &ListenerConfig,
    addresses: &[A],
    owner: bool,
) {
    let transport = match config.transport() {
        ListenerTransport::Tcp => "tcp",
        ListenerTransport::Unix => "unix",
        ListenerTransport::Quic => "quic",
    };
    let (protocols, tls): (&[HttpProtocol], bool) = match &config.settings {
        ListenerSettings::Tcp(tcp) => {
            (&tcp.protocols, matches!(tcp.security, TcpSecurity::Tls { .. }))
        }
        ListenerSettings::Quic(_) => (&[HttpProtocol::H3], true),
    };
    let addresses = Joined(addresses);
    let protocols = Protocols(protocols);
    let execution = config.execution.as_str();
    if owner {
        tracing::info!(listener = %config.id, transport, %addresses, %protocols, tls, execution, "🌐 listener accepting");
    } else {
        tracing::debug!(listener = %config.id, transport, %addresses, %protocols, tls, execution, "🌐 listener accepting");
    }
}

struct Joined<'a, A>(&'a [A]);

impl<A: fmt::Display> fmt::Display for Joined<'_, A> {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        for (at, item) in self.0.iter().enumerate() {
            if at != 0 {
                f.write_str(", ")?;
            }
            item.fmt(f)?;
        }
        Ok(())
    }
}

struct Protocols<'a>(&'a [HttpProtocol]);

impl fmt::Display for Protocols<'_> {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        for (at, protocol) in self.0.iter().enumerate() {
            if at != 0 {
                f.write_str(", ")?;
            }
            f.write_str(match protocol {
                HttpProtocol::H1 => "h1",
                HttpProtocol::H2 => "h2",
                HttpProtocol::H3 => "h3",
            })?;
        }
        Ok(())
    }
}

/// A component that owns listener declarations, read when the master
/// states them. The declarations are read then, not copied here, so a
/// reload is what is stated.
pub trait DeclaresListeners: Send + Sync + 'static {
    fn declared_listeners(&self) -> Vec<Arc<ListenerConfig>>;
}

/// The listener owners of this process. A supervised master binds nothing:
/// its workers do. It states what they accept on once their boot is
/// confirmed, from here.
#[derive(Default)]
pub struct ListenerOwners(Mutex<Vec<Weak<dyn DeclaresListeners>>>);

impl ListenerOwners {
    /// Record `owner`, creating the registry entry on first use.
    pub fn add(
        state: &AppState,
        owner: Weak<dyn DeclaresListeners>,
    ) -> registry::Result<()> {
        let registry = state.registry_ref();
        let owners = match registry.resolve::<Self>() {
            Some(owners) => owners,
            None => {
                registry.insert(Arc::new(Self::default()))?;
                registry.resolve::<Self>().ok_or_else(|| {
                    registry::Error::msg("listener owners were not registered")
                })?
            }
        };
        owners.0.lock().expect("listener owners poisoned").push(owner);
        Ok(())
    }
}

/// The master's statement of every declared listener, once its workers
/// booted. A process without listener owners states nothing.
pub fn state_declared(state: &AppState) {
    let Some(owners) = state.registry_ref().resolve::<ListenerOwners>() else {
        return;
    };
    let owners: Vec<_> = owners
        .0
        .lock()
        .expect("listener owners poisoned")
        .iter()
        .filter_map(Weak::upgrade)
        .collect();
    for owner in owners {
        for config in owner.declared_listeners() {
            state_accepting(&config, &config.listen, true);
        }
    }
}

/// An application-owned adapter supplies a listener's prepared inputs.
/// Transport owns the Runnable; the adapter owns selection and publication.
#[async_trait]
pub trait Adapter: Send + Sync + 'static {
    type Deps: Resolve<AppState>;

    fn config(&self) -> Arc<ListenerConfig>;

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        None
    }

    async fn serve(
        self: Arc<Self>,
        deps: Self::Deps,
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()>;
}

/// One managed listener. Edge and single-application servers supply adapters
/// to this same generation and execution boundary.
pub struct Listener<A>(Arc<A>);

impl<A: Adapter> Listener<A> {
    pub fn new(adapter: A) -> Self {
        Self(Arc::new(adapter))
    }

    pub fn adapter(&self) -> Arc<A> {
        self.0.clone()
    }

    pub fn instance(self: &Arc<Self>) -> RunnableInstance<AppState> {
        let name: Arc<str> = format!("Listener:{}", self.0.config().id).into();
        RunnableInstance::new(name, self.clone())
    }
}

impl<A> std::ops::Deref for Listener<A> {
    type Target = A;
    fn deref(&self) -> &A {
        &self.0
    }
}

#[async_trait]
impl<A: Adapter> Runnable<AppState, A::Deps> for Listener<A> {
    fn execution_spec(
        &self,
        _: &AppState,
    ) -> ExecutionSpec {
        execution_spec(self.0.config().execution)
    }

    fn start_policy(
        &self,
        state: &AppState,
    ) -> ServiceStartPolicy {
        if state.is_master() {
            ServiceStartPolicy::Unavailable
        } else {
            ServiceStartPolicy::Automatic
        }
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        self.0.as_reloadable()
    }

    async fn run(
        self: Arc<Self>,
        deps: A::Deps,
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        self.0.clone().serve(deps, state, context).await
    }
}
