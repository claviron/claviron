//! A client pool whose admission holds where its declaration says: in this
//! process, or across the fleet. Clients and waiting callers stay in each
//! process either way; with `scope: fleet` a request also takes a permit from
//! the fleet's admission table before its local client, and returns it when
//! the request ends. See `docs/design/FLEET_CLIENT_ADMISSION.md`.

use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use claviron_orbit::ClientAdmissionProvider;
use claviron_orbit::client_admission::{AdmissionPermit, Key};
use claviron_runtime::AppState;

use super::config::{PoolConfig, PoolScope};
use super::{
    AcquireError, ActiveRequestPolicy, ClientLease, ClientManager, ClientPool,
    OptionsError, PoolOptions,
};

/// Why a pool could not be built, or a request not admitted.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionError<E: std::error::Error + 'static> {
    #[error("client pool bounds: {0}")]
    Options(#[from] OptionsError),
    #[error("{0}")]
    Pool(#[source] AcquireError<E>),
    #[error("scope: fleet needs the fleet admission table, which {0}")]
    FleetUnavailable(&'static str),
    #[error("fleet admission table: {0}")]
    Table(String),
    #[error("max_concurrency {0} does not fit the fleet admission table")]
    Concurrency(usize),
    #[error("client pool wait queue is full (maximum {max_waiting})")]
    QueueFull { max_waiting: usize },
    #[error("timed out acquiring a client after {0:?}")]
    Timeout(Duration),
}

/// One request budget for the fleet, under a key.
struct FleetBudget {
    provider: Arc<ClientAdmissionProvider>,
    key: Key,
    max_concurrency: u32,
    max_waiting: usize,
    waiting: AtomicUsize,
}

/// A local client pool, with the fleet's admission when its scope is
/// `fleet`.
pub struct AdmittedPool<M: ClientManager> {
    local: ClientPool<M>,
    fleet: Option<FleetBudget>,
    acquire_timeout: Option<Duration>,
}

/// A client admitted under its pool's scope; the permit, if any, returns
/// with it.
pub struct AdmittedLease<M: ClientManager> {
    lease: ClientLease<M>,
    _permit: Option<AdmissionPermit>,
}

impl<M: ClientManager> std::ops::Deref for AdmittedLease<M> {
    type Target = ClientLease<M>;

    fn deref(&self) -> &Self::Target {
        &self.lease
    }
}

impl<M: ClientManager> AdmittedLease<M> {
    /// Retire the client instead of returning it; the permit returns.
    pub fn discard(self) {
        self.lease.discard();
    }
}

impl<M: ClientManager> AdmittedPool<M>
where
    M::Error: std::error::Error + 'static,
{
    /// Build the pool `config` declares. `name` keys the fleet budget: the
    /// consumer and the remote side whose limit it is, such as `acme` and a
    /// CA's directory URL. Every process naming the same key shares one
    /// budget and must declare the same `max_concurrency`.
    pub fn new(
        state: &AppState,
        name: &str,
        manager: M,
        config: &PoolConfig,
    ) -> Result<Self, AdmissionError<M::Error>> {
        config.validate()?;
        let fleet = match config.scope {
            PoolScope::Process => None,
            PoolScope::Fleet => {
                let provider = state
                    .registry_ref()
                    .resolve::<ClientAdmissionProvider>()
                    .ok_or(AdmissionError::FleetUnavailable(
                        "this process did not register",
                    ))?;
                let max_concurrency = u32::try_from(config.max_concurrency)
                    .map_err(|_| {
                        AdmissionError::Concurrency(config.max_concurrency)
                    })?;
                Some(FleetBudget {
                    provider,
                    key: key(name),
                    max_concurrency,
                    max_waiting: config.max_waiting,
                    waiting: AtomicUsize::new(0),
                })
            }
        };
        // With a fleet stage the wait bound covers both stages, so the local
        // pool does not apply it a second time.
        let mut options = PoolOptions::from(config);
        let acquire_timeout = config.acquire_timeout;
        if fleet.is_some() {
            options = options.acquire_timeout(None);
        }
        Ok(Self {
            local: ClientPool::new(manager, options)?,
            fleet,
            acquire_timeout,
        })
    }

    /// The process-local pool: its maintenance, pause and resume.
    pub fn local(&self) -> &ClientPool<M> {
        &self.local
    }

    /// Admit one request: the fleet's permit first, then a local client.
    pub async fn acquire(
        &self,
        policy: ActiveRequestPolicy,
    ) -> Result<AdmittedLease<M>, AdmissionError<M::Error>> {
        let Some(fleet) = &self.fleet else {
            let lease = self
                .local
                .acquire(policy)
                .await
                .map_err(AdmissionError::Pool)?;
            return Ok(AdmittedLease { lease, _permit: None });
        };
        let admitted = async {
            let permit = fleet.admit().await?;
            let lease = self
                .local
                .acquire(policy)
                .await
                .map_err(AdmissionError::Pool)?;
            Ok(AdmittedLease { lease, _permit: Some(permit) })
        };
        match self.acquire_timeout {
            Some(timeout) => tokio::time::timeout(timeout, admitted)
                .await
                .map_err(|_| AdmissionError::Timeout(timeout))?,
            None => admitted.await,
        }
    }
}

impl FleetBudget {
    /// One permit, waiting on the table's own wake while the budget is spent.
    async fn admit<E: std::error::Error + 'static>(
        &self
    ) -> Result<AdmissionPermit, AdmissionError<E>> {
        let pool = self.provider.handle().ok_or(
            AdmissionError::FleetUnavailable("is not open in this process"),
        )?;
        let table = |error: orbitive::pool::Error| {
            AdmissionError::Table(error.to_string())
        };
        let mut waiter = None;
        loop {
            let since = pool.version(self.key).map_err(table)?;
            match pool.admit(self.key, self.max_concurrency) {
                Ok(permit) => return Ok(permit),
                Err(orbitive::pool::Error::ConcurrencyLimit { .. }) => {}
                Err(error) => return Err(table(error)),
            }
            if waiter.is_none() {
                waiter = Some(Waiter::join(&self.waiting, self.max_waiting)?);
            }
            poll_fn(|cx| pool.poll_capacity(self.key, since, cx))
                .await
                .map_err(table)?;
        }
    }
}

/// One caller counted against `max_waiting` until it is admitted or gives up.
struct Waiter<'a>(&'a AtomicUsize);

impl<'a> Waiter<'a> {
    fn join<E: std::error::Error + 'static>(
        waiting: &'a AtomicUsize,
        max_waiting: usize,
    ) -> Result<Self, AdmissionError<E>> {
        waiting
            .try_update(Ordering::AcqRel, Ordering::Acquire, |now| {
                (now < max_waiting).then_some(now + 1)
            })
            .map_err(|_| AdmissionError::QueueFull { max_waiting })?;
        Ok(Self(waiting))
    }
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// A stable 128-bit key for `name`: FNV-1a, the same in every process and
/// every build, as a shared budget's key must be.
fn key(name: &str) -> Key {
    const OFFSET: u128 = 0x6c62272e07bb014262b821756295c58d;
    const PRIME: u128 = 0x0000000001000000000000000000013b;
    let digest = name.bytes().fold(OFFSET, |hash, byte| {
        (hash ^ u128::from(byte)).wrapping_mul(PRIME)
    });
    Key::new(digest)
}

#[cfg(test)]
#[path = "../../tests/clients/admitted.rs"]
mod tests;
