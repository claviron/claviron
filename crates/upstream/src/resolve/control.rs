//! Reload handoff owns cancellation and observes query/lease future release.
//! Serving profiles and externally named tables are not owned by this control.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::{DesiredProfile, ProfileResolver};

pub(crate) struct Binding {
    pub(crate) desired: Arc<DesiredProfile>,
    pub(crate) owner: Arc<ProfileResolver>,
}
pub(crate) struct Job {
    pub(crate) cancel: CancellationToken,
    pub(crate) joined: CancellationToken,
}
#[derive(Default)]
struct State {
    paused: usize,
    bindings: BTreeMap<String, Arc<Binding>>,
    jobs: BTreeMap<String, Arc<Job>>,
}
#[derive(Default)]
pub(crate) struct Control {
    state: Mutex<State>,
    pub(crate) changed: Notify,
    #[cfg(feature = "metrics")]
    pub(crate) metrics: super::metrics::Store,
}
impl std::fmt::Debug for Control {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("ResolutionControl").finish_non_exhaustive()
    }
}
pub(crate) struct Pause(Arc<Control>);
pub(crate) struct Completion {
    control: Arc<Control>,
    name: String,
    pub(crate) job: Arc<Job>,
}
impl Control {
    /// The application binds the resolver; this crate supplies no default.
    pub(crate) fn driver(
        &self,
        factory: Option<&Arc<dyn claviron_resolver::ResolverFactory>>,
        config: &claviron_resolver::ResolverConfig,
    ) -> Result<
        Arc<dyn claviron_resolver::Resolver>,
        claviron_resolver::ResolveError,
    > {
        match factory {
            Some(factory) => factory.open(config),
            None => Err(claviron_resolver::ResolveError::new(
                claviron_resolver::ResolveErrorKind::Config,
                "no DNS resolver is bound; the application must bind \
                 `dyn claviron_resolver::ResolverFactory` before boot",
            )),
        }
    }

    pub(crate) fn bindings(&self) -> BTreeMap<String, Arc<Binding>> {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).bindings.clone()
    }
    pub(crate) fn pause(self: &Arc<Self>) -> Pause {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.paused += 1;
        for job in state.jobs.values() {
            job.cancel.cancel();
        }
        self.changed.notify_waiters();
        Pause(self.clone())
    }
    pub(crate) fn publish(
        &self,
        bindings: BTreeMap<String, Arc<Binding>>,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            state.paused > 0 && state.jobs.is_empty(),
            "adoption requires joined local resolver handoff"
        );
        state.bindings = bindings;
    }
    pub(crate) fn attach(self: &Arc<Self>) -> Vec<(Arc<Binding>, Completion)> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.paused > 0 {
            return vec![];
        }
        let pending = state
            .bindings
            .iter()
            .filter(|(name, _)| !state.jobs.contains_key(*name))
            .map(|(name, binding)| (name.clone(), binding.clone()))
            .collect::<Vec<_>>();
        pending
            .into_iter()
            .map(|(name, binding)| {
                let job = Arc::new(Job {
                    cancel: CancellationToken::new(),
                    joined: CancellationToken::new(),
                });
                state.jobs.insert(name.clone(), job.clone());
                (binding, Completion { control: self.clone(), name, job })
            })
            .collect()
    }
}
impl Pause {
    pub(crate) async fn joined(&self) {
        let jobs = self
            .0
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .jobs
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for job in jobs {
            job.joined.cancelled().await;
        }
    }
}
impl Drop for Pause {
    fn drop(&mut self) {
        self.0.state.lock().unwrap_or_else(|p| p.into_inner()).paused -= 1;
        self.0.changed.notify_waiters();
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        let mut state =
            self.control.state.lock().unwrap_or_else(|p| p.into_inner());
        if state
            .jobs
            .get(&self.name)
            .is_some_and(|job| Arc::ptr_eq(job, &self.job))
        {
            state.jobs.remove(&self.name);
        }
        // The containing future drops its pending query/exact tenure before
        // this local completion guard. This never ACKs origin-native IO.
        self.job.joined.cancel();
        self.control.changed.notify_waiters();
    }
}
