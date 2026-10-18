//! One current service per profile; one process owner observes registry changes.
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock, Weak};

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::app::{CompiledUpstreamApp, UpstreamApp};
use crate::compile::CompiledUpstreams;
use crate::compile::profile::Profile;
use crate::proxy::service::UpstreamService;
use crate::proxy::service::runtime::ServiceRuntime;

#[derive(Default)]
struct Snapshot {
    compiled: Option<Arc<CompiledUpstreams>>,
    desired: Option<Arc<crate::resolve::DesiredUpstreams>>,
    services: BTreeMap<String, UpstreamService>,
    current: BTreeMap<String, Arc<crate::app::current::CurrentService>>,
}
pub(crate) struct Registry {
    snapshot: RwLock<Snapshot>,
    pub(crate) changed: Arc<Notify>,
    generations: Arc<AtomicU64>,
    driver: Mutex<Option<JoinHandle<()>>>,
    retired: Mutex<Vec<UpstreamService>>,
}
impl std::fmt::Debug for Registry {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("profiles", &self.services().len())
            .finish()
    }
}
impl Registry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            snapshot: RwLock::new(Snapshot::default()),
            changed: Arc::new(Notify::new()),
            generations: Arc::new(AtomicU64::new(0)),
            driver: Mutex::new(None),
            retired: Mutex::new(Vec::new()),
        })
    }
    pub(crate) fn profile(
        &self,
        name: &str,
    ) -> Option<Arc<Profile>> {
        self.snapshot
            .read()
            .ok()?
            .services
            .get(name)
            .map(|service| service.inner.profile.clone())
    }
    pub(crate) fn service(
        &self,
        name: &str,
    ) -> Option<UpstreamService> {
        self.snapshot.read().ok()?.services.get(name).cloned()
    }
    pub(crate) fn services(&self) -> Vec<UpstreamService> {
        self.snapshot
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .services
            .values()
            .cloned()
            .collect()
    }
    pub(crate) fn app(
        &self,
        name: &str,
    ) -> Option<UpstreamApp> {
        let snapshot = self.snapshot.read().ok()?;
        let compiled = snapshot.compiled.as_ref()?;
        let matcher = compiled.apps.get(name).cloned().or_else(|| {
            compiled
                .profiles
                .contains_key(name)
                .then(|| CompiledUpstreamApp::root_profile(name))
        })?;
        {
            let current = matcher
                .profiles()
                .map(|profile| {
                    Some((
                        profile.to_owned(),
                        snapshot.current.get(profile)?.clone(),
                    ))
                })
                .collect::<Option<BTreeMap<_, _>>>()?;
            Some(UpstreamApp::bound(name, matcher, current))
        }
    }
    pub(crate) fn adopt(
        self: &Arc<Self>,
        compiled: Option<Arc<CompiledUpstreams>>,
        mut runtime: ServiceRuntime,
    ) -> Vec<UpstreamService> {
        runtime.changed = self.changed.clone();
        runtime.generations = self.generations.clone();
        let retired = {
            let mut snapshot =
                self.snapshot.write().unwrap_or_else(|p| p.into_inner());
            let mut old = std::mem::take(&mut snapshot.services);
            let mut services = BTreeMap::new();
            if let Some(compiled) = &compiled {
                for (name, profile) in &compiled.profiles {
                    // DNS changes the serving members without changing desired
                    // config. An identical reload must retain that current view.
                    let desired_unchanged =
                        snapshot.compiled.as_ref().is_some_and(|previous| {
                            previous.profiles.get(name).is_some_and(|old| {
                                old.as_ref() == profile.as_ref()
                            })
                        });
                    let previous = old.remove(name);
                    let service = match previous {
                        Some(service)
                            if desired_unchanged
                                || service.inner.profile.as_ref()
                                    == profile.as_ref() =>
                        {
                            service
                        }
                        previous => {
                            if let Some(service) = previous {
                                old.insert(name.clone(), service);
                            }
                            UpstreamService::with_runtime(
                                profile.clone(),
                                runtime.clone(),
                            )
                        }
                    };
                    services.insert(name.clone(), service);
                }
            }
            // Seal captured old handles while the registry publication is still
            // locked. The process-owned drain may not be polled until later.
            let retired = old.into_values().collect::<Vec<_>>();
            for service in &retired {
                service.retire();
            }
            let current = {
                let mut previous = std::mem::take(&mut snapshot.current);
                services
                    .iter()
                    .map(|(name, service)| {
                        let current =
                            previous.remove(name).unwrap_or_else(|| {
                                crate::app::current::CurrentService::new(
                                    service,
                                )
                            });
                        current.publish(service);
                        (name.clone(), current)
                    })
                    .collect()
                // Removed entries are not revived on name reuse. Captured apps
                // keep their sealed last service, preserving route ownership/503.
            };
            *snapshot = Snapshot { compiled, desired: None, services, current };
            retired
        };
        self.retired
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .extend(retired.iter().cloned());
        let mut driver = self.driver.lock().unwrap_or_else(|p| p.into_inner());
        if driver.is_none() {
            *driver = Some(tokio::spawn(run(Arc::downgrade(self), runtime)));
        }
        self.changed.notify_waiters();
        retired
    }
    pub(crate) fn desired_profile(
        &self,
        name: &str,
    ) -> Option<Arc<crate::resolve::DesiredProfile>> {
        self.snapshot.read().ok()?.desired.as_ref()?.profile(name)
    }
    /// Complete desired config adoption, with one authoritative exclusion for
    /// every changed named profile. Rejected preparation seals no serving handle.
    pub(crate) fn adopt_resolved(
        self: &Arc<Self>,
        desired: Arc<crate::resolve::DesiredUpstreams>,
        candidates: BTreeMap<String, crate::resolve::MaterializedProfile>,
        table: &crate::resolve::ResolutionTable,
        mut runtime: ServiceRuntime,
    ) -> Result<Vec<UpstreamService>, crate::proxy::service::MembershipError>
    {
        runtime.changed = self.changed.clone();
        runtime.generations = self.generations.clone();
        let mut snapshot =
            self.snapshot.write().unwrap_or_else(|p| p.into_inner());
        let mut prepared = BTreeMap::new();
        let mut retained = BTreeMap::new();
        for (name, candidate) in &candidates {
            if table.fleet_authority() != runtime.lock.is_some() {
                return Err(
                    crate::proxy::service::MembershipError::StateBinding(
                        Box::new(crate::resolve::PublicationError::Invalid(
                            "boot authority differs from runtime",
                        )),
                    ),
                );
            }
            let unchanged = snapshot
                .desired
                .as_ref()
                .and_then(|old| old.profile(name))
                .zip(desired.profile(name))
                .is_some_and(|(old, next)| old == next);
            if unchanged && let Some(service) = snapshot.services.get(name) {
                retained.insert(name.clone(), service.clone());
            } else {
                prepared.insert(
                    name.clone(),
                    crate::proxy::service::prepare::PreparedService::new(
                        candidate.profile.clone(),
                        runtime.clone(),
                    )?,
                );
            }
        }
        let retired = snapshot
            .services
            .iter()
            .filter(|(name, _)| !retained.contains_key(*name))
            .map(|(_, service)| service.clone())
            .collect::<Vec<_>>();
        let admissions = retired
            .iter()
            .map(|service| {
                service
                    .inner
                    .lifecycle
                    .admission
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
            })
            .collect::<Vec<_>>();
        let approvals =
            prepared.keys().map(|name| &candidates[name]).collect::<Vec<_>>();
        let committed = table
            .with_profiles(&approvals, || {
                let mut services = retained;
                for (name, candidate) in prepared {
                    services.insert(name, candidate.commit());
                }
                for service in &retired {
                    service.retire_locked();
                }
                self.retired
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .extend(retired.iter().cloned());
                let mut previous = std::mem::take(&mut snapshot.current);
                let current = services
                    .iter()
                    .map(|(name, service)| {
                        let current =
                            previous.remove(name).unwrap_or_else(|| {
                                crate::app::current::CurrentService::new(
                                    service,
                                )
                            });
                        current.publish(service);
                        (name.clone(), current)
                    })
                    .collect();
                let compiled = Arc::new(CompiledUpstreams {
                    profiles: candidates
                        .iter()
                        .map(|(name, candidate)| {
                            (name.clone(), candidate.profile.clone())
                        })
                        .collect(),
                    apps: desired.apps.clone(),
                });
                *snapshot = Snapshot {
                    compiled: Some(compiled),
                    desired: Some(desired),
                    services,
                    current,
                };
            })
            .map_err(|error| {
                crate::proxy::service::MembershipError::StateBinding(Box::new(
                    error,
                ))
            })?;
        if committed.is_none() {
            return Err(crate::proxy::service::MembershipError::Superseded);
        }
        drop(admissions);
        drop(snapshot);
        let mut driver = self.driver.lock().unwrap_or_else(|p| p.into_inner());
        if driver.is_none() {
            *driver = Some(tokio::spawn(run(Arc::downgrade(self), runtime)));
        }
        self.changed.notify_waiters();
        Ok(retired)
    }
    /// Publish only against the exact view captured before preparing a candidate.
    /// The process driver owns old-view drain, independently of the caller.
    pub(crate) fn publish_membership(
        &self,
        expected: &crate::proxy::service::MembershipTarget,
        profile: Arc<Profile>,
    ) -> Result<UpstreamService, crate::proxy::service::MembershipError> {
        self.publish_membership_checked(expected, profile, None)
    }
    pub(crate) fn publish_resolved_membership(
        &self,
        expected: &crate::proxy::service::MembershipTarget,
        profile: Arc<Profile>,
        publications: &[(crate::resolve::ResolutionEntry, u64)],
    ) -> Result<UpstreamService, crate::proxy::service::MembershipError> {
        self.publish_membership_checked(expected, profile, Some(publications))
    }
    fn publish_membership_checked(
        &self,
        expected: &crate::proxy::service::MembershipTarget,
        profile: Arc<Profile>,
        publications: Option<&[(crate::resolve::ResolutionEntry, u64)]>,
    ) -> Result<UpstreamService, crate::proxy::service::MembershipError> {
        let mut snapshot =
            self.snapshot.write().unwrap_or_else(|p| p.into_inner());
        let name = expected.service.inner.profile.name.clone();
        let previous = snapshot
            .services
            .get(&name)
            .ok_or(crate::proxy::service::MembershipError::Superseded)?
            .clone();
        if !Arc::ptr_eq(&previous.inner, &expected.service.inner) {
            return Err(crate::proxy::service::MembershipError::Superseded);
        }
        let next = match publications {
            Some(publications) => {
                let next = previous.replace_resolved_membership_at(
                    profile,
                    expected.generation,
                    publications,
                    |next| {
                        if !Arc::ptr_eq(&previous.inner, &next.inner) {
                            self.retired
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .push(previous.clone());
                            snapshot
                                .current
                                .get(&name)
                                .expect("serving profile has a current service")
                                .publish(next);
                            snapshot
                                .services
                                .insert(name.clone(), next.clone());
                        }
                    },
                )?;
                drop(snapshot);
                if !Arc::ptr_eq(&previous.inner, &next.inner) {
                    self.changed.notify_waiters();
                }
                return Ok(next);
            }
            None => {
                if previous
                    .inner
                    .profile
                    .origins
                    .iter()
                    .chain(profile.origins.iter())
                    .any(|origin| {
                        matches!(
                            origin.slot.0.as_ref(),
                            Some(crate::state::SlotRef::Resolution(_))
                        )
                    })
                {
                    return Err(
                        crate::proxy::service::MembershipError::StateBinding(
                            Box::new(
                                crate::resolve::PublicationError::Invalid(
                                    "published members require authoritative revisions",
                                ),
                            ),
                        ),
                    );
                }
                previous
                    .replace_membership_at(profile, Some(expected.generation))?
            }
        };
        if Arc::ptr_eq(&previous.inner, &next.inner) {
            return Ok(next);
        }
        // Queue before exposing the successor. No await between synchronous
        // seal, process-owned retirement and captured-app publication.
        self.retired
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(previous.clone());
        snapshot
            .current
            .get(&name)
            .expect("serving profile has a current service")
            .publish(&next);
        snapshot.services.insert(name.clone(), next.clone());
        drop(snapshot);
        self.changed.notify_waiters();
        Ok(next)
    }
    fn take_retired(&self) -> Vec<UpstreamService> {
        std::mem::take(
            &mut *self.retired.lock().unwrap_or_else(|p| p.into_inner()),
        )
    }
}
impl Drop for Registry {
    fn drop(&mut self) {
        for service in self
            .snapshot
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .services
            .values()
        {
            service.epoch().background.cancel();
        }
        for service in
            self.retired.get_mut().unwrap_or_else(|p| p.into_inner()).iter()
        {
            service.epoch().background.cancel();
        }
        if let Some(driver) =
            self.driver.get_mut().unwrap_or_else(|p| p.into_inner()).take()
        {
            driver.abort();
        }
    }
}

// Process-owned retirement/drain driver; health duties belong to Runnables.
async fn run(
    registry: Weak<Registry>,
    runtime: ServiceRuntime,
) {
    let mut retirements = tokio::task::JoinSet::new();
    loop {
        let notified = runtime.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let Some(current) = registry.upgrade() else {
            return;
        };
        for service in current.take_retired() {
            retirements.spawn(async move { service.stop().await });
        }
        drop(current);
        tokio::select! {
            biased;
            _ = runtime.shutdown.cancelled() => {
                if let Some(current) = registry.upgrade() {
                    let services = current.services(); drop(current);
                    for service in services { let epoch = service.epoch(); epoch.background.cancel(); epoch.tasks.close(); epoch.tasks.wait().await; }
                }
                while let Some(result) = retirements.join_next().await { report(result); }
                return;
            }
            _ = &mut notified => {},
            result = retirements.join_next(), if !retirements.is_empty() => { if let Some(result) = result { report(result); } },
        }
    }
}
fn report(
    result: Result<
        claviron_runtime::gate::GateDrainOutcome,
        tokio::task::JoinError,
    >
) {
    match result {
        Err(error) => {
            tracing::error!(%error, "upstream profile retirement failed")
        }
        Ok(claviron_runtime::gate::GateDrainOutcome::Forced { remaining }) => {
            tracing::debug!(
                remaining,
                "upstream profile drain deadline elapsed"
            )
        }
        Ok(_) => {}
    }
}
