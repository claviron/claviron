//! Typed, domain-owned lifecycle policy for shared-memory resources.
//!
//! Orbitive owns physical POSIX SHM mechanics. This module binds those
//! mechanics to Claviron provider lifecycle without treating the active
//! master role as reset or unlink authority.

use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use claviron_runtime::registry::{
    self, Bootable, Component, Finalizable, Reloadable,
};
use claviron_runtime::{AppState, ComponentOrder};
use orbitive::OrbitTyped;
use tracing::{error, trace};

use crate::{FleetProvider, OrbitStateExt};

/// Physical shape of a managed shared-memory resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShmShape {
    OrbitRing,
    OrbitTable,
    PosixRegion,
}

/// Non-mutating result of inspecting a resource's physical object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhysicalShmState {
    Missing,
    Valid { actual_size: usize },
}

impl From<orbitive::shm::ShmValidation> for PhysicalShmState {
    fn from(value: orbitive::shm::ShmValidation) -> Self {
        match value {
            orbitive::shm::ShmValidation::Missing => Self::Missing,
            orbitive::shm::ShmValidation::Valid { actual_size } => {
                Self::Valid { actual_size }
            }
        }
    }
}

/// Provider-owned evidence about live users of a resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceActivity {
    Active,
    Quiescent,
    Unknown,
}

/// Domain authority for deciding whether a named SHM resource still has live
/// users outside the current process.
///
/// The lifecycle layer owns the question, but not the evidence. A supervisor
/// can answer it from exact process presence; another embedder may provide a
/// different source. Absence of this capability is `Unknown`, never proof of
/// quiescence.
#[async_trait]
pub trait ShmActivitySource: Send + Sync + 'static {
    async fn activity(
        &self,
        state: &AppState,
        descriptor: ShmResourceDescriptor,
    ) -> registry::Result<ResourceActivity>;
}

/// What to do when a destructive boot policy finds active users.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActiveBootPolicy {
    Reuse,
    Reject,
}

/// Domain policy for a physically valid resource found at boot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExistingBootPolicy {
    Reuse,
    ResetWhenQuiescent { when_active: ActiveBootPolicy },
    RecreateWhenQuiescent { when_active: ActiveBootPolicy },
    Reject,
}

/// Domain policy after runnables have drained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalizePolicy {
    Preserve,
    ResetWhenQuiescent,
    UnlinkWhenQuiescent,
}

/// Stable declaration used by lifecycle decisions and diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShmResourceDescriptor {
    pub provider: &'static str,
    pub resource: &'static str,
    pub shape: ShmShape,
    pub orbit_kind: Option<u8>,
    pub existing_boot: ExistingBootPolicy,
    pub finalize: FinalizePolicy,
}

/// Result of creating a resource after a `Missing` observation.
///
/// `Reused` covers the create race where another process publishes a valid
/// object between validation and the locked open-or-create operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShmCreateOutcome {
    Created,
    Reused,
    /// The public domain handle does not expose create provenance. The object
    /// was missing at probe time and is now open, but a peer may have won the
    /// race in between.
    Opened,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShmBootAction {
    Created,
    Reused,
    Opened,
    Reset,
    Recreated,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShmBootDecision {
    pub descriptor: ShmResourceDescriptor,
    pub physical: PhysicalShmState,
    pub activity: Option<ResourceActivity>,
    pub action: ShmBootAction,
}

#[derive(Clone, Debug, Default)]
pub struct ShmBootOutcome {
    pub decisions: Vec<ShmBootDecision>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShmFinalizeAction {
    Preserved,
    PreservedActive,
    Reset,
    Unlinked,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShmFinalizeDecision {
    pub descriptor: ShmResourceDescriptor,
    pub activity: Option<ResourceActivity>,
    pub action: ShmFinalizeAction,
}

#[derive(Clone, Debug, Default)]
pub struct ShmFinalizeOutcome {
    pub decisions: Vec<ShmFinalizeDecision>,
}

/// Lifecycle phase attached to a runtime inventory report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShmLifecyclePhase {
    Boot,
    Finalize,
}

/// Latest successful decision retained by the runtime inventory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShmLifecycleReport {
    Boot(ShmBootDecision),
    Finalize(ShmFinalizeDecision),
}

impl ShmLifecycleReport {
    pub fn phase(&self) -> ShmLifecyclePhase {
        match self {
            Self::Boot(_) => ShmLifecyclePhase::Boot,
            Self::Finalize(_) => ShmLifecyclePhase::Finalize,
        }
    }

    pub fn descriptor(&self) -> ShmResourceDescriptor {
        match self {
            Self::Boot(decision) => decision.descriptor,
            Self::Finalize(decision) => decision.descriptor,
        }
    }
}

/// Registry-visible declaration inventory and latest runtime decisions.
///
/// Entries are keyed by `(provider, resource)` and each lifecycle phase keeps
/// only its latest decision. This remains bounded even if boot-like lifecycle
/// operations are repeated in the future.
pub struct ShmLifecycleInventory {
    descriptors: RwLock<Vec<ShmResourceDescriptor>>,
    reports: RwLock<Vec<ShmLifecycleReport>>,
}

impl ShmLifecycleInventory {
    fn new() -> Self {
        Self {
            descriptors: RwLock::new(Vec::new()),
            reports: RwLock::new(Vec::new()),
        }
    }

    /// Static declarations for every resource registered through the managed
    /// wrapper.
    pub fn descriptors(&self) -> Vec<ShmResourceDescriptor> {
        self.descriptors
            .read()
            .expect("SHM descriptor inventory poisoned")
            .clone()
    }

    /// Latest successful runtime decision for each resource and phase.
    pub fn reports(&self) -> Vec<ShmLifecycleReport> {
        self.reports.read().expect("SHM report inventory poisoned").clone()
    }

    fn declare(
        &self,
        descriptor: ShmResourceDescriptor,
    ) -> registry::Result<()> {
        let mut descriptors = self
            .descriptors
            .write()
            .expect("SHM descriptor inventory poisoned");
        if descriptors.iter().any(|existing| {
            existing.provider == descriptor.provider
                && existing.resource == descriptor.resource
        }) {
            return Err(registry::Error::msg(format!(
                "duplicate shared-memory resource '{}:{}'",
                descriptor.provider, descriptor.resource
            )));
        }
        descriptors.push(descriptor);
        Ok(())
    }

    fn record(
        &self,
        report: ShmLifecycleReport,
    ) {
        let descriptor = report.descriptor();
        let phase = report.phase();
        let mut reports =
            self.reports.write().expect("SHM report inventory poisoned");
        reports.retain(|existing| {
            let existing_descriptor = existing.descriptor();
            existing_descriptor.provider != descriptor.provider
                || existing_descriptor.resource != descriptor.resource
                || existing.phase() != phase
        });
        reports.push(report);
    }
}

/// Physical adapter plus domain policy for one shared-memory resource.
///
/// Activity checks are deliberately implemented by the owning provider: only
/// that domain knows whether a heartbeat, worker-exit observation, or another
/// signal proves quiescence. When it needs a full observation interval, that
/// wait belongs inside `activity`.
#[async_trait]
pub trait ManagedShmResource: Send + Sync {
    fn descriptor(&self) -> ShmResourceDescriptor;

    /// Inspect physical shape without creating, mapping, resetting, or
    /// unlinking. Semantic header validation may run in `reuse`.
    fn validate_physical(
        &self,
        state: &AppState,
    ) -> registry::Result<PhysicalShmState>;

    fn create_or_reuse(
        &self,
        state: &AppState,
    ) -> registry::Result<ShmCreateOutcome>;

    /// Attach to and semantically validate an existing resource.
    fn reuse(
        &self,
        state: &AppState,
    ) -> registry::Result<()>;

    async fn activity(
        &self,
        _state: &AppState,
    ) -> registry::Result<ResourceActivity> {
        Ok(ResourceActivity::Unknown)
    }

    /// Whether the descriptor's destructive existing-object policy is enabled
    /// for this configured run. Disabled means validated reuse.
    fn destructive_boot_enabled(
        &self,
        _state: &AppState,
    ) -> bool {
        true
    }

    /// Whether the descriptor's conditional finalize cleanup is enabled for
    /// this configured run. Disabled means preserve.
    fn finalize_cleanup_enabled(
        &self,
        _state: &AppState,
    ) -> bool {
        true
    }

    fn reset(
        &self,
        _state: &AppState,
    ) -> registry::Result<()> {
        Err(unsupported(self.descriptor(), "reset"))
    }

    fn recreate(
        &self,
        _state: &AppState,
    ) -> registry::Result<()> {
        Err(unsupported(self.descriptor(), "recreate"))
    }

    fn unlink(
        &self,
        _state: &AppState,
    ) -> registry::Result<()> {
        Err(unsupported(self.descriptor(), "unlink"))
    }
}

/// Typed physical adapter for an ordinary Orbit ring.
///
/// It derives the conventional segment name and exact fleet-aware mapping size
/// from `T`, validates without mutation, and delegates semantic header checks
/// to `Fleet::shm_ring::<T>()`.
pub struct OrbitRingResource<T> {
    descriptor: ShmResourceDescriptor,
    _marker: PhantomData<fn() -> T>,
}

/// One ordinary Orbit ring registered as an always-reused, always-preserved
/// transient transport or read model.
pub struct ReusableOrbitRing<T> {
    name: &'static str,
    resource: OrbitRingResource<T>,
}

impl<T> ReusableOrbitRing<T>
where
    T: OrbitTyped,
{
    pub fn new(
        provider: &'static str,
        resource: &'static str,
    ) -> Self {
        Self {
            name: provider,
            resource: OrbitRingResource::new(
                provider,
                resource,
                ExistingBootPolicy::Reuse,
                FinalizePolicy::Preserve,
            ),
        }
    }
}

impl<T> ManagedShmResources for ReusableOrbitRing<T>
where
    T: OrbitTyped,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource> {
        vec![&self.resource]
    }
}

impl<T> OrbitRingResource<T>
where
    T: OrbitTyped,
{
    pub const fn new(
        provider: &'static str,
        resource: &'static str,
        existing_boot: ExistingBootPolicy,
        finalize: FinalizePolicy,
    ) -> Self {
        Self {
            descriptor: ShmResourceDescriptor {
                provider,
                resource,
                shape: ShmShape::OrbitRing,
                orbit_kind: Some(T::KIND),
                existing_boot,
                finalize,
            },
            _marker: PhantomData,
        }
    }

    fn fleet(
        &self,
        state: &AppState,
    ) -> registry::Result<Arc<orbitive::Fleet>> {
        let fleet = state.fleet().ok_or_else(|| {
            registry::Error::msg("Orbit fleet is unavailable")
        })?;
        if !fleet.is_shm() {
            return Err(registry::Error::msg(format!(
                "shared-memory resource '{}:{}' requires an SHM-backed Orbit fleet",
                self.descriptor.provider, self.descriptor.resource
            )));
        }
        Ok(fleet)
    }
}

#[async_trait]
impl<T> ManagedShmResource for OrbitRingResource<T>
where
    T: OrbitTyped,
{
    fn descriptor(&self) -> ShmResourceDescriptor {
        self.descriptor
    }

    fn validate_physical(
        &self,
        state: &AppState,
    ) -> registry::Result<PhysicalShmState> {
        let fleet = self.fleet(state)?;
        let expected_size =
            orbitive::core::ring_shm::segment_size_for_spec_and_fleet(
                T::RING_SPEC,
                fleet.fleet_capacity(),
            )?;
        let name = orbitive::shm::ring_segment_name(fleet.name(), T::KIND);
        Ok(orbitive::shm::ShmRegion::validate_existing(&name, expected_size)?
            .into())
    }

    fn create_or_reuse(
        &self,
        state: &AppState,
    ) -> registry::Result<ShmCreateOutcome> {
        let ring = self.fleet(state)?.shm_ring::<T>()?;
        Ok(if ring.created() {
            ShmCreateOutcome::Created
        } else {
            ShmCreateOutcome::Reused
        })
    }

    fn reuse(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        self.fleet(state)?.shm_ring::<T>()?;
        Ok(())
    }

    fn reset(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        self.fleet(state)?.reset_ring::<T>()?;
        Ok(())
    }

    fn unlink(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        self.fleet(state)?.shm_ring::<T>()?.unlink()?;
        Ok(())
    }
}

/// Domain-owned resource set consumed by [`ManagedShmProvider`].
///
/// This type deliberately does not extend [`Component`]. The domain object
/// cannot be inserted into the runtime registry by itself; only the managed
/// wrapper implements the provider lifecycle.
#[async_trait]
pub trait ManagedShmResources: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    fn boot_priority(&self) -> Option<u8> {
        None
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::default()
    }

    /// Whether this resource set participates in the current process mode.
    fn enabled(
        &self,
        _state: &AppState,
    ) -> bool {
        true
    }

    /// Load domain configuration required by resource policy before the first
    /// physical decision. Registration remains side-effect free.
    async fn prepare(
        &self,
        _state: &AppState,
    ) -> registry::Result<()> {
        Ok(())
    }

    fn reloadable(&self) -> bool {
        false
    }

    async fn reload(
        &self,
        _state: &AppState,
    ) -> registry::Result<()> {
        Ok(())
    }

    fn shm_resources(&self) -> Vec<&dyn ManagedShmResource>;
}

/// Optional handle surface exposed by a managed resource set.
///
/// This keeps existing capability consumers independent of lifecycle details:
/// they resolve the managed provider and obtain its domain handle as before.
pub trait ManagedShmHandle: ManagedShmResources {
    type Handle;

    fn managed_handle(&self) -> Option<Arc<Self::Handle>>;
}

/// The only registry provider for a [`ManagedShmResources`] implementation.
///
/// Construction is private to [`register`], which binds provider
/// lifecycle and inventory registration. Consequently a managed domain object
/// cannot accidentally expose metadata while bypassing boot/finalize policy.
pub struct ManagedShmProvider<P> {
    resources: P,
    inventory: Arc<ShmLifecycleInventory>,
}

impl<P> ManagedShmProvider<P> {
    pub fn resources(&self) -> &P {
        &self.resources
    }
}

impl<P> ManagedShmProvider<P>
where
    P: ManagedShmHandle,
{
    pub fn handle(&self) -> Option<Arc<P::Handle>> {
        self.resources.managed_handle()
    }
}

impl<P> Component<AppState> for ManagedShmProvider<P>
where
    P: ManagedShmResources,
{
    // Every managed segment lives in the fleet's SHM namespace. Optional so a
    // disabled resource set stays registered without one; boot requires it
    // once the set is enabled. The resources reach the fleet through the
    // state, the same way finalize does, which receives no dependencies.
    type Deps = (Option<Arc<FleetProvider>>,);

    fn name(&self) -> &'static str {
        self.resources.name()
    }

    fn boot_priority(&self) -> Option<u8> {
        self.resources.boot_priority()
    }

    fn order(&self) -> ComponentOrder {
        self.resources.order()
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<AppState>> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        self.resources.reloadable().then_some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl<P> Bootable<AppState, (Option<Arc<FleetProvider>>,)>
    for ManagedShmProvider<P>
where
    P: ManagedShmResources,
{
    async fn boot(
        &self,
        (fleet_provider,): (Option<Arc<FleetProvider>>,),
        state: &AppState,
    ) -> registry::Result<()> {
        if !self.resources.enabled(state) {
            return Ok(());
        }
        if fleet_provider.is_none() {
            return Err(registry::Error::msg(format!(
                "'{}' requires FleetProvider",
                self.resources.name()
            )));
        }
        self.resources.prepare(state).await?;
        let outcome = boot(&self.resources, state).await?;
        for decision in outcome.decisions {
            self.inventory.record(ShmLifecycleReport::Boot(decision));
        }
        Ok(())
    }
}

#[async_trait]
impl<P> Reloadable<AppState> for ManagedShmProvider<P>
where
    P: ManagedShmResources,
{
    async fn reload(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        self.resources.reload(state).await
    }
}

#[async_trait]
impl<P> Finalizable<AppState> for ManagedShmProvider<P>
where
    P: ManagedShmResources,
{
    async fn finalize(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        if !self.resources.enabled(state) {
            return Ok(());
        }
        let outcome = finalize(&self.resources, state).await?;
        for decision in outcome.decisions {
            self.inventory.record(ShmLifecycleReport::Finalize(decision));
        }
        Ok(())
    }
}

/// Register a domain-owned SHM resource set through the mandatory lifecycle
/// wrapper and publish its static descriptors to the shared inventory.
pub fn register<P>(
    state: &AppState,
    resources: P,
) -> registry::Result<()>
where
    P: ManagedShmResources,
{
    let registry = state.registry_ref();
    if registry.resolve::<ManagedShmProvider<P>>().is_some() {
        return Ok(());
    }

    let inventory = match registry.resolve::<ShmLifecycleInventory>() {
        Some(inventory) => inventory,
        None => {
            let inventory = Arc::new(ShmLifecycleInventory::new());
            registry.insert(Arc::clone(&inventory))?;
            inventory
        }
    };
    for resource in resources.shm_resources() {
        inventory.declare(resource.descriptor())?;
    }
    registry.register(Arc::new(ManagedShmProvider { resources, inventory }))?;
    Ok(())
}

/// Register an ordinary transient/read-model ring whose retained bytes are
/// made harmless by cursor or freshness semantics rather than boot reset.
pub fn register_reusable_ring<T>(
    state: &AppState,
    provider: &'static str,
    resource: &'static str,
) -> registry::Result<()>
where
    T: OrbitTyped,
{
    register(state, ReusableOrbitRing::<T>::new(provider, resource))
}

/// Resolve the process-wide SHM declaration and runtime decision inventory.
pub fn inventory(state: &AppState) -> Option<Arc<ShmLifecycleInventory>> {
    state.registry_ref().resolve::<ShmLifecycleInventory>()
}

/// Ask the registered domain authority for a resource's current activity.
/// Standalone mode is intrinsically quiescent because there are no peer
/// Claviron processes. Supervised modes fail closed when no authority exists.
pub async fn activity(
    state: &AppState,
    descriptor: ShmResourceDescriptor,
) -> registry::Result<ResourceActivity> {
    if state.is_standalone() {
        return Ok(ResourceActivity::Quiescent);
    }
    match state.registry_ref().resolve_dyn::<dyn ShmActivitySource>() {
        Some(source) => source.activity(state, descriptor).await,
        None => Ok(ResourceActivity::Unknown),
    }
}

/// Apply typed boot policy to every SHM resource declared by a provider.
pub async fn boot<P>(
    provider: &P,
    state: &AppState,
) -> registry::Result<ShmBootOutcome>
where
    P: ManagedShmResources + ?Sized,
{
    let mut outcome = ShmBootOutcome::default();
    for resource in provider.shm_resources() {
        let descriptor = resource.descriptor();
        let physical = match resource.validate_physical(state) {
            Ok(physical) => physical,
            Err(error) => {
                error!(
                    target: "claviron::shm",
                    provider = descriptor.provider,
                    resource = descriptor.resource,
                    action = ?ShmBootAction::Rejected,
                    error = %error,
                    "shared-memory boot decision"
                );
                return Err(error);
            }
        };
        let decision = match physical {
            PhysicalShmState::Missing => {
                resource.create_or_reuse(state).map(|outcome| {
                    let action = match outcome {
                        ShmCreateOutcome::Created => ShmBootAction::Created,
                        ShmCreateOutcome::Reused => ShmBootAction::Reused,
                        ShmCreateOutcome::Opened => ShmBootAction::Opened,
                    };
                    (None, action)
                })
            }
            // A configuration check leaves what it finds: it may be looking at
            // the live service's segment.
            PhysicalShmState::Valid { .. }
                if is_config_check(state)
                    || !resource.destructive_boot_enabled(state) =>
            {
                resource.reuse(state).map(|_| (None, ShmBootAction::Reused))
            }
            PhysicalShmState::Valid { .. } => {
                apply_existing_boot_policy(
                    resource,
                    state,
                    descriptor.existing_boot,
                )
                .await
            }
        };
        let (activity, action) = match decision {
            Ok(decision) => decision,
            Err(error) => {
                error!(
                    target: "claviron::shm",
                    provider = descriptor.provider,
                    resource = descriptor.resource,
                    physical = ?physical,
                    action = ?ShmBootAction::Rejected,
                    error = %error,
                    "shared-memory boot decision"
                );
                return Err(error);
            }
        };
        let decision =
            ShmBootDecision { descriptor, physical, activity, action };
        trace!(
            target: "claviron::shm",
            provider = descriptor.provider,
            resource = descriptor.resource,
            physical = ?physical,
            activity = ?activity,
            action = ?action,
            "shared-memory boot decision"
        );
        outcome.decisions.push(decision);
    }
    Ok(outcome)
}

/// Apply typed finalization policy to every SHM resource declared by a
/// provider. Active users always preserve the object; unknown activity fails
/// closed instead of authorizing a destructive operation.
pub async fn finalize<P>(
    provider: &P,
    state: &AppState,
) -> registry::Result<ShmFinalizeOutcome>
where
    P: ManagedShmResources + ?Sized,
{
    let mut outcome = ShmFinalizeOutcome::default();
    for resource in provider.shm_resources() {
        let descriptor = resource.descriptor();
        let decision =
            apply_finalize_policy(resource, state, descriptor.finalize).await;
        let (activity, action) = match decision {
            Ok(decision) => decision,
            Err(error) => {
                error!(
                    target: "claviron::shm",
                    provider = descriptor.provider,
                    resource = descriptor.resource,
                    action = ?ShmFinalizeAction::Rejected,
                    error = %error,
                    "shared-memory finalize decision"
                );
                return Err(error);
            }
        };
        let decision = ShmFinalizeDecision { descriptor, activity, action };
        trace!(
            target: "claviron::shm",
            provider = descriptor.provider,
            resource = descriptor.resource,
            activity = ?activity,
            action = ?action,
            "shared-memory finalize decision"
        );
        outcome.decisions.push(decision);
    }
    Ok(outcome)
}

/// Whether this process only checks configuration; see
/// [`claviron_runtime::ConfigCheck`]. Its boot reuses what it finds and its
/// finalize preserves everything, whatever a resource's policy says.
fn is_config_check(state: &AppState) -> bool {
    state.registry_ref().resolve::<claviron_runtime::ConfigCheck>().is_some()
}

async fn apply_finalize_policy(
    resource: &dyn ManagedShmResource,
    state: &AppState,
    policy: FinalizePolicy,
) -> registry::Result<(Option<ResourceActivity>, ShmFinalizeAction)> {
    let descriptor = resource.descriptor();
    if policy == FinalizePolicy::Preserve
        || is_config_check(state)
        || !resource.finalize_cleanup_enabled(state)
    {
        return Ok((None, ShmFinalizeAction::Preserved));
    }

    let activity = resource.activity(state).await?;
    let action = match activity {
        ResourceActivity::Active => ShmFinalizeAction::PreservedActive,
        ResourceActivity::Unknown => {
            return Err(registry::Error::msg(format!(
                "shared-memory resource '{}:{}' has unknown activity; refusing {:?}",
                descriptor.provider, descriptor.resource, policy
            )));
        }
        ResourceActivity::Quiescent => match policy {
            FinalizePolicy::ResetWhenQuiescent => {
                resource.reset(state)?;
                ShmFinalizeAction::Reset
            }
            FinalizePolicy::UnlinkWhenQuiescent => {
                resource.unlink(state)?;
                ShmFinalizeAction::Unlinked
            }
            FinalizePolicy::Preserve => unreachable!(),
        },
    };
    Ok((Some(activity), action))
}

async fn apply_existing_boot_policy(
    resource: &dyn ManagedShmResource,
    state: &AppState,
    policy: ExistingBootPolicy,
) -> registry::Result<(Option<ResourceActivity>, ShmBootAction)> {
    match policy {
        ExistingBootPolicy::Reuse => {
            resource.reuse(state)?;
            Ok((None, ShmBootAction::Reused))
        }
        ExistingBootPolicy::Reject => Err(registry::Error::msg(format!(
            "shared-memory resource '{}:{}' already exists and policy rejects reuse",
            resource.descriptor().provider,
            resource.descriptor().resource
        ))),
        ExistingBootPolicy::ResetWhenQuiescent { when_active } => {
            apply_destructive_boot_policy(
                resource,
                state,
                when_active,
                ShmBootAction::Reset,
            )
            .await
        }
        ExistingBootPolicy::RecreateWhenQuiescent { when_active } => {
            apply_destructive_boot_policy(
                resource,
                state,
                when_active,
                ShmBootAction::Recreated,
            )
            .await
        }
    }
}

async fn apply_destructive_boot_policy(
    resource: &dyn ManagedShmResource,
    state: &AppState,
    when_active: ActiveBootPolicy,
    destructive_action: ShmBootAction,
) -> registry::Result<(Option<ResourceActivity>, ShmBootAction)> {
    let descriptor = resource.descriptor();
    let activity = resource.activity(state).await?;
    match activity {
        ResourceActivity::Quiescent => {
            match destructive_action {
                ShmBootAction::Reset => resource.reset(state)?,
                ShmBootAction::Recreated => resource.recreate(state)?,
                ShmBootAction::Created
                | ShmBootAction::Reused
                | ShmBootAction::Opened
                | ShmBootAction::Rejected => {
                    unreachable!()
                }
            }
            Ok((Some(activity), destructive_action))
        }
        ResourceActivity::Active if when_active == ActiveBootPolicy::Reuse => {
            resource.reuse(state)?;
            Ok((Some(activity), ShmBootAction::Reused))
        }
        ResourceActivity::Active => Err(registry::Error::msg(format!(
            "shared-memory resource '{}:{}' is active; boot policy rejects reuse",
            descriptor.provider, descriptor.resource
        ))),
        ResourceActivity::Unknown => Err(registry::Error::msg(format!(
            "shared-memory resource '{}:{}' has unknown activity; refusing destructive boot",
            descriptor.provider, descriptor.resource
        ))),
    }
}

fn unsupported(
    descriptor: ShmResourceDescriptor,
    operation: &str,
) -> registry::Error {
    registry::Error::msg(format!(
        "shared-memory resource '{}:{}' does not implement {operation}",
        descriptor.provider, descriptor.resource
    ))
}
