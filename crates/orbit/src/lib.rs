//! # `claviron-orbit` — bind Orbitive into claviron's lifecycle
//!
//! Orbitive is framework-agnostic and can be used without Claviron. This crate
//! is the **bind layer**: it joins
//! the Orbit fleet at the right lifecycle point, bridges cross-process events,
//! adapts the Orbit cache data plane to Claviron's runtime and backing stores,
//! and owns Claviron's keyed atomic counter table.
//!
//! Anything that wants Orbit *without* Claviron skips this crate and uses the
//! feature-gated `orbitive` facade directly.
//!
//! See `crates/orbit/VISION.md` §11 for the two-crate split rationale.
//!
//! Each claviron process (master, worker, standalone) joins the shared
//! SHM fleet at boot and the resulting handle is reachable through
//! `state.fleet()`.

use std::sync::Arc;

use claviron_runtime::AppState;
pub use orbitive::{Fleet, NodeId, OrbitTyped, RingSpec};

pub mod arena;
pub mod cache;
pub mod cell;
pub mod config;
pub mod counter;
pub mod event;
mod fleet;
pub mod invoke;
pub mod lock;
pub mod placement;
mod registry;
pub mod service;
#[cfg(unix)]
pub mod shm;
pub mod stream;
pub mod transport;

pub use arena::{Arena, ArenaBootPolicy, ArenaProvider};
pub use cell::orbital::{Error as CellError, Orbital};
pub use cell::{Cells, CellsProvider, Text};
pub use counter::{Counter, CounterProvider};
pub use event::{
    APPLICATION_EVENT_PAYLOAD_MAX, ApplicationEventLayout,
    ApplicationEventRegistry, EventRegistry, EventRing, FleetEventCodec,
    FleetEventRegistry,
};
pub use fleet::FleetProvider;
pub use invoke::{
    Invocation, InvocationBus, InvocationCodec, InvocationId,
    InvocationProvider, InvocationSubscription,
};
pub use placement::{Dispatch, DispatchPolicy, Dispatched, Scope, Scoped};
pub use registry::{
    register, register_arena, register_arena_with_policy, register_cells,
    register_counter, register_fleet, register_invoke, register_lock,
    register_streams,
};
pub use stream::StreamsProvider;
// ─────────────────────────────────────────────────────────────────────
// Extension trait — opt-in `state.fleet()` accessor
// ─────────────────────────────────────────────────────────────────────

/// Adds a `state.fleet()` accessor. Bring this trait into scope
/// (`use claviron_orbit::OrbitStateExt;`) to use it.
///
/// Without the import, `AppState` is unaware of orbit — claviron
/// stays clean of orbit-* dependencies at the `AppState` API surface.
pub trait OrbitStateExt {
    /// The shared [`Fleet`] handle for this process. Returns `None`
    /// before the `FleetProvider` has booted.
    fn fleet(&self) -> Option<Arc<Fleet>>;
}

impl OrbitStateExt for AppState {
    fn fleet(&self) -> Option<Arc<Fleet>> {
        self.registry_ref().resolve::<FleetProvider>()?.handle()
    }
}

// ─────────────────────────────────────────────────────────────────────
// Public registration entry — call from the embedder's bootstrap
// ─────────────────────────────────────────────────────────────────────

// Re-export the upstream primitive surface so consumers can
// `use claviron_orbit::{Fleet, NetId64, …};` without a second
// crate-level import. (`OrbitTyped` is already brought into scope by
// the `use orbitive::…` line at the top of this module; re-exporting it
// again would shadow.)
pub use orbitive::events::{
    EVENT_PAYLOAD_MAX, EVENT_RING_KIND, EVENT_RING_SPEC, FleetEvent,
    FleetEventBus, FleetEventCursor, FleetEventPoll,
};
pub use orbitive::{Frame, NetId64, OrbitEpoch};
