//! Local broadcast lifecycle hooks, independent of ApplicationEvent topics.
//!
//! Occupancy is the first hook. These are in-process callbacks, not HTTP
//! webhooks, durable jobs or a replacement for fleet application events.

mod occupancy;

pub(crate) use occupancy::owner::BroadcastOccupancyService;
pub use occupancy::{Hooks, OccupancyChange, OccupancyKind, Subscription};
