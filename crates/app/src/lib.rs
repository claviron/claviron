//! User-level application identity, lifecycle, and control contracts.
//!
//! Execution engines own live application state. This crate provides the
//! language-neutral manager and controller boundary used to expose that state
//! without treating Claviron infrastructure services as user applications.

mod controller;
pub mod environment;
mod event;
pub mod fleet;
mod identity;
pub mod invoke;
pub mod link;
pub mod management;
mod manager;
mod policy;
mod status;

pub use controller::{ApplicationControlResult, ApplicationController};
pub use event::{
    APPLICATION_EVENT_TOPIC, ApplicationEvent, ApplicationEventError,
    EventArrayKey, EventTopic, EventValue, link_routing_key,
};
pub use identity::{
    ApplicationId, ApplicationIdentityError, ApplicationName,
    ApplicationRuntime,
};
pub use manager::{ApplicationManager, ApplicationManagerError, register};
pub use policy::{ApplicationLimits, ApplicationRetryPolicy};
pub use status::{
    ApplicationDescription, ApplicationLifecycle, ApplicationPoolStatus,
    ApplicationSnapshot,
};
