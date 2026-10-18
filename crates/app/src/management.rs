//! Process-local application management observations.
//!
//! These events report real execution-engine lifecycle transitions. They are
//! deliberately separate from the user-level `ApplicationEvent` messaging
//! contract.

use claviron_runtime::events::Event;

use crate::ApplicationId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationStarted {
    pub id: ApplicationId,
    pub generation: u64,
}

impl Event for ApplicationStarted {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationStopped {
    pub id: ApplicationId,
    pub generation: u64,
}

impl Event for ApplicationStopped {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationFailed {
    pub id: ApplicationId,
    pub generation: u64,
    pub error: String,
}

impl Event for ApplicationFailed {}

#[cfg(test)]
#[path = "../tests/management.rs"]
mod tests;
