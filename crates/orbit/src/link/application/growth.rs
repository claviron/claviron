//! The owner's side of worker growth, shared by every owner of a fleet: a
//! runner's master with its supervisor, or a task service running the
//! fleet's processes. The owner publishes its policy; a sender that finds no
//! receiver to run its exchange now reports pressure once per episode; the
//! owner adds one worker and the episode ends when the receivers serving the
//! application change. Adding the worker is the owner's own action.

use super::{ApplicationSegment, DirectoryWatcher, GrowthPolicy};

/// One unmet-demand episode to act on: add a worker within `policy`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Grow {
    pub policy: GrowthPolicy,
    /// Receiver lanes serving the application when the episode was heard.
    pub serving: usize,
}

/// An owner's growth state for one application in one fleet.
pub struct FleetGrowth {
    application: String,
    policy: Option<GrowthPolicy>,
    /// While an episode is open: how many receivers served when it opened.
    growing: Option<usize>,
}

impl FleetGrowth {
    /// Publish `policy` in the fleet (`None` publishes static: senders then
    /// report nothing).
    pub fn publish(
        segment: &ApplicationSegment,
        application: impl Into<String>,
        policy: Option<GrowthPolicy>,
    ) -> Self {
        segment.inbox().set_growth_policy(policy);
        Self { application: application.into(), policy, growing: None }
    }

    pub fn policy(&self) -> Option<GrowthPolicy> {
        self.policy
    }

    /// The watcher reported pressure. `Some` is an episode to act on; the
    /// episode stays open until the serving receivers change, so a worker
    /// that is still starting is not asked for twice. Pressure reported
    /// under an earlier policy is ended at once.
    pub fn pressure(
        &mut self,
        segment: &ApplicationSegment,
        watcher: &mut DirectoryWatcher,
    ) -> Option<Grow> {
        let Some(policy) = self.policy else {
            watcher.take_pressure();
            return None;
        };
        let serving = segment.inbox().serving(&self.application).len();
        self.growing = Some(serving);
        Some(Grow { policy, serving })
    }

    /// The lane directory moved. An open episode ends once the receivers
    /// serving the application differ from when it opened: the new worker's
    /// lane arrived, or one left. Callers joining do not end it.
    pub fn directory_changed(
        &mut self,
        segment: &ApplicationSegment,
        watcher: &mut DirectoryWatcher,
    ) {
        if let Some(before) = self.growing
            && segment.inbox().serving(&self.application).len() != before
        {
            watcher.take_pressure();
            self.growing = None;
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/link/application/growth.rs"]
mod tests;
