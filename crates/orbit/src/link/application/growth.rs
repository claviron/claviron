//! The owner's side of worker growth, shared by every owner of a fleet: a
//! runner's master with its supervisor, or a task service running the
//! fleet's processes. The owner publishes its policy; a sender that finds no
//! receiver to run its exchange now reports pressure once per episode; the
//! owner adds one worker and ends the episode once that worker arrived, or
//! once it knows it will not.
//!
//! What arriving means is the owner's: a runner's master counts the
//! receivers serving its application, a task service matches the lanes to
//! the processes it started. Adding the worker is the owner's own action.

use super::{ApplicationSegment, DirectoryWatcher, GrowthPolicy};

/// An owner's growth state for one fleet.
pub struct FleetGrowth {
    policy: Option<GrowthPolicy>,
    /// Whether an episode is open: a worker was asked for and has not
    /// arrived yet.
    growing: bool,
}

impl FleetGrowth {
    /// Publish `policy` in the fleet (`None` publishes static: senders then
    /// report nothing).
    pub fn publish(
        segment: &ApplicationSegment,
        policy: Option<GrowthPolicy>,
    ) -> Self {
        segment.inbox().set_growth_policy(policy);
        Self { policy, growing: false }
    }

    pub fn policy(&self) -> Option<GrowthPolicy> {
        self.policy
    }

    /// Publish another policy, such as `None` once the owner can add no
    /// more workers. An open episode stays open until [`Self::arrived`].
    pub fn set_policy(
        &mut self,
        segment: &ApplicationSegment,
        policy: Option<GrowthPolicy>,
    ) {
        segment.inbox().set_growth_policy(policy);
        self.policy = policy;
    }

    /// The watcher reported pressure. `Some` is an episode to act on: add
    /// one worker within the policy. The episode stays open until
    /// [`Self::arrived`], so a worker that is still starting is not asked
    /// for twice. Pressure reported under no policy is ended at once.
    pub fn pressure(
        &mut self,
        watcher: &mut DirectoryWatcher,
    ) -> Option<GrowthPolicy> {
        let Some(policy) = self.policy else {
            watcher.take_pressure();
            return None;
        };
        if self.growing {
            return None;
        }
        self.growing = true;
        Some(policy)
    }

    /// Whether a worker was asked for and has not arrived yet.
    pub fn growing(&self) -> bool {
        self.growing
    }

    /// The worker asked for arrived, or will not: end the episode, so the
    /// next report opens another.
    pub fn arrived(
        &mut self,
        watcher: &mut DirectoryWatcher,
    ) {
        if self.growing {
            watcher.take_pressure();
            self.growing = false;
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/link/application/growth.rs"]
mod tests;
