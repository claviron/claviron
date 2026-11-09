//! A prepared application destination, shared by a route's request handlers.

use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};

use super::{ApplicationClient, Exchange, Inbox, Pending, Result};

/// One application in an already joined destination fleet.
///
/// Prepare this when building a route. Sending does not look up the fleet
/// client or application name in a map. Lane membership is refreshed only
/// when Link's directory generation changes; admission and depth remain live.
#[derive(Clone)]
pub struct ApplicationTarget(Arc<Prepared>);

struct Prepared {
    client: Arc<ApplicationClient>,
    application: Arc<str>,
    lanes: Lanes,
}

struct Lanes(ArcSwap<Snapshot>);

struct Snapshot {
    generation: u64,
    lanes: Vec<usize>,
}

impl Snapshot {
    fn read(
        inbox: &Inbox,
        application: &str,
    ) -> Self {
        // Stamp before scanning. A concurrent directory change must make
        // this snapshot stale, never label an older scan with a newer epoch.
        let generation = inbox.directory();
        Self { generation, lanes: inbox.serving(application) }
    }
}

impl Lanes {
    fn new(
        inbox: &Inbox,
        application: &str,
    ) -> Self {
        Self(ArcSwap::from_pointee(Snapshot::read(inbox, application)))
    }

    fn load(
        &self,
        inbox: &Inbox,
        application: &str,
    ) -> Guard<Arc<Snapshot>> {
        let current = self.0.load();
        if current.generation == inbox.directory() {
            return current;
        }
        let next = Arc::new(Snapshot::read(inbox, application));
        // One publication attempt. A delayed refresher cannot overwrite a
        // snapshot another sender published after it started. No retry loop
        // or waiter is introduced when several requests observe a change.
        let previous = self.0.compare_and_swap(&*current, Arc::clone(&next));
        if Arc::ptr_eq(&previous, &current) {
            Guard::from_inner(next)
        } else {
            previous
        }
    }
}

impl ApplicationTarget {
    pub(super) fn new(
        client: Arc<ApplicationClient>,
        application: Arc<str>,
    ) -> Self {
        let lanes = Lanes::new(client.inbox(), &application);
        Self(Arc::new(Prepared { client, application, lanes }))
    }

    pub fn application(&self) -> &str {
        &self.0.application
    }

    /// Commit one exchange using the current lane membership and load.
    /// Empty membership is retained only until the directory changes.
    pub fn send(
        &self,
        mut request: Exchange,
    ) -> Result<Pending> {
        let lanes =
            self.0.lanes.load(self.0.client.inbox(), &self.0.application);
        self.0.client.send_to(&self.0.application, &lanes.lanes, &mut request)
    }

    /// Wait for receiver or creator stream capacity without holding either
    /// reservation while parked. Dropping
    /// this future cancels only the unsubmitted exchange. The caller owns
    /// any waiting budget; this method adds no timer or retry interval.
    pub async fn send_wait(
        &self,
        mut request: Exchange,
    ) -> Result<Pending> {
        loop {
            let lanes =
                self.0.lanes.load(self.0.client.inbox(), &self.0.application);
            match self.0.client.send_to(
                &self.0.application,
                &lanes.lanes,
                &mut request,
            ) {
                Err(super::Error::CapacityFull) => {}
                Err(super::Error::Stream(orbitive::stream::Error::Full {
                    ..
                })) => {
                    // A local endpoint returning and a remote peer releasing
                    // are different events. Arm both before retrying. A local
                    // directory/close notification must also interrupt this wait.
                    let returned = self.0.client.streams_returned().notified();
                    let changed = self.0.client.membership_changed().notified();
                    tokio::pin!(returned, changed);
                    returned.as_mut().enable();
                    changed.as_mut().enable();
                    let released = self.0.client.stream_capacity_changed();
                    let current = self
                        .0
                        .lanes
                        .load(self.0.client.inbox(), &self.0.application);
                    match self.0.client.send_to(
                        &self.0.application,
                        &current.lanes,
                        &mut request,
                    ) {
                        Err(super::Error::Stream(
                            orbitive::stream::Error::Full { .. },
                        )) => {
                            tokio::select! {
                                _ = returned => {},
                                _ = changed => {},
                                result = released => result.map_err(super::Error::Stream)?,
                            }
                        }
                        Err(super::Error::CapacityFull) => {}
                        result => return result,
                    }
                    continue;
                }
                result => return result,
            }
            let changed = self.0.client.capacity_changed().notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.0.client.watch_capacity(&lanes.lanes)?;
            // Register, then re-read membership and retry admission. Either
            // the retry sees room or a concurrent return wakes this wait.
            let current =
                self.0.lanes.load(self.0.client.inbox(), &self.0.application);
            match self.0.client.send_to(
                &self.0.application,
                &current.lanes,
                &mut request,
            ) {
                Err(super::Error::CapacityFull) => changed.await,
                Err(super::Error::Stream(orbitive::stream::Error::Full {
                    ..
                })) => {}
                result => return result,
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/link/application/target.rs"]
mod tests;
