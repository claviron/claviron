//! A prepared application destination, shared by a route's request handlers.

use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};

use super::{Exchange, Inbox, Pending, Result, TransportClient};

/// One application in an already joined destination fleet.
///
/// Prepare this when building a route. Sending does not look up the fleet
/// client or application name in a map. Lane membership is refreshed only
/// when Link's directory generation changes; admission and depth remain live.
#[derive(Clone)]
pub struct TransportTarget(Arc<Prepared>);

struct Prepared {
    client: Arc<TransportClient>,
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

impl TransportTarget {
    pub(super) fn new(
        client: Arc<TransportClient>,
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
        request: Exchange,
        body_follows: bool,
    ) -> Result<Pending> {
        let lanes =
            self.0.lanes.load(self.0.client.inbox(), &self.0.application);
        self.0.client.send_to(
            &self.0.application,
            &lanes.lanes,
            request,
            body_follows,
        )
    }
}

#[cfg(test)]
#[path = "../../../tests/link/transport/target.rs"]
mod tests;
