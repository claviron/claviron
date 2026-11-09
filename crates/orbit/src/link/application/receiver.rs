//! Shared application intake: frame readiness and exact origin-owner recovery.
use std::sync::Arc;

use orbitive::link::InboxReceiver;
use tokio::task::JoinSet;

use super::{
    ApplicationSegment, EDGE_RUNTIME, Peers, Received, Result, map_link_error,
};
use claviron_runtime::FleetName;

/// The sole reader of an application lane. Capacity never gates this reader.
/// Bounded receivers also observe caller process exit, so an unpublished
/// reservation cannot survive its originating owner. No polling is used.
pub struct ApplicationReceiver {
    reader: InboxReceiver,
    recovery: Option<Recovery>,
}

struct Recovery {
    segment: Arc<ApplicationSegment>,
    peers: Peers,
    exits: JoinSet<Result<()>>,
}

impl Recovery {
    fn refresh(&mut self) {
        let (came, _) = self.peers.update(self.segment.inbox());
        for peer in came {
            let segment = Arc::clone(&self.segment);
            self.exits.spawn(async move {
                segment.recover_on_exit(&peer).await.map(|_| ())
            });
        }
    }
}

impl ApplicationReceiver {
    pub(super) fn new(
        segment: Arc<ApplicationSegment>,
        lane: usize,
    ) -> Result<Self> {
        let reader = segment.receiver(lane)?;
        let recovery = if segment
            .inbox()
            .work_capacity(lane)
            .map_err(map_link_error)?
            .is_some()
        {
            let mut recovery = Recovery {
                peers: Peers::new(
                    FleetName::new(&segment.fleet_name),
                    |role| role == EDGE_RUNTIME,
                ),
                segment,
                exits: JoinSet::new(),
            };
            recovery.refresh();
            Some(recovery)
        } else {
            None
        };
        Ok(Self { reader, recovery })
    }

    /// Copy the next frame or report a directory/capacity notification.
    /// Cancelling this future leaves unread frames and reservations intact.
    pub async fn next(
        &mut self,
        frame: &mut Vec<u8>,
    ) -> Result<Received> {
        let Some(recovery) = self.recovery.as_mut() else {
            return self.reader.next(frame).await.map_err(map_link_error);
        };
        loop {
            tokio::select! {
                biased;
                Some(finished) = recovery.exits.join_next(), if !recovery.exits.is_empty() => {
                    finished.map_err(|_| super::Error::Malformed("Link origin observer failed"))??;
                },
                received = self.reader.next(frame) => {
                    let received = received.map_err(map_link_error)?;
                    if matches!(received, Received::Directory(_)) { recovery.refresh(); }
                    return Ok(received);
                }
            }
        }
    }
}
