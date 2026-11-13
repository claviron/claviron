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
    segment: Arc<ApplicationSegment>,
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
        let recovery = if segment.inbox().inline_exchange_epoch(lane).is_some()
            || segment
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
                segment: Arc::clone(&segment),
                exits: JoinSet::new(),
            };
            recovery.refresh();
            Some(recovery)
        } else {
            None
        };
        Ok(Self { reader, segment, recovery })
    }

    /// Keep reply-space and peer notifications moving while accepted work
    /// drains, leaving unread frames to lane recovery.
    pub async fn drain_notification(&mut self) -> Result<()> {
        self.receive(None).await.map(|_| ())
    }

    /// Copy the next frame or report a directory/capacity notification.
    /// Cancelling this future leaves unread frames and reservations intact.
    pub async fn next(
        &mut self,
        frame: &mut Vec<u8>,
    ) -> Result<Received> {
        self.receive(Some(frame)).await
    }

    async fn receive(
        &mut self,
        mut frame: Option<&mut Vec<u8>>,
    ) -> Result<Received> {
        loop {
            let read = async {
                match frame.as_deref_mut() {
                    Some(frame) => self.reader.next(frame).await,
                    None => self.reader.next_notification().await,
                }
                .map_err(map_link_error)
            };
            let received = if let Some(recovery) = self.recovery.as_mut() {
                tokio::select! {
                    biased;
                    Some(finished) = recovery.exits.join_next(), if !recovery.exits.is_empty() => {
                        finished.map_err(|_| super::Error::Malformed("Link origin observer failed"))??;
                        continue;
                    },
                    received = read => received?,
                }
            } else {
                read.await?
            };
            if matches!(
                received,
                Received::Directory(_) | Received::Capacity(_)
            ) {
                self.segment.notify_reply_space();
            }
            if matches!(received, Received::Directory(_))
                && let Some(recovery) = self.recovery.as_mut()
            {
                recovery.refresh();
            }
            return Ok(received);
        }
    }
}
