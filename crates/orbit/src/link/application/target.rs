//! A prepared application destination, shared by a route's request handlers.

use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};

use super::selection::Selection;
use super::{
    ApplicationClient, Exchange, ExchangeBody, HttpPending, Inbox, LinkBalance,
    Pending, Result,
};

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
    selection: Selection,
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
        sender: usize,
    ) -> Self {
        // Stamp before scanning. A concurrent directory change must make
        // this snapshot stale, never label an older scan with a newer epoch.
        let generation = inbox.directory();
        let lanes =
            super::client::rendezvous_order(sender, inbox.serving(application));
        Self { generation, lanes }
    }
}

impl Lanes {
    fn new(
        inbox: &Inbox,
        application: &str,
        sender: usize,
    ) -> Self {
        Self(ArcSwap::from_pointee(Snapshot::read(inbox, application, sender)))
    }

    fn load(
        &self,
        inbox: &Inbox,
        application: &str,
        sender: usize,
    ) -> Guard<Arc<Snapshot>> {
        let current = self.0.load();
        if current.generation == inbox.directory() {
            return current;
        }
        let next = Arc::new(Snapshot::read(inbox, application, sender));
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
        balance: LinkBalance,
    ) -> Self {
        let lanes = Lanes::new(client.inbox(), &application, client.lane());
        let selection = Selection::new(balance, client.lane());
        Self(Arc::new(Prepared { client, application, lanes, selection }))
    }

    pub fn application(&self) -> &str {
        &self.0.application
    }

    /// Bytes one direction of a body stream holds in this destination, as its
    /// owner declared them: one window of a reply.
    pub fn stream_buffer_bytes(&self) -> usize {
        self.0.client.stream_buffer_bytes()
    }

    /// Commit one exchange using the current lane membership and load.
    /// Empty membership is retained only until the directory changes.
    pub fn send(
        &self,
        mut request: Exchange,
    ) -> Result<Pending> {
        let lanes = self.0.lanes.load(
            self.0.client.inbox(),
            &self.0.application,
            self.0.client.lane(),
        );
        self.0
            .client
            .send_to(
                &self.0.application,
                &lanes.lanes,
                &mut request,
                Some(&self.0.selection),
                false,
            )
            .map(HttpPending::byte_stream)
    }

    /// Send, waiting only for this worker's own creator stream capacity. A
    /// receiver's capacity is never waited for here: a bounded receiver
    /// queues past what it runs, and when every window is full the exchange
    /// is refused at once (`CapacityFull`). Dropping this future cancels only
    /// the unsubmitted exchange. The caller owns any waiting budget; this
    /// method adds no timer or retry interval.
    pub async fn send_wait(
        &self,
        request: Exchange,
    ) -> Result<Pending> {
        self.dispatch_wait(request, false).await.map(HttpPending::byte_stream)
    }

    /// Native HTTP callers may use the response transport advertised by
    /// the selected runner. Streamed requests still use the byte ring.
    pub async fn send_http_wait(
        &self,
        request: Exchange,
    ) -> Result<HttpPending> {
        let eligible =
            !request.upgrade && !matches!(request.body, ExchangeBody::Stream);
        self.dispatch_wait(request, eligible).await
    }

    async fn dispatch_wait(
        &self,
        mut request: Exchange,
        allow_shared_response: bool,
    ) -> Result<HttpPending> {
        let mut creator_pressure = None;
        loop {
            let result = if let Some(shared_response) = creator_pressure {
                let turn = self
                    .wait_creator_turn(shared_response, allow_shared_response)
                    .await?;
                let lanes = self.0.lanes.load(
                    self.0.client.inbox(),
                    &self.0.application,
                    self.0.client.lane(),
                );
                self.0.client.send_with_creator_turn(
                    &self.0.application,
                    &lanes.lanes,
                    &mut request,
                    turn,
                    &self.0.selection,
                    allow_shared_response,
                )
            } else {
                let lanes = self.0.lanes.load(
                    self.0.client.inbox(),
                    &self.0.application,
                    self.0.client.lane(),
                );
                self.0.client.send_to(
                    &self.0.application,
                    &lanes.lanes,
                    &mut request,
                    Some(&self.0.selection),
                    allow_shared_response,
                )
            };
            match result {
                Err(super::Error::Stream(orbitive::stream::Error::Full {
                    ..
                })) => creator_pressure = Some(Some(false)),
                Err(super::Error::Stream(
                    orbitive::stream::Error::WouldBlock,
                )) => {
                    let lanes = self.0.lanes.load(
                        self.0.client.inbox(),
                        &self.0.application,
                        self.0.client.lane(),
                    );
                    let mixed = allow_shared_response
                        && lanes.lanes.iter().any(|&lane| {
                            self.0
                                .client
                                .inbox()
                                .paired_bodies_epoch(lane)
                                .is_some()
                        });
                    creator_pressure =
                        Some(if mixed { None } else { Some(false) });
                }
                Err(super::Error::SharedResponseFull) => {
                    creator_pressure = Some(Some(true))
                }
                // Sent, refused because every window is full, or gone: a
                // sender never waits for receiver capacity. A full window is
                // answered at once; a receiver's queue is where waiting
                // happens.
                result => return result,
            }
        }
    }

    fn check_destination(&self) -> Result<()> {
        self.0.client.check_open(&self.0.application)?;
        let current = self.0.lanes.load(
            self.0.client.inbox(),
            &self.0.application,
            self.0.client.lane(),
        );
        if current.lanes.is_empty() {
            return Err(super::Error::NoConsumer(
                self.0.application.to_string(),
            ));
        }
        Ok(())
    }

    async fn wait_creator_turn(
        &self,
        shared_response: Option<bool>,
        allow_shared_response: bool,
    ) -> Result<super::creator::CreatorTurn<'_>> {
        // Keep the same mutex future across membership wakes to retain FIFO
        // position. Queued destinations can still withdraw or close promptly.
        let acquiring = self.0.client.creator_turn();
        tokio::pin!(acquiring);
        let turn = loop {
            let changed = self.0.client.membership_changed().notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.check_destination()?;
            tokio::select! {
                turn = &mut acquiring => break turn,
                _ = changed => {},
            }
        };
        let Some(shared_response) = shared_response else {
            return Ok(turn);
        };
        loop {
            self.check_destination()?;
            if self.0.client.creator_available(shared_response) {
                return Ok(turn);
            }
            // Local return alone cannot prove remote release. Observe both,
            // then recheck the real slot state before parking without resources.
            let returned = self.0.client.streams_returned().notified();
            let changed = self.0.client.membership_changed().notified();
            tokio::pin!(returned, changed);
            returned.as_mut().enable();
            changed.as_mut().enable();
            let released =
                self.0.client.stream_capacity_changed(shared_response);
            self.check_destination()?;
            if self.0.client.creator_available(shared_response) {
                return Ok(turn);
            }
            tokio::select! {
                _ = returned => {},
                _ = changed => {
                    if allow_shared_response {
                        return Ok(turn);
                    }
                },
                result = released => result.map_err(super::Error::Stream)?,
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/link/application/target.rs"]
mod tests;
