//! Groups and addressed boxes, fleet-wide: the engine under Django Channels'
//! channel layer, with nothing of Django in it.
//!
//! A *box* is one receiver's address — a Channels consumer's channel. It lives
//! in the process that opened it, and its name says which one
//! (`{prefix}.n{node}!{seq}`), so a message for it is put in directly when it
//! is here and published on the fleet when it is not, for its owner alone to
//! take. A *group* is a name boxes join. No process holds the whole of a
//! group: each knows which of its own boxes joined which group, and a group
//! message is delivered to the local members at once and published on the
//! fleet once, for every other process to deliver to theirs — the place a
//! shared table (Redis, for `channels_redis`) would otherwise sit.
//!
//! Messages are bytes; what they encode is the caller's. A box holds at most
//! its capacity: a direct send past it is refused, a group send passes the
//! full box over, as `channels_redis` does. A message older than the box's
//! expiry is dropped when it is reached; nothing runs a timer for it. Both
//! come from the caller's configuration, and unwritten means no limit.
//!
//! Only process-specific boxes are served. A Channels name without `!` — a
//! plain channel shared by workers — is refused rather than half served.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::Notify;

use crate::fleet::{FRAME_KIND_GROUPS, FleetFanout};

const OP_SEND: u8 = 1;
const OP_GROUP_SEND: u8 = 2;

static BOX_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Called once, by whoever puts the next message in an empty box a receiver
/// is waiting on — on the putting thread, so it should only ring something.
pub type Wake = Box<dyn FnOnce() + Send>;

/// Why a group operation was refused.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GroupError {
    #[error("the channel is full")]
    Full,
    #[error("no such channel in this process")]
    UnknownChannel,
    #[error("only process-specific channels (with '!') are served")]
    NotProcessSpecific,
    #[error("the broadcast service is not running")]
    NotRunning,
    #[error("fleet publication failed: {0}")]
    Fleet(String),
}

/// One receiver's box.
struct Mailbox {
    queue: Mutex<VecDeque<(Instant, Bytes)>>,
    capacity: Option<usize>,
    expiry: Option<Duration>,
    arrived: Notify,
    /// A receiver that waits on its own wake-up rather than on `arrived`.
    wake: Mutex<Option<Wake>>,
}

impl Mailbox {
    /// Put one message in, refused when full.
    fn put(
        &self,
        message: Bytes,
    ) -> Result<(), GroupError> {
        let mut queue = self.queue.lock().expect("mailbox lock poisoned");
        self.drop_expired(&mut queue);
        if self.capacity.is_some_and(|capacity| queue.len() >= capacity) {
            return Err(GroupError::Full);
        }
        queue.push_back((Instant::now(), message));
        drop(queue);
        self.arrived.notify_one();
        if let Some(wake) =
            self.wake.lock().expect("mailbox lock poisoned").take()
        {
            wake();
        }
        Ok(())
    }

    /// The next message if there is one; otherwise `wake` is kept for the
    /// next put, in place of any earlier one. Checked and kept under the
    /// queue's lock, so a message cannot arrive between the two unseen.
    fn take_or_wake(
        &self,
        wake: Wake,
    ) -> Option<Bytes> {
        let mut queue = self.queue.lock().expect("mailbox lock poisoned");
        self.drop_expired(&mut queue);
        if let Some((_, message)) = queue.pop_front() {
            return Some(message);
        }
        *self.wake.lock().expect("mailbox lock poisoned") = Some(wake);
        None
    }

    fn drop_expired(
        &self,
        queue: &mut VecDeque<(Instant, Bytes)>,
    ) {
        if let Some(expiry) = self.expiry {
            while queue.front().is_some_and(|(at, _)| at.elapsed() >= expiry) {
                queue.pop_front();
            }
        }
    }

    /// The next message, waiting for one to arrive.
    async fn take(&self) -> Bytes {
        loop {
            let arrived = self.arrived.notified();
            tokio::pin!(arrived);
            arrived.as_mut().enable();
            {
                let mut queue =
                    self.queue.lock().expect("mailbox lock poisoned");
                self.drop_expired(&mut queue);
                if let Some((_, message)) = queue.pop_front() {
                    return message;
                }
            }
            arrived.await;
        }
    }
}

/// This process's boxes and its boxes' group memberships.
#[derive(Default)]
pub(crate) struct Groups {
    node: u16,
    boxes: Mutex<HashMap<Arc<str>, Arc<Mailbox>>>,
    members: Mutex<HashMap<String, HashSet<Arc<str>>>>,
}

impl Groups {
    pub(crate) fn new(node: u16) -> Self {
        Self { node, ..Self::default() }
    }

    /// Open a box: its name, which the caller hands out as the address.
    pub(crate) fn open(
        &self,
        prefix: &str,
        capacity: Option<usize>,
        expiry: Option<Duration>,
    ) -> String {
        let name = format!(
            "{prefix}.n{}!{}",
            self.node,
            BOX_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let mailbox = Mailbox {
            queue: Mutex::new(VecDeque::new()),
            capacity,
            expiry,
            arrived: Notify::new(),
            wake: Mutex::new(None),
        };
        self.boxes
            .lock()
            .expect("boxes lock poisoned")
            .insert(Arc::from(name.as_str()), Arc::new(mailbox));
        name
    }

    /// Forget a box and its memberships.
    pub(crate) fn close(
        &self,
        channel: &str,
    ) {
        self.boxes.lock().expect("boxes lock poisoned").remove(channel);
        let mut members = self.members.lock().expect("members lock poisoned");
        members.retain(|_, channels| {
            channels.remove(channel);
            !channels.is_empty()
        });
    }

    fn mailbox(
        &self,
        channel: &str,
    ) -> Option<Arc<Mailbox>> {
        self.boxes.lock().expect("boxes lock poisoned").get(channel).cloned()
    }

    /// The node a process-specific channel belongs to.
    fn owner(channel: &str) -> Result<u16, GroupError> {
        let (head, _) =
            channel.split_once('!').ok_or(GroupError::NotProcessSpecific)?;
        let (_, node) =
            head.rsplit_once(".n").ok_or(GroupError::NotProcessSpecific)?;
        node.parse().map_err(|_| GroupError::NotProcessSpecific)
    }

    /// Send to one box: put in here, or published for its owner.
    pub(crate) fn send(
        &self,
        fleet: Option<&FleetFanout>,
        channel: &str,
        message: Bytes,
    ) -> Result<(), GroupError> {
        let owner = Self::owner(channel)?;
        if owner == self.node {
            return self
                .mailbox(channel)
                .ok_or(GroupError::UnknownChannel)?
                .put(message);
        }
        let fleet = fleet.ok_or(GroupError::UnknownChannel)?;
        publish(fleet, OP_SEND, channel, &message)
    }

    /// The next message for a box of this process.
    pub(crate) async fn receive(
        &self,
        channel: &str,
    ) -> Result<Bytes, GroupError> {
        let mailbox =
            self.mailbox(channel).ok_or(GroupError::UnknownChannel)?;
        Ok(mailbox.take().await)
    }

    /// The next message for a box of this process without waiting, or
    /// `None` with `wake` kept for its arrival.
    pub(crate) fn receive_or_wake(
        &self,
        channel: &str,
        wake: Wake,
    ) -> Result<Option<Bytes>, GroupError> {
        let mailbox =
            self.mailbox(channel).ok_or(GroupError::UnknownChannel)?;
        Ok(mailbox.take_or_wake(wake))
    }

    /// Join a box of this process to a group.
    pub(crate) fn add(
        &self,
        group: &str,
        channel: &str,
    ) -> Result<(), GroupError> {
        if Self::owner(channel)? != self.node || self.mailbox(channel).is_none()
        {
            return Err(GroupError::UnknownChannel);
        }
        self.members
            .lock()
            .expect("members lock poisoned")
            .entry(group.to_owned())
            .or_default()
            .insert(Arc::from(channel));
        Ok(())
    }

    pub(crate) fn discard(
        &self,
        group: &str,
        channel: &str,
    ) {
        let mut members = self.members.lock().expect("members lock poisoned");
        if let Some(channels) = members.get_mut(group) {
            channels.remove(channel);
            if channels.is_empty() {
                members.remove(group);
            }
        }
    }

    /// Send to a group: this process's members now, the fleet's through one
    /// publication. The count is this process's deliveries.
    pub(crate) fn group_send(
        &self,
        fleet: Option<&FleetFanout>,
        group: &str,
        message: Bytes,
    ) -> Result<usize, GroupError> {
        let delivered = self.deliver_group(group, &message);
        if let Some(fleet) = fleet {
            publish(fleet, OP_GROUP_SEND, group, &message)?;
        }
        Ok(delivered)
    }

    fn deliver_group(
        &self,
        group: &str,
        message: &Bytes,
    ) -> usize {
        let channels: Vec<Arc<str>> = self
            .members
            .lock()
            .expect("members lock poisoned")
            .get(group)
            .map(|channels| channels.iter().cloned().collect())
            .unwrap_or_default();
        channels
            .iter()
            .filter_map(|channel| self.mailbox(channel))
            .filter(|mailbox| mailbox.put(message.clone()).is_ok())
            .count()
    }

    /// A publication from another process.
    pub(crate) fn deliver(
        &self,
        payload: &[u8],
    ) {
        let Some((op, target, message)) = decode(payload) else {
            tracing::warn!("ignored malformed group publication");
            return;
        };
        match op {
            OP_SEND => {
                if Self::owner(target).ok() == Some(self.node)
                    && let Some(mailbox) = self.mailbox(target)
                {
                    let _ = mailbox.put(message);
                }
            }
            OP_GROUP_SEND => {
                self.deliver_group(target, &message);
            }
            _ => tracing::warn!(
                op,
                "ignored group publication of an unknown kind"
            ),
        }
    }
}

fn publish(
    fleet: &FleetFanout,
    op: u8,
    target: &str,
    message: &[u8],
) -> Result<(), GroupError> {
    let target_len = u16::try_from(target.len())
        .map_err(|_| GroupError::Fleet("name too long".to_owned()))?;
    let mut payload = BytesMut::with_capacity(3 + target.len() + message.len());
    payload.put_u8(op);
    payload.put_u16_le(target_len);
    payload.put_slice(target.as_bytes());
    payload.put_slice(message);
    fleet
        .publish_bytes(FRAME_KIND_GROUPS, &payload)
        .map_err(|error| GroupError::Fleet(error.to_string()))
}

fn decode(payload: &[u8]) -> Option<(u8, &str, Bytes)> {
    let op = *payload.first()?;
    let len = u16::from_le_bytes(payload.get(1..3)?.try_into().ok()?) as usize;
    let target = std::str::from_utf8(payload.get(3..3 + len)?).ok()?;
    Some((op, target, Bytes::copy_from_slice(&payload[3 + len..])))
}

#[cfg(test)]
#[path = "../tests/groups.rs"]
mod tests;
