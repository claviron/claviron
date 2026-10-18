use bytes::{BufMut, Bytes, BytesMut};
use claviron_pusher::{PresenceMember, PublishedEvent};
use orbitive::fleet::FleetLaneCursor;
use orbitive::{Fleet, NetId64, OrbitTyped, RingSpec};
use serde::{Deserialize, Serialize};
use tracing::warn;

const PUBLICATION_RING_KIND: u8 = 236;
const PAYLOAD_RING_KIND: u8 = 237;
/// Which consumer a fleet publication is for. One ring carries every kind:
/// a new consumer is a new frame kind, not a new segment, and each reader
/// takes its own and passes over the rest.
pub(crate) const FRAME_KIND_PUSHER: u8 = 1;
/// Django Channels' layer ([`crate::groups`]).
pub(crate) const FRAME_KIND_GROUPS: u8 = 2;
const FRAME_KIND_PAYLOAD: u8 = 1;
const PROTOCOL_VERSION: u8 = 1;
const PAYLOAD_CHUNK_BYTES: usize = 4 * 1024;
const PAYLOAD_CHUNKS: usize = 32;
const MAX_PUBLICATION_BYTES: usize = PAYLOAD_CHUNK_BYTES * PAYLOAD_CHUNKS;
const DESCRIPTOR_BYTES: usize = 1 + 8 + 2 + 4;

#[derive(Clone)]
pub(crate) struct PublicationRecord;

impl OrbitTyped for PublicationRecord {
    const KIND: u8 = PUBLICATION_RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(32, DESCRIPTOR_BYTES);
}

#[derive(Clone)]
pub(crate) struct PayloadRecord;

impl OrbitTyped for PayloadRecord {
    const KIND: u8 = PAYLOAD_RING_KIND;
    const RING_SPEC: RingSpec =
        RingSpec::per_node(PAYLOAD_CHUNKS, PAYLOAD_CHUNK_BYTES);
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum FleetPublication {
    Event {
        application: String,
        event: PublishedEvent,
    },
    PresenceJoined {
        application: String,
        channel: String,
        socket_id: String,
        member: PresenceMember,
    },
    PresenceLeft {
        application: String,
        channel: String,
        socket_id: String,
    },
    PresenceReset,
    SubscriptionJoined {
        app_id: String,
        channel: String,
        socket_id: String,
        member: Option<PresenceMember>,
    },
    SubscriptionLeft {
        app_id: String,
        channel: String,
        socket_id: String,
    },
    SubscriptionReset,
}

pub(crate) struct FleetInbound {
    pub source_node: u16,
    pub publication: FleetPublication,
}

pub(crate) struct FleetFanout {
    fleet: std::sync::Arc<Fleet>,
}

impl FleetFanout {
    pub(crate) fn new(fleet: std::sync::Arc<Fleet>) -> Self {
        Self { fleet }
    }

    pub(crate) fn cursor_at_head(&self) -> FleetLaneCursor {
        self.fleet.lane_cursor_at_head::<PublicationRecord>()
    }

    pub(crate) fn node_id(&self) -> u16 {
        self.fleet.node_id().get()
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    ))]
    pub(crate) fn event_fd(
        &self
    ) -> std::io::Result<orbitive::core::RingEventFd> {
        self.fleet.ring_event_fd::<PublicationRecord>()
    }

    pub(crate) fn publish(
        &self,
        application: &str,
        event: &PublishedEvent,
    ) -> crate::Result<()> {
        self.publish_publication(&FleetPublication::Event {
            application: application.to_owned(),
            event: event.clone(),
        })
    }

    pub(crate) fn publish_presence_joined(
        &self,
        application: &str,
        channel: &str,
        socket_id: &str,
        member: &PresenceMember,
    ) -> crate::Result<()> {
        self.publish_publication(&FleetPublication::PresenceJoined {
            application: application.to_owned(),
            channel: channel.to_owned(),
            socket_id: socket_id.to_owned(),
            member: member.clone(),
        })
    }

    pub(crate) fn publish_presence_left(
        &self,
        application: &str,
        channel: &str,
        socket_id: &str,
    ) -> crate::Result<()> {
        self.publish_publication(&FleetPublication::PresenceLeft {
            application: application.to_owned(),
            channel: channel.to_owned(),
            socket_id: socket_id.to_owned(),
        })
    }

    pub(crate) fn publish_presence_reset(&self) -> crate::Result<()> {
        self.publish_publication(&FleetPublication::PresenceReset)
    }

    pub(crate) fn publish_subscription_joined(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &str,
        member: Option<&PresenceMember>,
    ) -> crate::Result<()> {
        self.publish_publication(&FleetPublication::SubscriptionJoined {
            app_id: app_id.to_owned(),
            channel: channel.to_owned(),
            socket_id: socket_id.to_owned(),
            member: member.cloned(),
        })
    }

    pub(crate) fn publish_subscription_left(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &str,
    ) -> crate::Result<()> {
        self.publish_publication(&FleetPublication::SubscriptionLeft {
            app_id: app_id.to_owned(),
            channel: channel.to_owned(),
            socket_id: socket_id.to_owned(),
        })
    }

    pub(crate) fn publish_subscription_reset(&self) -> crate::Result<()> {
        self.publish_publication(&FleetPublication::SubscriptionReset)
    }

    fn publish_publication(
        &self,
        publication: &FleetPublication,
    ) -> crate::Result<()> {
        self.publish_bytes(FRAME_KIND_PUSHER, &serde_json::to_vec(publication)?)
    }

    /// Publish `payload` to every process in the fleet under `frame_kind`:
    /// cut into the payload ring's chunks, announced by one descriptor on the
    /// publication ring, and the readers woken. The bytes are the caller's;
    /// this carries them without reading them.
    pub(crate) fn publish_bytes(
        &self,
        frame_kind: u8,
        payload: &[u8],
    ) -> crate::Result<()> {
        if payload.len() > MAX_PUBLICATION_BYTES {
            return Err(crate::Error::Fleet(format!(
                "fleet publication is {} bytes; maximum is {MAX_PUBLICATION_BYTES}",
                payload.len()
            )));
        }

        let chunks = payload
            .chunks(PAYLOAD_CHUNK_BYTES)
            .map(Bytes::copy_from_slice)
            .collect::<Vec<_>>();
        let ids = self.fleet.publish_batch::<PayloadRecord>(
            FRAME_KIND_PAYLOAD,
            0,
            chunks,
        );
        let first = ids.first().copied().ok_or_else(|| {
            crate::Error::Fleet(
                "fleet publication encoded no payload".to_owned(),
            )
        })?;
        let descriptor = encode_descriptor(first, ids.len(), payload.len())?;

        #[cfg(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "macos"
        ))]
        self.fleet
            .publish_notified::<PublicationRecord>(
                frame_kind,
                PROTOCOL_VERSION as u64,
                descriptor,
            )
            .map_err(|error| crate::Error::Fleet(error.to_string()))?;
        #[cfg(not(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "macos"
        )))]
        self.fleet.publish::<PublicationRecord>(
            frame_kind,
            PROTOCOL_VERSION as u64,
            descriptor,
        );
        Ok(())
    }

    pub(crate) fn poll(
        &self,
        cursor: &mut FleetLaneCursor,
    ) -> Vec<FleetInbound> {
        self.poll_with_local(cursor, false)
    }

    pub(crate) fn poll_including_local(
        &self,
        cursor: &mut FleetLaneCursor,
    ) -> Vec<FleetInbound> {
        self.poll_with_local(cursor, true)
    }

    fn poll_with_local(
        &self,
        cursor: &mut FleetLaneCursor,
        include_local: bool,
    ) -> Vec<FleetInbound> {
        self.poll_frames(cursor, FRAME_KIND_PUSHER, include_local)
            .into_iter()
            .filter_map(|(source_node, payload)| {
                serde_json::from_slice::<FleetPublication>(&payload)
                    .map_err(|error| warn!(%error, "ignored malformed Broadcast fleet publication"))
                    .ok()
                    .map(|publication| FleetInbound { source_node, publication })
            })
            .collect()
    }

    /// Every publication since `cursor` from the other processes, of any kind,
    /// for a reader that serves more than one: one pass over the ring, and
    /// the caller dispatches by kind.
    pub(crate) fn poll_all(
        &self,
        cursor: &mut FleetLaneCursor,
    ) -> Vec<(u8, u16, Vec<u8>)> {
        let poll = self.fleet.poll_lanes::<PublicationRecord>(cursor);
        if poll.loss.overwritten > 0 || poll.loss.unavailable > 0 {
            warn!(
                overwritten = poll.loss.overwritten,
                unavailable = poll.loss.unavailable,
                "Broadcast fleet publication ring lagged"
            );
        }
        poll.frames
            .into_iter()
            .filter(|frame| frame.id.node() != self.fleet.node_id().get())
            .filter_map(|frame| {
                let (kind, source_node) = (frame.kind, frame.id.node());
                self.read_payload(frame)
                    .map(|payload| (kind, source_node, payload))
            })
            .collect()
    }

    /// A Pusher publication's bytes, decoded.
    pub(crate) fn decode_pusher(payload: &[u8]) -> Option<FleetPublication> {
        serde_json::from_slice::<FleetPublication>(payload)
            .map_err(|error| warn!(%error, "ignored malformed Broadcast fleet publication"))
            .ok()
    }

    /// Every publication of `frame_kind` since `cursor`, as its source node
    /// and its bytes. Other kinds are other consumers' and are passed over;
    /// this process's own only when `include_local`.
    pub(crate) fn poll_frames(
        &self,
        cursor: &mut FleetLaneCursor,
        frame_kind: u8,
        include_local: bool,
    ) -> Vec<(u16, Vec<u8>)> {
        let poll = self.fleet.poll_lanes::<PublicationRecord>(cursor);
        if poll.loss.overwritten > 0 || poll.loss.unavailable > 0 {
            warn!(
                overwritten = poll.loss.overwritten,
                unavailable = poll.loss.unavailable,
                "Broadcast fleet publication ring lagged"
            );
        }

        poll.frames
            .into_iter()
            .filter(|frame| frame.kind == frame_kind)
            .filter(|frame| {
                include_local || frame.id.node() != self.fleet.node_id().get()
            })
            .filter_map(|frame| {
                let source_node = frame.id.node();
                self.read_payload(frame).map(|payload| (source_node, payload))
            })
            .collect()
    }

    /// The bytes a publication's descriptor points at, gathered from the
    /// payload ring. `None` when they are gone or do not add up.
    fn read_payload(
        &self,
        frame: orbitive::Frame,
    ) -> Option<Vec<u8>> {
        if frame.ver != PROTOCOL_VERSION as u64 {
            warn!(id = %frame.id, "ignored incompatible Broadcast fleet publication");
            return None;
        }
        let (first, chunk_count, value_len) =
            decode_descriptor(&frame.payload)?;
        if first.kind() != PAYLOAD_RING_KIND
            || first.node() != frame.id.node()
            || chunk_count == 0
            || chunk_count > PAYLOAD_CHUNKS
            || value_len > MAX_PUBLICATION_BYTES
        {
            warn!(id = %frame.id, "ignored invalid Broadcast fleet publication descriptor");
            return None;
        }

        let mut payload = Vec::with_capacity(value_len);
        for offset in 0..chunk_count as u64 {
            let id = NetId64::make(
                PAYLOAD_RING_KIND,
                first.node(),
                first.counter().checked_add(offset)?,
            );
            let chunk = self.fleet.read_lane_at::<PayloadRecord>(
                orbitive::NodeId::new(first.node()),
                id.counter(),
            )?;
            if chunk.id != id || chunk.kind != FRAME_KIND_PAYLOAD {
                return None;
            }
            payload.extend_from_slice(&chunk.payload);
            if payload.len() > value_len {
                return None;
            }
        }
        (payload.len() == value_len).then_some(payload)
    }
}

fn encode_descriptor(
    first: NetId64,
    chunk_count: usize,
    value_len: usize,
) -> crate::Result<Bytes> {
    let chunk_count = u16::try_from(chunk_count).map_err(|_| {
        crate::Error::Fleet("too many fleet payload chunks".to_owned())
    })?;
    let value_len = u32::try_from(value_len).map_err(|_| {
        crate::Error::Fleet("fleet payload is too large".to_owned())
    })?;
    let mut out = BytesMut::with_capacity(DESCRIPTOR_BYTES);
    out.put_u8(PROTOCOL_VERSION);
    out.put_u64_le(first.raw());
    out.put_u16_le(chunk_count);
    out.put_u32_le(value_len);
    Ok(out.freeze())
}

fn decode_descriptor(bytes: &[u8]) -> Option<(NetId64, usize, usize)> {
    if bytes.len() != DESCRIPTOR_BYTES || bytes[0] != PROTOCOL_VERSION {
        return None;
    }
    let first =
        NetId64::from_raw(u64::from_le_bytes(bytes[1..9].try_into().ok()?));
    let chunks = u16::from_le_bytes(bytes[9..11].try_into().ok()?) as usize;
    let len = u32::from_le_bytes(bytes[11..15].try_into().ok()?) as usize;
    Some((first, chunks, len))
}

#[cfg(all(test, unix))]
#[path = "../tests/fleet.rs"]
mod tests;
