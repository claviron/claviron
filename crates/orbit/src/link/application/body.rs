//! Claviron's configured body table over the generic `orbitive::link` layer.
//!
//! The HTTP exchange frame carries the ticket; `orbit-link` owns the reusable
//! duplex slot and its node/incarnation recovery. Claviron owns the kind and
//! geometry because they are part of this deployment's physical ABI.

use std::future::poll_fn;
use std::sync::Arc;

use orbitive::link::LinkBodies;
pub use orbitive::stream::Error as StreamError;
pub use orbitive::stream::exchange::{
    DEFAULT_CHUNK_BYTES, ExchangeEndpoint, FlowEvent, Receiver, ResetCode,
    Sender,
};
use orbitive::stream::exchange::{
    ExchangeSpec, ExchangeTicket, Exchanges, PayloadArenaSpec,
};
use orbitive::stream::{Endpoint, Incarnation, StreamSpec, Streams, Ticket, WriteHalf};
use orbitive::{Fleet, NodeId};
use tokio::io::AsyncWriteExt;

use super::{LINK_SPEC, Result, link_spec, map_link_error};

/// Control descriptors and the two directional arenas for paired HTTP Link.
pub const SHARED_RESPONSE_CONTROL_KIND: u8 = 178;
pub const SHARED_RESPONSE_REQUEST_KIND: u8 = 179;
pub const SHARED_RESPONSE_PAYLOAD_KIND: u8 = 177;

/// A reset published after the reply head entered the inbox.
pub const RESPONSE_BODY_RESET: u32 = 1;

pub(super) fn shared_response_spec(
    spec: orbitive::link::LinkSpec
) -> (ExchangeSpec, usize) {
    let slot_bytes = spec.streams.buffer_bytes.min(4096);
    let flow_slots = spec.streams.buffer_bytes / slot_bytes;
    (
        ExchangeSpec::directional(
            StreamSpec::new(
                SHARED_RESPONSE_CONTROL_KIND,
                spec.streams.lane_capacity,
                512,
            ),
            // The request is already in the inbox in this protocol revision.
            PayloadArenaSpec::new(SHARED_RESPONSE_REQUEST_KIND, flow_slots, 1),
            PayloadArenaSpec::new(
                SHARED_RESPONSE_PAYLOAD_KIND,
                flow_slots * spec.streams.lane_capacity,
                slot_bytes,
            ),
        ),
        flow_slots,
    )
}

pub(super) fn open_shared_response(
    name: &str,
    spec: orbitive::link::LinkSpec,
    lane: NodeId,
    incarnation: u64,
) -> Result<Exchanges> {
    let fleet = Arc::new(Fleet::join_shm_as(name, spec.fleet_capacity, lane)?);
    let (exchange, flow_slots) = shared_response_spec(spec);
    Ok(Exchanges::open(fleet, Incarnation::new(incarnation), exchange)?
        .with_flow_slots(flow_slots)?)
}

pub(super) fn recover_shared_response(
    name: &str,
    spec: orbitive::link::LinkSpec,
    node: NodeId,
    incarnation: u64,
) -> Result<()> {
    use orbitive::shm::{ShmRegion, ShmValidation, ring_segment_name};

    let (exchange, _) = shared_response_spec(spec);
    let control = ring_segment_name(name, SHARED_RESPONSE_CONTROL_KIND);
    if matches!(
        ShmRegion::validate_existing(
            &control,
            orbitive::stream::segment_size_for(
                spec.fleet_capacity,
                exchange.control
            )
        )
        .map_err(orbitive::core::Error::Io)?,
        ShmValidation::Missing,
    ) {
        return Ok(());
    }
    open_shared_response(name, spec, node, incarnation)?
        .node_dead(node, Incarnation::new(incarnation));
    Ok(())
}

/// This process's handle on the destination fleet's duplex body table.
pub struct ApplicationBodies {
    link: LinkBodies,
    shared_response: Option<Exchanges>,
}

impl ApplicationBodies {
    /// Inherit the inbox's access and geometry; never reopen a group Link in
    /// the caller's private UID namespace by accident.
    pub fn for_segment(
        segment: &super::ApplicationSegment,
        lane_index: usize,
        incarnation: u64,
    ) -> Result<Self> {
        let link = segment
            .link()
            .bodies(lane_index, incarnation)
            .map_err(map_link_error)?;
        Ok(Self { link, shared_response: None })
    }
    pub fn open(
        fleet: Arc<Fleet>,
        incarnation: u64,
    ) -> Result<Self> {
        let spec = link_spec(fleet.fleet_capacity());
        let link = LinkBodies::open(fleet, incarnation, spec)
            .map_err(map_link_error)?;
        Ok(Self { link, shared_response: None })
    }

    pub fn for_fleet(
        fleet_name: &str,
        lane_index: usize,
        incarnation: u64,
    ) -> Result<Self> {
        let link = LinkBodies::for_name(
            fleet_name,
            lane_index,
            incarnation,
            LINK_SPEC,
        )
        .map_err(map_link_error)?;
        Ok(Self { link, shared_response: None })
    }

    /// Prepare the HTTP response arena before advertising its ticket format.
    pub fn prepare_http(
        &mut self,
        segment: &super::ApplicationSegment,
        lane: usize,
        incarnation: u64,
    ) -> Result<()> {
        if segment.allows_shared_response() {
            self.shared_response =
                Some(segment.open_shared_response(lane, incarnation)?);
            segment.inbox().advertise_paired_bodies(lane).map_err(map_link_error)?;
        }
        Ok(())
    }

    /// Open the HTTP exchange directions offered by the caller.
    pub fn accept_http(
        &self,
        exchange: &super::Exchange,
    ) -> Result<(Option<orbitive::stream::ReadHalf>, Option<ResponseWriter>)> {
        let Some(ticket) = exchange.stream else { return Ok((None, None)) };
        if ticket.id.kind() == SHARED_RESPONSE_CONTROL_KIND {
            if usize::from(ticket.id.node()) != usize::from(exchange.origin)
                || exchange.upgrade
                || matches!(exchange.body, super::ExchangeBody::Stream)
            {
                return Err(super::Error::Malformed("invalid HTTP response ticket"));
            }
            let (writer, _) = self.accept_shared_response(ticket)?.split();
            Ok((None, Some(ResponseWriter::Payload(writer))))
        } else {
            let (reader, writer) = self.accept(ticket)?.split();
            Ok((Some(reader), Some(ResponseWriter::ByteStream(writer))))
        }
    }

    pub fn accept_shared_response(
        &self,
        ticket: Ticket,
    ) -> Result<ExchangeEndpoint> {
        self.shared_response
            .as_ref()
            .ok_or(super::Error::Malformed(
                "receiver has no shared response table",
            ))?
            .open_peer(ExchangeTicket::from_stream_ticket(ticket))
            .map_err(Into::into)
    }

    pub fn create(&self) -> Result<(Endpoint, Ticket)> {
        self.link.create().map_err(map_link_error)
    }

    /// Rearm both the shared slot and the retained endpoint's local writer.
    ///
    /// A bodyless request may already have sent FIN. Rearming only the shared
    /// directions would leave that retained `WriteHalf` locally closed.
    pub fn rearm(
        &self,
        endpoint: &mut Endpoint,
    ) -> Result<()> {
        self.link.rearm_endpoint(endpoint).map_err(map_link_error)
    }

    pub fn accept(
        &self,
        ticket: Ticket,
    ) -> Result<Endpoint> {
        self.link.accept(ticket).map_err(map_link_error)
    }

    pub fn is_live(
        &self,
        ticket: &Ticket,
    ) -> bool {
        self.link.is_live(ticket)
    }

    pub fn node_died(
        &self,
        node: NodeId,
        incarnation: u64,
    ) {
        self.link.node_died(node, incarnation);
        if let Some(shared) = &self.shared_response {
            shared.node_dead(node, Incarnation::new(incarnation));
        }
    }

    pub fn reclaimed(
        &self,
        lanes: &[orbitive::link::Reclaimed],
    ) {
        for lane in lanes {
            self.node_died(NodeId::new(lane.lane as u16), lane.incarnation);
        }
    }

    pub fn streams(&self) -> &Streams {
        self.link.streams()
    }

    pub fn unlink(&self) -> Result<()> {
        if let Some(shared) = &self.shared_response {
            shared.unlink()?;
        }
        self.link.unlink().map_err(map_link_error)
    }
}

/// The application side of a Link response. Its backing is chosen by the
/// caller's ticket and is transparent to the HTTP producer.
pub enum ResponseWriter {
    ByteStream(WriteHalf),
    Payload(Sender),
}

impl ResponseWriter {
    pub async fn peer_closed(&mut self) -> std::result::Result<(), StreamError> {
        match self {
            Self::ByteStream(writer) => writer.peer_closed().await,
            Self::Payload(writer) => writer.peer_closed().await,
        }
    }

    pub fn start(&mut self) -> std::result::Result<(), StreamError> {
        match self {
            Self::ByteStream(_) => Ok(()),
            Self::Payload(writer) => writer.start(None),
        }
    }

    pub async fn write(
        &mut self,
        mut bytes: &[u8],
    ) -> std::result::Result<(), StreamError> {
        while !bytes.is_empty() {
            let written = self.write_chunk(bytes).await?;
            bytes = &bytes[written..];
        }
        Ok(())
    }

    /// Write up to one transport chunk and report the published byte count.
    pub async fn write_chunk(
        &mut self,
        bytes: &[u8],
    ) -> std::result::Result<usize, StreamError> {
        if bytes.is_empty() {
            return Ok(0);
        }
        match self {
            Self::ByteStream(writer) => {
                let written = writer.write(bytes).await?;
                if written == 0 {
                    return Err(StreamError::Io(std::io::ErrorKind::WriteZero.into()));
                }
                Ok(written)
            }
            Self::Payload(writer) => {
                let chunk_bytes = writer
                    .chunk_geometry()
                    .chunk_bytes_for_target(DEFAULT_CHUNK_BYTES)
                    .expect("HTTP response has payload credit");
                let chunk = &bytes[..bytes.len().min(chunk_bytes)];
                loop {
                    poll_fn(|cx| writer.poll_ready(chunk.len(), cx)).await?;
                    match writer.data(chunk) {
                        Ok(_) => return Ok(chunk.len()),
                        // Other responses on this lane may reserve the same arena credit.
                        Err(StreamError::PayloadFull { .. } | StreamError::WouldBlock) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }

    pub async fn finish(&mut self) -> std::result::Result<(), StreamError> {
        match self {
            Self::ByteStream(writer) => writer.shutdown().await.map_err(StreamError::Io),
            Self::Payload(writer) => {
                poll_fn(|cx| writer.poll_ready(0, cx)).await?;
                writer.finish()
            }
        }
    }

    pub fn finish_empty(&mut self) -> std::result::Result<(), StreamError> {
        match self {
            Self::ByteStream(writer) => {
                writer.finish_quietly();
                Ok(())
            }
            Self::Payload(writer) => writer.finish_empty(),
        }
    }

    pub fn reset(&mut self) {
        match self {
            Self::ByteStream(writer) => writer.reset(),
            Self::Payload(writer) => {
                let _ = writer.reset(ResetCode::new(RESPONSE_BODY_RESET));
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/link/application/body.rs"]
mod tests;
