//! Claviron's configured body table over the generic `orbitive::link` layer.
//!
//! The HTTP exchange frame carries the ticket; `orbit-link` owns the reusable
//! duplex slot and its node/incarnation recovery. Claviron owns the kind and
//! geometry because they are part of this deployment's physical ABI.

use std::sync::Arc;

use orbitive::link::LinkBodies;
use orbitive::stream::{Endpoint, Streams, Ticket};
use orbitive::{Fleet, NodeId};

use super::{Result, TRANSPORT_LINK_SPEC, map_link_error, transport_link_spec};

/// This process's handle on the destination fleet's duplex body table.
pub struct TransportBodies {
    link: LinkBodies,
}

impl TransportBodies {
    pub fn open(
        fleet: Arc<Fleet>,
        incarnation: u64,
    ) -> Result<Self> {
        let spec = transport_link_spec(fleet.fleet_capacity());
        let link = LinkBodies::open(fleet, incarnation, spec)
            .map_err(map_link_error)?;
        Ok(Self { link })
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
            TRANSPORT_LINK_SPEC,
        )
        .map_err(map_link_error)?;
        Ok(Self { link })
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
    }

    pub fn reclaimed(
        &self,
        lanes: &[orbitive::link::Reclaimed],
    ) {
        self.link.reclaim(lanes);
    }

    pub fn streams(&self) -> &Streams {
        self.link.streams()
    }

    pub fn unlink(&self) -> Result<()> {
        self.link.unlink().map_err(map_link_error)
    }
}

#[cfg(test)]
#[path = "../../../tests/link/transport/body.rs"]
mod tests;
