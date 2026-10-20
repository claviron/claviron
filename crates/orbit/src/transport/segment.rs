//! Claviron's namespace and application naming over the generic `orbitive::link`
//! inbox segment.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::{
    ApplicationStateNotice, ControlCommand, Error, Result, ShutdownNotice,
    TRANSPORT_INBOX_KIND, TRANSPORT_LINK_SPEC, map_link_error,
    transport_link_spec,
};
#[cfg(test)]
use orbitive::link::Reclaimed;
use orbitive::link::{
    DirectoryWatcher, Inbox, InboxReceiver, Joined, LinkSegment, LinkSpec,
};

/// The geometry every Claviron application transport peer must agree on.
pub const INBOX_GEOMETRY: orbitive::link::InboxGeometry =
    TRANSPORT_LINK_SPEC.inbox;

/// A mapped application inbox, backed by `orbitive::link::LinkSegment`.
pub struct TransportSegment {
    link: Arc<LinkSegment>,
    fleet_name: String,
    spec: LinkSpec,
}

impl TransportSegment {
    /// Open a table with Claviron's inbox geometry and an explicit test or
    /// embedding capacity.
    pub fn open(
        fleet_name: &str,
        fleet_capacity: usize,
    ) -> Result<Self> {
        let capacity = u16::try_from(fleet_capacity).map_err(|_| {
            Error::Malformed("transport fleet capacity exceeds u16")
        })?;
        let spec = transport_link_spec(capacity);
        let link = Arc::new(
            LinkSegment::open(fleet_name, spec).map_err(map_link_error)?,
        );
        Ok(Self { link, fleet_name: fleet_name.to_owned(), spec })
    }

    pub fn for_fleet(fleet_name: &str) -> Result<Self> {
        let link = Arc::new(
            LinkSegment::open(fleet_name, TRANSPORT_LINK_SPEC)
                .map_err(map_link_error)?,
        );
        Ok(Self {
            link,
            fleet_name: fleet_name.to_owned(),
            spec: TRANSPORT_LINK_SPEC,
        })
    }

    /// Attach only when the fleet's inbox already exists. Management must not
    /// create an empty Link fleet while reporting an edge shutdown.
    pub fn existing_for_fleet(fleet_name: &str) -> Result<Option<Self>> {
        let name =
            orbitive::shm::ring_segment_name(fleet_name, TRANSPORT_INBOX_KIND);
        match orbitive::shm::ShmRegion::validate_existing(
            &name,
            TRANSPORT_LINK_SPEC.inbox_segment_bytes(),
        )
        .map_err(|_| Error::Malformed("could not inspect Link inbox"))?
        {
            orbitive::shm::ShmValidation::Missing => Ok(None),
            orbitive::shm::ShmValidation::Valid { .. } => {
                Self::for_fleet(fleet_name).map(Some)
            }
        }
    }

    pub fn join(
        &self,
        application: &str,
        runtime: &str,
        incarnation: u64,
    ) -> Result<Joined> {
        check_application_name(application)?;
        self.link
            .join(application, runtime, incarnation)
            .map_err(map_link_error)
    }

    pub fn inbox(&self) -> &Inbox {
        self.link.inbox()
    }

    pub fn receiver(
        &self,
        lane: usize,
    ) -> Result<InboxReceiver> {
        self.link.receiver(lane).map_err(map_link_error)
    }

    /// Forward a command to one ready application lane. A successful write
    /// proves only inbox acceptance, never command execution.
    pub fn control(
        &self,
        application: &str,
        command: &ControlCommand,
    ) -> Result<()> {
        check_application_name(application)?;
        if command.application != application {
            return Err(Error::Malformed(
                "transport control target differs from application",
            ));
        }
        let lane = self
            .inbox()
            .targets(application)
            .into_iter().find(|(lane, _)| {
                matches!(self.inbox().role(*lane).ok().flatten(), Some(role) if role != super::EDGE_RUNTIME)
            })
            .ok_or_else(|| Error::NoConsumer(application.to_owned()))?
            .0;
        if let Err(error) = self.inbox().write(lane, &command.encode()?) {
            self.reclaim_if_gone(lane);
            return Err(map_link_error(error));
        }
        Ok(())
    }

    /// After a refused write: take `lane` back if its process was killed
    /// without releasing it, so it reads as a departure rather than as a lane
    /// that stays full. Returns whether it was.
    pub fn reclaim_if_gone(
        &self,
        lane: usize,
    ) -> bool {
        match self.link.reclaim_if_gone_with(lane, |gone| {
            if let Err(error) = self.end_streams(gone) {
                tracing::error!(lane, %error, "could not recover Link bodies");
            }
        }) {
            Ok(Some(gone)) => {
                tracing::info!(
                    lane,
                    pid = gone.pid,
                    incarnation = gone.incarnation,
                    "🛰 Link lane left by a process that did not release it; reclaimed"
                );
                true
            }
            Ok(None) => false,
            Err(error) => {
                tracing::debug!(
                    lane,
                    "Link lane could not be checked: {error}"
                );
                false
            }
        }
    }

    /// End the streams of an exited process, never a sibling or a newer
    /// incarnation. This is local ownership recovery, not an application event.
    /// [INCARNATION_MASKING] requires cleanup of that exact owner, not every
    /// process that has used the same logical lane.
    ///
    /// [INCARNATION_MASKING]: https://github.com/claviron/claviron/blob/main/docs/design/INCARNATION_MASKING.md
    pub fn process_exited(
        &self,
        pid: u32,
        incarnation: u64,
    ) -> Result<()> {
        let mut failure = None;
        let gone = self
            .link
            .reclaim_process(pid, incarnation, |gone| {
                if let Err(error) = self.end_streams(gone) {
                    failure = Some(error);
                }
            })
            .map_err(map_link_error)?;
        if let Some(error) = failure {
            return Err(error);
        }
        if !gone.is_empty() {
            tracing::debug!(pid, incarnation, lanes = gone.len(), fleet = %self.fleet_name,
                "🛰  exited process Link streams released");
        }
        Ok(())
    }

    fn end_streams(
        &self,
        gone: &orbitive::link::Reclaimed,
    ) -> Result<()> {
        orbitive::stream::Streams::node_dead_in_shm(
            &self.fleet_name,
            self.spec.fleet_capacity,
            self.spec.streams,
            orbitive::NodeId::new(gone.lane as u16),
            orbitive::stream::Incarnation::new(gone.incarnation),
        )
        .map(|_| ())
        .map_err(Error::Stream)
    }

    /// Publish one complete application snapshot batch from this runner lane
    /// to connected edge lanes. The lane stays claimed when an app is stopped.
    pub fn publish_application_state(
        &self,
        application: &str,
        rows: &[Vec<u8>],
    ) -> Result<usize> {
        check_application_name(application)?;
        if rows.is_empty() {
            return Err(Error::Malformed("application snapshot has no rows"));
        }
        let row_count =
            u16::try_from(rows.len()).map_err(|_| Error::FrameTooLarge)?;
        let inbox = self.inbox();
        let own_lane = (0..inbox.lanes()).find(|&lane| {
            inbox.owner(lane).ok().flatten() == Some(std::process::id())
                && inbox.service(lane).ok().flatten().as_deref() == Some(application)
                && matches!(inbox.role(lane).ok().flatten(), Some(role) if role != super::EDGE_RUNTIME)
                && inbox.consumers(lane).ok().is_some_and(|count| count > 0)
        });
        let Some(own_lane) = own_lane else { return Ok(0) };
        static NEXT_BATCH: AtomicU64 = AtomicU64::new(1);
        let batch = NEXT_BATCH.fetch_add(1, Ordering::Relaxed);
        let frames = rows
            .iter()
            .enumerate()
            .map(|(index, payload)| {
                ApplicationStateNotice {
                    application: application.to_owned(),
                    lane: u16::try_from(own_lane).map_err(|_| {
                        Error::Malformed("application lane exceeds u16")
                    })?,
                    incarnation: inbox
                        .incarnation(own_lane)
                        .map_err(map_link_error)?,
                    batch,
                    row_count,
                    row_index: u16::try_from(index)
                        .map_err(|_| Error::FrameTooLarge)?,
                    payload: payload.clone(),
                }
                .encode()
            })
            .collect::<Result<Vec<_>>>()?;
        let mut delivered = 0;
        for lane in 0..inbox.lanes() {
            if inbox.consumers(lane).map_err(map_link_error)? == 0
                || !matches!(inbox.role(lane).map_err(map_link_error)?, Some(role) if role == super::EDGE_RUNTIME)
            {
                continue;
            }
            let mut complete = true;
            for frame in &frames {
                if let Err(error) = inbox.write(lane, frame) {
                    if !self.reclaim_if_gone(lane) {
                        tracing::warn!(
                            lane,
                            "application state could not reach edge lane: {error}"
                        );
                    }
                    complete = false;
                    break;
                }
            }
            if complete {
                delivered += 1;
            }
        }
        Ok(delivered)
    }

    /// The edge master sends its shutdown notice to each connected application
    /// lane. Individual failed writes are reported, never treated as an app exit.
    pub fn announce_edge_shutdown(
        &self,
        edge: &str,
    ) -> Result<usize> {
        let frame = ShutdownNotice { edge: edge.to_owned() }.encode()?;
        let inbox = self.inbox();
        let mut delivered = 0;
        for lane in 0..inbox.lanes() {
            if inbox.consumers(lane).map_err(map_link_error)? == 0
                || !matches!(inbox.role(lane).map_err(map_link_error)?, Some(role) if role != super::EDGE_RUNTIME)
            {
                continue;
            }
            match inbox.write(lane, &frame) {
                Ok(_) => delivered += 1,
                Err(_) if self.reclaim_if_gone(lane) => {}
                Err(error) => tracing::warn!(
                    lane,
                    "edge shutdown notice could not reach application lane: {error}"
                ),
            }
        }
        Ok(delivered)
    }

    /// Be woken when a lane is claimed or released without holding a lane:
    /// for a process that serves nothing, such as a master.
    pub fn watch(
        &self,
        role: &str,
    ) -> Result<DirectoryWatcher> {
        self.link.watch(role).map_err(map_link_error)
    }

    /// The generic link segment underneath, for what binds to it directly.
    pub fn link(&self) -> &Arc<LinkSegment> {
        &self.link
    }

    pub fn created(&self) -> bool {
        self.link.created()
    }

    pub fn segment_bytes(&self) -> usize {
        self.link.segment_bytes()
    }

    pub fn unlink(&self) -> Result<()> {
        self.link.unlink().map_err(map_link_error)
    }
}

/// Application identity fits a lane, independently of the SHM namespace.
pub fn check_application_name(application: &str) -> Result<()> {
    if application.is_empty()
        || !application.bytes().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(character, b'-' | b'_' | b'.')
        })
    {
        return Err(Error::InvalidName {
            application: application.to_owned(),
            reason: "must contain letters, digits, '-', '_' or '.'".to_owned(),
        });
    }
    if application.len() > orbitive::link::LANE_NAME_MAX {
        return Err(Error::InvalidName {
            application: application.to_owned(),
            reason: format!(
                "longer than the {} bytes a lane holds",
                orbitive::link::LANE_NAME_MAX
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/transport/segment.rs"]
mod tests;
