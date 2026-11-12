//! Claviron's namespace and application naming over the generic `orbitive::link`
//! inbox segment.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::{
    Access, ApplicationStateNotice, ControlCommand, Error, LINK_SPEC, Result,
    ShutdownNotice, link_spec, map_link_error,
};
#[cfg(test)]
use orbitive::link::Reclaimed;
use orbitive::link::{
    DirectoryWatcher, Inbox, InboxReceiver, Joined, LinkSegment, LinkSpec,
};

/// The geometry every Claviron application link peer must agree on.
pub const INBOX_GEOMETRY: orbitive::link::InboxGeometry = LINK_SPEC.inbox;

/// A mapped application inbox, backed by `orbitive::link::LinkSegment`.
pub struct ApplicationSegment {
    link: Arc<LinkSegment>,
    pub(super) fleet_name: String,
    spec: LinkSpec,
    access: Access,
}

impl ApplicationSegment {
    /// Open a table with Claviron's inbox geometry and an explicit test or
    /// embedding capacity.
    pub fn open(
        fleet_name: &str,
        fleet_capacity: usize,
    ) -> Result<Self> {
        Self::open_with_access(fleet_name, fleet_capacity, &Access::OwnerOnly)
    }

    pub fn open_with_access(
        fleet_name: &str,
        fleet_capacity: usize,
        access: &Access,
    ) -> Result<Self> {
        let capacity = u16::try_from(fleet_capacity)
            .map_err(|_| Error::Malformed("link fleet capacity exceeds u16"))?;
        let spec = link_spec(capacity);
        let link =
            Arc::new(access.open(fleet_name, spec).map_err(map_link_error)?);
        Ok(Self {
            link,
            fleet_name: fleet_name.to_owned(),
            spec,
            access: access.clone(),
        })
    }

    pub fn for_fleet(fleet_name: &str) -> Result<Self> {
        Self::for_fleet_with_access(fleet_name, &Access::OwnerOnly)
    }

    pub fn for_fleet_with_access(
        fleet_name: &str,
        access: &Access,
    ) -> Result<Self> {
        Self::for_fleet_with_spec(fleet_name, LINK_SPEC, access)
    }

    pub fn for_fleet_with_spec(
        fleet_name: &str,
        spec: LinkSpec,
        access: &Access,
    ) -> Result<Self> {
        // The owner's shape, configured or compiled (bindings open with the
        // compiled one): every sender's replies must fit its inbox.
        if !super::geometry::lane_fits_inbox(spec) {
            return Err(Error::LaneExceedsInbox {
                lane_capacity: spec.streams.lane_capacity,
                inbox_capacity: spec.inbox.capacity,
            });
        }
        let link =
            Arc::new(access.open(fleet_name, spec).map_err(map_link_error)?);
        Ok(Self {
            link,
            fleet_name: fleet_name.to_owned(),
            spec,
            access: access.clone(),
        })
    }

    /// Attach a destination its owner created and adopt the shape the owner
    /// declared. A caller never creates a destination: `Ok(None)` while the
    /// owner's inbox or stream table does not exist yet. A shape beyond this
    /// build's compiled ceilings is refused with both shapes named.
    pub fn adopt(
        fleet_name: &str,
        access: &Access,
    ) -> Result<Option<Self>> {
        if let Access::Group(_) = access {
            // Orbitive adopts owner-only Links only. A group Link keeps this
            // build's compiled shape and is opened only when it exists.
            return if access
                .exists(fleet_name, LINK_SPEC)
                .map_err(map_link_error)?
            {
                Self::for_fleet_with_access(fleet_name, access).map(Some)
            } else {
                Ok(None)
            };
        }
        let Some(link) = access
            .adopt(fleet_name, super::LINK_INBOX_KIND, super::LINK_STREAM_KIND)
            .map_err(map_link_error)?
        else {
            return Ok(None);
        };
        let spec = link.spec();
        if !super::geometry::fits_build(spec) {
            return Err(Error::ShapeExceedsBuild {
                fleet: fleet_name.to_owned(),
                declared: super::geometry::shape_summary(spec),
                ceilings: super::geometry::shape_summary(LINK_SPEC),
            });
        }
        Ok(Some(Self {
            link: Arc::new(link),
            fleet_name: fleet_name.to_owned(),
            spec,
            access: access.clone(),
        }))
    }

    /// Wait until the fleet's first receiver has created its Link, then
    /// adopt the shape it declared. For an owner that runs a fleet's
    /// processes without creating their Link: it starts them, then waits
    /// here. No deadline: the creator's bell ends the wait. Owner-only, and
    /// one such owner per fleet.
    pub async fn adopt_when_created(fleet_name: &str) -> Result<Self> {
        let link = LinkSegment::adopt_when_created(
            fleet_name,
            super::LINK_INBOX_KIND,
            super::LINK_STREAM_KIND,
        )
        .await
        .map_err(map_link_error)?;
        let spec = link.spec();
        if !super::geometry::fits_build(spec) {
            return Err(Error::ShapeExceedsBuild {
                fleet: fleet_name.to_owned(),
                declared: super::geometry::shape_summary(spec),
                ceilings: super::geometry::shape_summary(LINK_SPEC),
            });
        }
        Ok(Self {
            link: Arc::new(link),
            fleet_name: fleet_name.to_owned(),
            spec,
            access: Access::OwnerOnly,
        })
    }

    /// Attach only when the fleet's inbox already exists. Management must not
    /// create an empty Link fleet while reporting an edge shutdown.
    pub fn existing_for_fleet(fleet_name: &str) -> Result<Option<Self>> {
        Self::existing_for_fleet_with_access(fleet_name, &Access::OwnerOnly)
    }

    /// Management and recovery read the shape the fleet's owner declared.
    pub fn existing_for_fleet_with_access(
        fleet_name: &str,
        access: &Access,
    ) -> Result<Option<Self>> {
        Self::adopt(fleet_name, access)
    }

    pub fn spec(&self) -> LinkSpec {
        self.spec
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

    pub fn join_with_capacity(
        &self,
        application: &str,
        runtime: &str,
        incarnation: u64,
        capacity: Option<usize>,
    ) -> Result<Joined> {
        check_application_name(application)?;
        if let Some(limit) = capacity {
            self.check_admission(limit)?;
        }
        self.link
            .join_with_capacity(application, runtime, incarnation, capacity)
            .map_err(map_link_error)
    }

    /// Publish how many of this receiver's window slots run at once; the rest
    /// are its own queue.
    pub fn set_work_execution(
        &self,
        lane: usize,
        execution: usize,
    ) -> Result<()> {
        self.inbox().set_work_execution(lane, execution).map_err(map_link_error)
    }

    /// Change the bound for this process's joined receiver without replacing it.
    pub fn update_work_capacity(
        &self,
        joined: &Joined,
        limit: usize,
    ) -> Result<()> {
        self.check_admission(limit)?;
        self.link.update_work_capacity(joined, limit).map_err(map_link_error)
    }

    /// The segment-level form of `LinkGeometry::admits`, for every receiver
    /// that joins here, configured or not (bindings join directly).
    fn check_admission(
        &self,
        max_in_flight: usize,
    ) -> Result<()> {
        let inbox_capacity = self.spec.inbox.capacity;
        if max_in_flight == 0 || max_in_flight >= inbox_capacity {
            return Err(Error::AdmissionExceedsInbox {
                max_in_flight,
                inbox_capacity,
            });
        }
        Ok(())
    }

    pub fn reserve_work(
        &self,
        target: usize,
        origin: usize,
    ) -> Result<Option<super::WorkReservation>> {
        self.link.reserve_work(target, origin).map_err(map_link_error)
    }

    pub fn accept_work(
        &self,
        lane: usize,
        ticket: super::WorkTicket,
    ) -> Result<super::WorkExecution> {
        self.link.accept_work(lane, ticket).map_err(map_link_error)
    }

    /// Validate inbox admission before a frame reaches application code.
    /// Raw requests cannot bypass a bound; work tickets cannot carry control.
    pub fn accept_frame(
        &self,
        lane: usize,
        received: super::Received,
        frame: &[u8],
    ) -> Result<Option<(usize, super::WorkExecution)>> {
        match received {
            super::Received::Work(ticket) => {
                let execution = self.accept_work(lane, ticket)?;
                if !matches!(
                    super::frame_kind(frame),
                    Ok(super::FrameKind::Request)
                ) {
                    return Err(Error::Malformed(
                        "work admission carried a non-request frame",
                    ));
                }
                Ok(Some((ticket.origin(), execution)))
            }
            super::Received::Frame => {
                if self
                    .inbox()
                    .work_capacity(lane)
                    .map_err(map_link_error)?
                    .is_some()
                    && matches!(
                        super::frame_kind(frame),
                        Ok(super::FrameKind::Request)
                    )
                {
                    return Err(Error::Malformed(
                        "bounded receiver requires a work admission",
                    ));
                }
                Ok(None)
            }
            _ => Err(Error::Malformed(
                "notification does not carry an application frame",
            )),
        }
    }

    /// Application intake, including exact origin-death recovery when bounded.
    pub fn application_receiver(
        self: &Arc<Self>,
        lane: usize,
    ) -> Result<super::ApplicationReceiver> {
        super::ApplicationReceiver::new(Arc::clone(self), lane)
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
                "control target differs from application",
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

    /// Wait until the kernel has released this exited process's lane. Blocks;
    /// see `LinkSegment::wait_lane_released`.
    pub fn wait_lane_released(
        &self,
        lane: usize,
        pid: u32,
        incarnation: u64,
    ) -> Result<()> {
        self.link
            .wait_lane_released(lane, pid, incarnation)
            .map_err(map_link_error)
    }

    /// Observe native process exit, then recover under its exact kernel hold.
    /// Both callers and bounded receivers use this; no timer infers death.
    pub async fn recover_on_exit(
        self: Arc<Self>,
        peer: &super::Peer,
    ) -> Result<bool> {
        let Some(pid) = peer.pid else { return Ok(false) };
        let handle = match claviron_sys::process::identity(pid)
            .map(claviron_sys::process::ProcessHandle::open)
        {
            Some(Ok(handle)) => Some(handle),
            None => None,
            Some(Err(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                None
            }
            Some(Err(error)) => {
                return Err(map_link_error(orbitive::link::Error::Io(error)));
            }
        };
        if let Some(handle) = handle
            && !self.reclaim_if_gone(peer.lane)
        {
            handle.wait().await.map_err(|error| {
                map_link_error(orbitive::link::Error::Io(error))
            })?;
        }
        // Exit notification may precede lock release. An owned cold-path
        // thread waits on that exact hold; the runtime never blocks on it.
        let (released, wait) = tokio::sync::oneshot::channel();
        let lane = peer.lane;
        let incarnation = peer.incarnation;
        let owned = Arc::clone(&self);
        std::thread::Builder::new()
            .name("orbit-lane-release".to_owned())
            .spawn(move || {
                let _ = released.send(owned.wait_lane_released(
                    lane,
                    pid,
                    incarnation,
                ));
            })
            .map_err(|error| {
                map_link_error(orbitive::link::Error::Io(error))
            })?;
        wait.await.map_err(|_| {
            Error::Malformed("Link lane release observer ended")
        })??;
        self.process_exited(pid, incarnation)?;
        Ok(true)
    }

    fn end_streams(
        &self,
        gone: &orbitive::link::Reclaimed,
    ) -> Result<()> {
        self.access
            .recover(&self.fleet_name, self.spec, gone.lane, gone.incarnation)
            .map_err(map_link_error)
    }

    /// Publish one complete application snapshot batch from this runner lane
    /// to connected edge lanes, or to the one edge lane in `to` that asked for
    /// it. Pass the publishing process's PID captured by runtime state. The
    /// lane stays claimed when an app is stopped.
    pub fn publish_application_state(
        &self,
        application: &str,
        pid: u32,
        rows: &[Vec<u8>],
        to: Option<usize>,
    ) -> Result<usize> {
        check_application_name(application)?;
        if rows.is_empty() {
            return Err(Error::Malformed("application snapshot has no rows"));
        }
        let row_count =
            u16::try_from(rows.len()).map_err(|_| Error::FrameTooLarge)?;
        let inbox = self.inbox();
        let own_lane = (0..inbox.lanes()).find(|&lane| {
            inbox.owner(lane).ok().flatten() == Some(pid)
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
            if to.is_some_and(|asked| asked != lane)
                || inbox.consumers(lane).map_err(map_link_error)? == 0
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

    /// Publish one reply frame into the lane of the Edge that asked.
    ///
    /// [`Error::ProducerBusy`] means this process's replies that Edges have
    /// not read yet hold every record it may publish with; the Edge's lane may
    /// have room. Wait with [`Self::record_released`] and publish again.
    pub fn publish_reply(
        &self,
        origin: usize,
        frame: &[&[u8]],
    ) -> Result<()> {
        self.link
            .inbox()
            .write_vectored(origin, frame)
            .map(|_| ())
            .map_err(map_link_error)
    }

    /// Wait until an Edge reads one of this process's frames, after
    /// [`Self::publish_reply`] returned [`Error::ProducerBusy`]. The reader
    /// rings it; there is no deadline and no poll.
    pub async fn record_released(&self) -> Result<()> {
        self.link.record_released().await.map_err(map_link_error)
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
#[path = "../../../tests/link/application/segment.rs"]
mod tests;
