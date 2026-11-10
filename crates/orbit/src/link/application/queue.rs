//! The receiver's side of window admission: run up to the published
//! execution, hold the rest of the window in arrival order, start the head
//! when a running exchange ends. No timer and no wake: the end of one
//! exchange is what starts the next.

use std::collections::VecDeque;

use super::{
    ApplicationSegment, Error, Received, Result, WorkExecution, WorkTicket,
};

/// An exchange to start: its frame and, for a bounded receiver, the accepted
/// admission it carries until it ends.
pub type Start = (Vec<u8>, Option<(usize, WorkExecution)>);

/// What became of an offered request frame.
pub enum Offered {
    /// Run it now.
    Start(Start),
    /// Every running place is taken: it waits in this receiver's queue with
    /// its reservation still held, and starts when one ends.
    Held,
}

/// One receiver lane's running count and queue. Execution is read from the
/// lane's capacity row each time, so a live bound change applies at once.
#[derive(Default)]
pub struct WorkQueue {
    running: usize,
    waiting: VecDeque<(Vec<u8>, WorkTicket)>,
}

impl WorkQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Exchanges waiting now, withdrawn ones not yet passed included.
    pub fn waiting(&self) -> usize {
        self.waiting.len()
    }

    pub fn running(&self) -> usize {
        self.running
    }

    /// A request frame arrived on `lane`. Accepted at once while a running
    /// place is free; otherwise held with its ticket still reserved, so its
    /// sender can still withdraw it. The frame is taken only when it starts
    /// or is held; on an error it stays with the caller, to refuse.
    pub fn offer(
        &mut self,
        segment: &ApplicationSegment,
        lane: usize,
        received: Received,
        frame: &mut Vec<u8>,
    ) -> Result<Offered> {
        let execution = segment
            .inbox()
            .work_execution(lane)
            .map_err(super::error::map_link_error)?;
        match (received, execution) {
            (Received::Work(ticket), Some(execution))
                if self.running >= execution =>
            {
                if !matches!(
                    super::frame_kind(frame),
                    Ok(super::FrameKind::Request)
                ) {
                    segment.inbox().withdraw_work(ticket);
                    return Err(Error::Malformed(
                        "work admission carried a non-request frame",
                    ));
                }
                self.purge_if_full(segment, lane, execution)?;
                self.waiting.push_back((std::mem::take(frame), ticket));
                Ok(Offered::Held)
            }
            (received, _) => {
                let admission = segment.accept_frame(lane, received, frame)?;
                self.running += 1;
                Ok(Offered::Start((std::mem::take(frame), admission)))
            }
        }
    }

    /// A running exchange on `lane` ended. The next one to start, if any;
    /// queued entries whose sender withdrew them, or died, are passed.
    pub fn finished(
        &mut self,
        segment: &ApplicationSegment,
        lane: usize,
    ) -> Option<Start> {
        self.running = self.running.saturating_sub(1);
        let execution = segment.inbox().work_execution(lane).ok().flatten()?;
        while self.running < execution {
            let (frame, ticket) = self.waiting.pop_front()?;
            match segment.accept_frame(lane, Received::Work(ticket), &frame) {
                Ok(admission) => {
                    self.running += 1;
                    return Some((frame, admission));
                }
                // Withdrawn at its sender's deadline, or its sender died.
                Err(_) => continue,
            }
        }
        None
    }

    /// Live entries never outnumber the queue (each holds a window slot);
    /// withdrawn ones can. Drop those before growing past the queue.
    fn purge_if_full(
        &mut self,
        segment: &ApplicationSegment,
        lane: usize,
        execution: usize,
    ) -> Result<()> {
        let window = segment
            .inbox()
            .work_capacity(lane)
            .map_err(super::error::map_link_error)?
            .unwrap_or(execution);
        if self.waiting.len() >= window.saturating_sub(execution) {
            self.waiting.retain(|(_, ticket)| {
                segment.inbox().check_work(*ticket).is_ok()
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/link/application/queue.rs"]
mod tests;
