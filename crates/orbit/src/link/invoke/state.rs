use std::collections::BTreeMap;

use tokio::sync::oneshot;

use super::Peer;
use crate::invoke::{CallError as Error, CallResult as Result};

pub(super) struct Pending {
    pub peer: Peer,
    pub result: oneshot::Sender<Result<Vec<u8>>>,
}

#[derive(Default)]
pub(super) struct State {
    pub running: bool,
    pub sequence: u64,
    pub pending: BTreeMap<u64, Pending>,
}

impl State {
    pub fn insert(
        &mut self,
        peer: Peer,
        result: oneshot::Sender<Result<Vec<u8>>>,
        capacity: usize,
    ) -> Result<u64> {
        if !self.running {
            return Err(Error::Closed);
        }
        if self.pending.len() >= capacity {
            return Err(Error::Busy);
        }
        self.sequence = self.sequence.checked_add(1).ok_or(Error::Closed)?;
        self.pending.insert(self.sequence, Pending { peer, result });
        Ok(self.sequence)
    }

    pub fn complete(
        &mut self,
        sequence: u64,
        peer: Peer,
        result: Result<Vec<u8>>,
    ) -> Option<Result<Vec<u8>>> {
        if self.pending.get(&sequence).is_some_and(|call| call.peer == peer)
            && let Some(call) = self.pending.remove(&sequence)
        {
            return call.result.send(result).err();
        }
        Some(result)
    }

    pub fn close(&mut self) {
        self.running = false;
        for (_, call) in std::mem::take(&mut self.pending) {
            let _ = call.result.send(Err(Error::OutcomeUnknown));
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/link/invoke/state.rs"]
mod tests;
