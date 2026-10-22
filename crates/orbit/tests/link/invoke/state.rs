use super::*;

#[test]
fn invoke_state_bounds_pending_calls_without_reusing_correlations() {
    let peer = Peer { lane: 1, incarnation: 9 };
    let mut state = State::default();
    let (tx, _) = oneshot::channel();
    assert_eq!(state.insert(peer, tx, 1), Err(Error::Closed));
    state.running = true;
    let (tx, _) = oneshot::channel();
    let first = state.insert(peer, tx, 1).unwrap();
    let (tx, _) = oneshot::channel();
    assert_eq!(state.insert(peer, tx, 1), Err(Error::Busy));
    state.pending.remove(&first);
    let (tx, _) = oneshot::channel();
    assert!(state.insert(peer, tx, 1).unwrap() > first);
    state.pending.clear();
    state.sequence = u64::MAX;
    let (tx, _) = oneshot::channel();
    assert_eq!(state.insert(peer, tx, 1), Err(Error::Closed));
}

#[test]
fn invoke_state_rejects_wrong_peer_and_stale_generation_results() {
    let peer = Peer { lane: 1, incarnation: 9 };
    let mut state = State { running: true, ..State::default() };
    let (tx, mut rx) = oneshot::channel();
    let id = state.insert(peer, tx, 1).unwrap();
    let _ = state.complete(id, Peer { incarnation: 8, ..peer }, Ok(vec![1]));
    let _ = state.complete(id, Peer { lane: 2, ..peer }, Ok(vec![1]));
    assert!(matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
    let _ = state.complete(id, peer, Ok(vec![2]));
    assert_eq!(rx.try_recv().unwrap().unwrap(), [2]);
    assert!(state.pending.is_empty());
    assert_eq!(state.complete(id, peer, Ok(vec![3])), Some(Ok(vec![3])));
}

#[test]
fn invoke_state_close_reports_uncertainty_not_safe_replay() {
    let mut state = State { running: true, ..State::default() };
    let (tx, mut rx) = oneshot::channel();
    state.insert(Peer { lane: 1, incarnation: 9 }, tx, 1).unwrap();
    state.close();
    assert_eq!(rx.try_recv().unwrap(), Err(Error::OutcomeUnknown));
    assert!(!state.running);
    assert!(state.pending.is_empty());
}
