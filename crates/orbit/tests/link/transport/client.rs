use super::*;

fn row(
    batch: u64,
    row_index: u16,
    payload: u8,
) -> ApplicationStateNotice {
    ApplicationStateNotice {
        application: "example-app".to_owned(),
        lane: 3,
        incarnation: 7,
        batch,
        row_count: 2,
        row_index,
        payload: vec![payload],
    }
}

#[test]
fn out_of_order_and_duplicate_rows_do_not_complete_a_partial_batch() {
    let mut state = ApplicationBatch::default();
    state.accept(row(11, 1, 2), Some(42));
    state.accept(row(11, 1, 2), Some(42));
    assert!(state.completed.is_none());

    state.accept(row(11, 0, 1), Some(42));
    let completed = state.completed.as_ref().unwrap();
    assert_eq!(completed.incarnation, 7);
    assert_eq!(completed.pid, Some(42));
    assert_eq!(completed.batch, 11);
    assert_eq!(completed.rows, vec![vec![1], vec![2]]);
    assert!(state.pending.is_none());

    state.accept(row(11, 0, 99), Some(42));
    state.accept(row(10, 0, 99), Some(42));
    assert_eq!(state.completed.as_ref().unwrap().rows[0], vec![1]);
    assert!(state.pending.is_none());
}

#[test]
fn newer_batch_withholds_old_snapshot_and_never_mixes_late_rows() {
    let mut state = ApplicationBatch::default();
    state.accept(row(10, 0, 1), Some(42));
    state.accept(row(10, 1, 2), Some(42));
    assert!(state.completed.is_some());

    state.accept(row(11, 0, 3), Some(42));
    assert!(state.completed.is_none());
    state.accept(row(12, 1, 6), Some(42));
    state.accept(row(11, 1, 4), Some(42));
    assert!(state.completed.is_none());
    assert!(state.pending.as_ref().unwrap().rows[0].is_none());

    state.accept(row(12, 0, 5), Some(42));
    let completed = state.completed.as_ref().unwrap();
    assert_eq!(completed.batch, 12);
    assert_eq!(completed.rows, vec![vec![5], vec![6]]);
}

#[test]
fn changed_owner_starts_a_new_batch_even_when_sequence_restarts() {
    for (incarnation, pid) in [(8, Some(42)), (7, Some(43)), (7, None)] {
        let mut state = ApplicationBatch::default();
        state.accept(row(11, 0, 1), Some(42));
        state.accept(row(11, 1, 2), Some(42));
        state.accept(row(12, 0, 3), Some(42));

        let mut first = row(1, 1, 6);
        first.incarnation = incarnation;
        state.accept(first, pid);
        assert!(state.completed.is_none());
        assert!(state.pending.as_ref().unwrap().rows[0].is_none());

        let mut second = row(1, 0, 5);
        second.incarnation = incarnation;
        state.accept(second, pid);
        let completed = state.completed.as_ref().unwrap();
        assert_eq!(completed.incarnation, incarnation);
        assert_eq!(completed.pid, pid);
        assert_eq!(completed.batch, 1);
        assert_eq!(completed.rows, vec![vec![5], vec![6]]);
    }
}

#[test]
fn inconsistent_row_count_cannot_complete_or_replace_a_batch() {
    let mut state = ApplicationBatch::default();
    state.accept(row(11, 0, 1), Some(42));
    let mut inconsistent = row(11, 1, 99);
    inconsistent.row_count = 3;
    state.accept(inconsistent, Some(42));
    assert!(state.completed.is_none());
    assert_eq!(state.pending.as_ref().unwrap().rows.len(), 2);
    assert!(state.pending.as_ref().unwrap().rows[1].is_none());

    state.accept(row(11, 1, 2), Some(42));
    assert_eq!(state.completed.as_ref().unwrap().rows, vec![vec![1], vec![2]]);
}
