use super::*;
use std::cell::Cell;

fn next(
    selection: &Selection,
    lanes: &[usize],
) -> Option<usize> {
    selection
        .round_robin(
            lanes,
            |_| true,
            |lane| Ok::<_, ()>(Some(lane)),
            || panic!("a runnable receiver must not report growth pressure"),
        )
        .unwrap()
}

#[test]
fn round_robin_cycles_without_filling_the_home_receiver() {
    let selection = Selection::new(LinkPolicy::RoundRobin, 1);
    let selected: Vec<_> =
        (0..6).map(|_| next(&selection, &[7, 3, 9]).unwrap()).collect();
    assert_eq!(selected, [3, 9, 7, 3, 9, 7]);
}

#[test]
fn round_robin_skips_saturated_workers_before_using_any_queue() {
    let selection = Selection::new(LinkPolicy::RoundRobin, 0);
    let mut reservations = Vec::new();
    let result = selection
        .round_robin(
            &[1, 2, 3],
            |lane| lane != 1,
            |lane| {
                reservations.push(lane);
                // Worker 2 became full after its readiness read.
                Ok::<_, ()>((lane == 3).then_some(lane))
            },
            || panic!("worker 3 can start now"),
        )
        .unwrap();
    assert_eq!(result, Some(3));
    assert_eq!(reservations, [2, 3]);
}

#[test]
fn round_robin_reports_pressure_before_queueing_and_skips_refusals() {
    let selection = Selection::new(LinkPolicy::RoundRobin, 0);
    let pressure = Cell::new(false);
    let result = selection
        .round_robin(
            &[1, 2],
            |_| false,
            |lane| {
                assert!(pressure.get());
                Ok::<_, ()>((lane == 2).then_some(lane))
            },
            || pressure.set(true),
        )
        .unwrap();
    assert_eq!(result, Some(2));
}

#[test]
fn round_robin_full_or_empty_snapshot_never_invents_a_wait() {
    let selection = Selection::new(LinkPolicy::RoundRobin, 0);
    let pressure = Cell::new(0);
    let mut reservations = 0;
    let result = selection
        .round_robin(
            &[1, 2],
            |_| false,
            |_| {
                reservations += 1;
                Ok::<Option<usize>, ()>(None)
            },
            || pressure.set(pressure.get() + 1),
        )
        .unwrap();
    assert_eq!(result, None);
    assert_eq!(reservations, 2);
    assert_eq!(pressure.get(), 1);
    assert_eq!(
        selection.round_robin(
            &[],
            |_| panic!("empty"),
            |_| -> Result<Option<usize>, ()> { panic!("empty") },
            || panic!("no receiver is not saturation"),
        ),
        Ok(None)
    );
}

#[test]
fn round_robin_cursor_survives_membership_changes_and_wraps() {
    let selection = Selection::new(LinkPolicy::RoundRobin, usize::MAX);
    assert!(next(&selection, &[2, 4]).is_some());
    assert_eq!(next(&selection, &[4]), Some(4));
    let seen: Vec<_> =
        (0..3).map(|_| next(&selection, &[4, 8, 9]).unwrap()).collect();
    assert!(seen.contains(&4) && seen.contains(&8) && seen.contains(&9));
}

#[test]
fn round_robin_clones_share_turns_but_independent_targets_do_not() {
    let selection =
        std::sync::Arc::new(Selection::new(LinkPolicy::RoundRobin, 0));
    let clone = std::sync::Arc::clone(&selection);
    assert_eq!(next(&selection, &[1, 2]), Some(1));
    assert_eq!(next(&clone, &[1, 2]), Some(2));
    let independent = Selection::new(LinkPolicy::RoundRobin, 0);
    assert_eq!(next(&independent, &[1, 2]), Some(1));
}

#[test]
fn round_robin_propagates_reservation_errors_without_replaying() {
    let selection = Selection::new(LinkPolicy::RoundRobin, 0);
    let mut attempts = 0;
    let result = selection.round_robin(
        &[1, 2],
        |_| true,
        |_| {
            attempts += 1;
            Err::<Option<usize>, _>("closed")
        },
        || panic!("an error is not pressure"),
    );
    assert_eq!(result, Err("closed"));
    assert_eq!(attempts, 1);
}
