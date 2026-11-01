use std::collections::BTreeMap;
use std::sync::Arc;

use claviron_http::ListenerId;
use claviron_http::listener::{ListenerSnapshot, ListenerSnapshots};

#[derive(Clone)]
struct Snapshot {
    id: ListenerId,
    port: u16,
    capacity: usize,
    policy_revision: usize,
}

impl ListenerSnapshot for Snapshot {
    fn listener_id(&self) -> &ListenerId {
        &self.id
    }

    fn same_binding(
        &self,
        other: &Self,
    ) -> bool {
        self.id == other.id
            && self.port == other.port
            && self.capacity == other.capacity
    }

    fn binding_conflicts(
        &self,
        other: &Self,
    ) -> bool {
        self.port != 0 && self.port == other.port
    }
}

fn groups(
    port: u16,
    capacity: usize,
    policy_revision: usize,
) -> BTreeMap<ListenerId, Arc<Snapshot>> {
    let id: ListenerId = "local".parse().unwrap();
    BTreeMap::from([(
        id.clone(),
        Arc::new(Snapshot { id, port, capacity, policy_revision }),
    )])
}

#[test]
fn reload_preserves_binding_and_restart_adopts_desired_geometry() {
    let snapshots = ListenerSnapshots::default();
    let id = "local".parse().unwrap();
    let run = snapshots.start(groups(8080, 2, 100)).unwrap();
    let accepted = snapshots.get(&id).unwrap();
    assert!(snapshots.start(groups(8080, 2, 100)).is_err());
    snapshots.publish(groups(8080, 2, 50)).unwrap();
    let updated = snapshots.get(&id).unwrap();
    assert!(!Arc::ptr_eq(&accepted, &updated));
    assert_eq!(updated.policy_revision, 50);
    assert!(snapshots.publish(groups(8081, 2, 50)).is_err());
    assert!(snapshots.publish(groups(8080, 3, 50)).is_err());
    assert!(Arc::ptr_eq(&updated, &snapshots.get(&id).unwrap()));
    snapshots.publish(BTreeMap::new()).unwrap();
    assert!(snapshots.get(&id).is_none());
    assert!(snapshots.start(groups(8081, 3, 50)).is_err());
    assert_eq!(run.groups().len(), 1);
    drop(run);
    let _next = snapshots.start(groups(8081, 3, 50)).unwrap();
    assert_eq!(snapshots.get(&id).unwrap().port, 8081);
    assert_eq!(accepted.port, 8080);
}

#[test]
fn mismatched_identity_is_never_published() {
    let snapshots = ListenerSnapshots::default();
    let original = groups(8080, 2, 100);
    let config = original.values().next().unwrap().clone();
    let wrong = BTreeMap::from([("other".parse().unwrap(), config)]);
    assert!(snapshots.start(wrong.clone()).is_err());
    let _run = snapshots.start(original).unwrap();
    assert!(snapshots.publish(wrong).is_err());
    assert!(snapshots.get(&"other".parse().unwrap()).is_none());
    assert!(snapshots.get(&"local".parse().unwrap()).is_some());
}

#[test]
fn listener_generations_start_and_stop_independently() {
    let snapshots = ListenerSnapshots::default();
    let first = groups(8080, 2, 1).pop_first().unwrap().1;
    let second_id: ListenerId = "second".parse().unwrap();
    let second = Arc::new(Snapshot {
        id: second_id.clone(),
        port: 8081,
        capacity: 3,
        policy_revision: 1,
    });
    let one = snapshots.start_listener(first.clone()).unwrap();
    let two = snapshots.start_listener(second.clone()).unwrap();
    assert!(Arc::ptr_eq(&snapshots.get(&first.id).unwrap(), &first));
    assert!(Arc::ptr_eq(&snapshots.get(&second_id).unwrap(), &second));
    drop(one);
    let next = Arc::new(Snapshot { port: 8082, ..first.as_ref().clone() });
    let _replacement = snapshots.start_listener(next.clone()).unwrap();
    assert!(Arc::ptr_eq(&snapshots.get(&second_id).unwrap(), &second));
    assert!(snapshots.start_listener(second.clone()).is_err());
    let changed = Arc::new(Snapshot { capacity: 4, ..second.as_ref().clone() });
    assert!(
        snapshots
            .publish(BTreeMap::from([
                (first.id.clone(), next),
                (second_id.clone(), changed)
            ]))
            .is_err()
    );
    assert!(Arc::ptr_eq(&snapshots.get(&second_id).unwrap(), &second));
    drop(two);
}

#[test]
fn removed_listener_keeps_its_bind_until_its_generation_drains() {
    let snapshots = ListenerSnapshots::default();
    let original = groups(8080, 2, 1).pop_first().unwrap().1;
    let running = snapshots.start_listener(original.clone()).unwrap();
    let replacement_id: ListenerId = "replacement".parse().unwrap();
    let replacement = Arc::new(Snapshot {
        id: replacement_id.clone(),
        ..original.as_ref().clone()
    });
    let candidate =
        BTreeMap::from([(replacement_id.clone(), replacement.clone())]);
    assert!(snapshots.publish(candidate.clone()).is_err());
    assert!(snapshots.get(&original.id).is_some());
    snapshots.publish(BTreeMap::new()).unwrap();
    assert!(snapshots.get(&original.id).is_none());
    assert!(snapshots.start_listener(replacement.clone()).is_err());
    drop(running);
    snapshots.publish(candidate).unwrap();
    let _replacement = snapshots.start_listener(replacement).unwrap();
    assert!(snapshots.get(&replacement_id).is_some());
}
