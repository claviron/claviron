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
    assert!(snapshots.publish(BTreeMap::new()).is_err());
    assert!(Arc::ptr_eq(&updated, &snapshots.get(&id).unwrap()));
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
