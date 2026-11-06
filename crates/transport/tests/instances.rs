use super::*;

#[test]
fn listener_declarations_preserve_retained_and_draining_identities() {
    let first: ListenerId = "first".parse().unwrap();
    let second: ListenerId = "second".parse().unwrap();
    let mut instances = ListenerInstances::default();
    instances.reconcile([&first, &second], |id| id.clone());
    let original = instances.declared[&first].clone();
    let sibling = instances.declared[&second].clone();
    instances.reconcile([&second], |_| panic!("unchanged instance recreated"));
    assert!(Arc::ptr_eq(&sibling, &instances.declared[&second]));
    assert!(instances.retired[&first].upgrade().is_some());
    instances
        .reconcile([&first, &second], |_| panic!("draining instance replaced"));
    assert!(Arc::ptr_eq(&original, &instances.declared[&first]));
    instances.reconcile([&second], |_| panic!("unchanged instance recreated"));
    drop(original);
    assert!(instances.retired[&first].upgrade().is_none());
    instances.reconcile([&second], |_| panic!("unchanged instance recreated"));
    assert!(!instances.retired.contains_key(&first));
}
