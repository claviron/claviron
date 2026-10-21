use super::*;

#[test]
fn subscription_guards_keep_overlapping_topics_until_the_last_owner_leaves() {
    let interest = Arc::new(LinkInterest::default());
    let changes = interest.watch();
    let first = interest.add(["example.policy.v1"]);
    let second = interest.add(["example.policy.v1", "example.status.v1"]);
    drop(first);
    assert_eq!(
        &**changes.borrow(),
        &["example.policy.v1".to_owned(), "example.status.v1".to_owned()]
    );
    drop(second);
    assert!(changes.borrow().is_empty());
}

#[test]
fn interest_is_connection_local_even_when_topic_names_match() {
    let first = Arc::new(LinkInterest::default());
    let second = Arc::new(LinkInterest::default());
    let first_changes = first.watch();
    let second_changes = second.watch();
    let subscription = first.add(["example.policy.v1"]);
    assert_eq!(first_changes.borrow().len(), 1);
    assert!(second_changes.borrow().is_empty());
    drop(subscription);
    assert!(first_changes.borrow().is_empty());
}
