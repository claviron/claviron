use super::*;

fn id(value: u64) -> SessionId {
    value.try_into().unwrap()
}

#[tokio::test]
async fn webtransport_saturated_session_does_not_block_other_input() {
    let mut routes = SessionStreams::new(NonZeroUsize::new(1).unwrap());
    let mut slow = routes.register(id(0));
    let mut other = routes.register(id(4));
    routes.route(id(0), "queued").unwrap();
    assert_eq!(routes.route(id(0), "refused"), Err("refused"));
    assert_eq!(routes.route(id(8), "unassociated"), Err("unassociated"));
    routes.route(id(4), "other session").unwrap();
    assert_eq!(other.recv().await, Some("other session"));
    assert_eq!(slow.recv().await, Some("queued"));
    routes.route(id(0), "resumed").unwrap();
    assert_eq!(slow.recv().await, Some("resumed"));
}

#[tokio::test]
async fn webtransport_session_completion_and_peer_close_release_receivers() {
    let mut routes = SessionStreams::new(NonZeroUsize::new(2).unwrap());
    let mut first = routes.register(id(0));
    let mut second = routes.register(id(4));
    routes.route(id(0), 1).unwrap();
    routes.route(id(4), 2).unwrap();
    routes.remove(id(0));
    assert_eq!(first.recv().await, Some(1));
    assert_eq!(first.recv().await, None);
    assert_eq!(routes.route(id(0), 3), Err(3));
    routes.close();
    assert_eq!(second.recv().await, Some(2));
    assert_eq!(second.recv().await, None);
    assert_eq!(routes.route(id(4), 4), Err(4));
}

#[test]
fn webtransport_dropped_handler_does_not_leave_a_routing_entry() {
    let mut routes = SessionStreams::new(NonZeroUsize::new(1).unwrap());
    drop(routes.register(id(0)));
    assert_eq!(routes.route(id(0), 1), Err(1));
    assert!(routes.sessions.is_empty());
    drop(routes.register(id(4)));
    routes.prune_closed();
    assert!(routes.sessions.is_empty());
}
