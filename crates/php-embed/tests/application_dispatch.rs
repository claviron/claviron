use super::*;

fn control() -> Arc<Control> {
    Arc::new(Control::new(Arc::new(Notify::new()), Arc::new(Notify::new())))
}

#[test]
fn application_dispatch_keeps_context_owned_until_cleanup() {
    let control = control();
    let (sender, mut receiver) = mpsc::channel(1);
    let mailbox = Mailbox::new(&sender, Arc::clone(&control));
    let mut first = Some(1);
    assert!(!mailbox.try_send(&mut first), "boot is not receive readiness");
    control.publish_ready();
    assert!(mailbox.try_send(&mut first));
    assert_eq!(receiver.try_recv().unwrap(), 1);
    let mut second = Some(2);
    assert!(
        !mailbox.try_send(&mut second),
        "dequeue does not finish PHP cleanup"
    );
    assert_eq!(second, Some(2));
    control.complete(true);
    assert!(mailbox.try_send(&mut second));
    assert_eq!(receiver.try_recv().unwrap(), 2);
    assert_eq!(control.served.load(Ordering::Acquire), 1);
}

#[test]
fn application_dispatch_old_snapshots_do_not_keep_mailboxes_open() {
    let control = control();
    control.publish_ready();
    let (sender, mut receiver) = mpsc::channel::<usize>(1);
    let published = arc_swap::ArcSwap::from_pointee(vec![Mailbox::new(
        &sender,
        Arc::clone(&control),
    )]);
    let old_snapshot = published.load_full();
    control.retire();
    published.store(Arc::new(Vec::new()));
    drop(sender);
    assert_eq!(
        receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    );
    let mut request = Some(1);
    assert!(!old_snapshot[0].try_send(&mut request));
    assert_eq!(request, Some(1));
}

#[test]
fn application_dispatch_accepted_mailbox_work_drains_after_retirement() {
    let control = control();
    control.publish_ready();
    let (sender, mut receiver) = mpsc::channel(1);
    let mailbox = Mailbox::new(&sender, Arc::clone(&control));
    assert!(mailbox.try_send(&mut Some(7)));
    control.retire();
    drop(sender);
    assert!(control.busy(), "stop preserves the accepted context's ownership");
    assert_eq!(receiver.try_recv().unwrap(), 7);
    control.complete(true);
    assert!(!control.busy());
    assert!(!control.claim());
    assert_eq!(
        receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    );
}

#[test]
fn application_dispatch_in_progress_publication_drains_after_stop() {
    let control = control();
    control.publish_ready();
    let (sender, mut receiver) = mpsc::channel(1);
    let mailbox = Mailbox::new(&sender, Arc::clone(&control));
    assert!(control.claim());
    let publishing = mailbox.sender.upgrade().unwrap();
    control.retire();
    drop(sender);
    assert_eq!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    publishing.try_send(13).unwrap();
    drop(publishing);
    assert_eq!(receiver.try_recv().unwrap(), 13);
    control.complete(true);
    assert_eq!(
        receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    );
    assert!(!mailbox.try_send(&mut Some(14)));
}

#[test]
fn application_dispatch_closed_mailbox_restores_unsent_work() {
    let control = control();
    control.publish_ready();
    let (sender, receiver) = mpsc::channel(1);
    let mailbox = Mailbox::new(&sender, Arc::clone(&control));
    drop(receiver);
    let mut request = Some(9);
    assert!(!mailbox.try_send(&mut request));
    assert_eq!(request, Some(9));
    assert!(!control.busy());
}

#[test]
fn application_dispatch_visits_every_context_when_cursor_wraps() {
    let channels = (0..3).map(|_| mpsc::channel(1)).collect::<Vec<_>>();
    let controls = (0..3).map(|_| control()).collect::<Vec<_>>();
    for control in &controls {
        control.publish_ready();
    }
    let mailboxes = channels
        .iter()
        .zip(&controls)
        .map(|((sender, _), control)| Mailbox::new(sender, Arc::clone(control)))
        .collect::<Vec<_>>();
    assert!(controls[0].claim());
    assert!(controls[1].claim());
    let cursor = AtomicUsize::new(usize::MAX);
    assert!(send_available(&mailboxes, &cursor, &mut Some(21)));
    assert!(controls[2].busy());
    assert_eq!(cursor.load(Ordering::Acquire), 0);
}
