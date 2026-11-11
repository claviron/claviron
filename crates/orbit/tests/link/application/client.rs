use super::*;
use std::task::Poll;

#[tokio::test]
async fn dropping_a_lent_reader_cancels_before_the_upload_returns() {
    let streams = orbitive::stream::Streams::new(
        Arc::new(orbitive::Fleet::join("loan-cancel", 2).unwrap()),
        orbitive::stream::Incarnation::new(1),
    )
    .unwrap();
    let (origin, ticket) = streams.create().unwrap();
    let remote = streams.open(ticket).unwrap();
    let (read, write) = origin.split();
    let hot = Arc::new(Hot::default());
    let loan = Arc::new(Loan {
        hot: Arc::clone(&hot),
        ticket,
        halves: Mutex::new((None, None)),
    });
    let reader = LentRead { half: Some(read), loan: Arc::clone(&loan) };
    let writer = LentWrite { half: Some(write), loan };
    let (_incoming, mut outgoing) = remote.split();
    drop(reader);
    outgoing.peer_closed().await.unwrap();
    assert!(hot.slots.lock().unwrap().is_empty());
    drop(writer);
    drop((_incoming, outgoing));
    let (mut endpoint, _) = hot.slots.lock().unwrap().pop_front().unwrap();
    endpoint.rearm(&streams).unwrap();
    let replacement = streams.open(ticket).unwrap();
    let (_read, mut write) = replacement.split();
    let mut waiting = std::pin::pin!(write.peer_closed());
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
}

fn reply_wait() -> (ReplyWait, Endpoint, ReadHalf, Arc<Waiting>) {
    let streams = orbitive::stream::Streams::new(
        Arc::new(orbitive::Fleet::join("reply-wait", 2).unwrap()),
        orbitive::stream::Incarnation::new(1),
    )
    .unwrap();
    let (origin, ticket) = streams.create().unwrap();
    let remote = streams.open(ticket).unwrap();
    let (read, _write) = origin.split();
    let waiting = Arc::new(Waiting::default());
    let (sender, receiver) = oneshot::channel();
    let owner = LaneOwner { lane: 0, pid: None, incarnation: 1 };
    waiting.insert(1, owner, sender);
    let reply = ReplyWait {
        reply: receiver,
        reset: read.reset_signal(),
        waiting: Arc::clone(&waiting),
        correlation: 1,
        completed: false,
        withdrawal: None,
    };
    (reply, remote, read, waiting)
}

#[tokio::test]
async fn a_reset_after_buffered_fin_ends_a_missing_reply_head() {
    use tokio::io::AsyncWriteExt;
    let (reply, mut remote, _read, waiting) = reply_wait();
    remote.write_all(b"body").await.unwrap();
    remote.finish().unwrap();
    remote.reset();
    assert!(matches!(
        reply.head().await,
        Err(Error::Stream(orbitive::stream::Error::Reset))
    ));
    assert!(waiting.senders.lock().unwrap().is_empty());
}

#[test]
fn abandoning_a_reply_wait_removes_its_correlation() {
    let (reply, _remote, _read, waiting) = reply_wait();
    drop(reply);
    assert!(waiting.senders.lock().unwrap().is_empty());
}

#[tokio::test]
async fn buffered_fin_does_not_complete_a_head_wait_or_consume_body_bytes() {
    use tokio::io::AsyncWriteExt;
    let (reply, mut remote, read, waiting) = reply_wait();
    remote.write_all(b"body").await.unwrap();
    remote.finish().unwrap();
    let mut head = Box::pin(reply.head());
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(head.as_mut().poll(&mut cx).is_pending());
    waiting
        .take(1)
        .unwrap()
        .send(ExchangeReply {
            correlation: 1,
            status: 200,
            headers: Vec::new(),
            body: ExchangeBody::Empty,
        })
        .unwrap();
    assert_eq!(head.await.unwrap().status, 200);
    let mut bytes = [0; 4];
    assert_eq!(read.try_read(&mut bytes).unwrap(), 4);
    assert_eq!(&bytes, b"body");
}

#[tokio::test]
async fn a_delivered_head_wins_over_a_later_body_reset() {
    let (reply, mut remote, _read, waiting) = reply_wait();
    waiting
        .take(1)
        .unwrap()
        .send(ExchangeReply {
            correlation: 1,
            status: 200,
            headers: Vec::new(),
            body: ExchangeBody::Empty,
        })
        .unwrap();
    remote.reset();
    assert_eq!(reply.head().await.unwrap().status, 200);
    assert!(waiting.senders.lock().unwrap().is_empty());
}

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

struct CreatorPressure {
    segment: Arc<ApplicationSegment>,
    worker: super::super::Joined,
    bodies: ApplicationBodies,
    receiver: super::super::ApplicationReceiver,
    client: Arc<ApplicationClient>,
}

impl CreatorPressure {
    fn new(tag: &str) -> Self {
        Self::with_capacity(tag, 1)
    }

    fn with_capacity(
        tag: &str,
        capacity: usize,
    ) -> Self {
        let name = format!("cs{tag}{:x}", std::process::id());
        let spec = orbitive::link::LinkSpec::new(
            4,
            super::super::LINK_SPEC.inbox_kind,
            orbitive::link::InboxGeometry::new(4, 1024),
            orbitive::stream::StreamSpec::new(
                super::super::LINK_SPEC.streams.kind,
                capacity,
                1024,
            ),
        );
        let segment = Arc::new(
            ApplicationSegment::for_fleet_with_spec(
                &name,
                spec,
                &Access::OwnerOnly,
            )
            .unwrap(),
        );
        let worker =
            segment.join_with_capacity("example", "rust", 1, Some(1)).unwrap();
        let bodies =
            ApplicationBodies::for_segment(&segment, worker.lane, 1).unwrap();
        let receiver = segment.application_receiver(worker.lane).unwrap();
        let client = ApplicationClient::join_segment(
            Arc::clone(&segment),
            "caller",
            2,
            None,
            None,
        )
        .unwrap();
        Self { segment, worker, bodies, receiver, client }
    }

    async fn occupy(&mut self) -> (Pending, Endpoint) {
        let pending =
            self.client.target("example").send(Exchange::default()).unwrap();
        let mut frame = Vec::new();
        loop {
            let received = self.receiver.next(&mut frame).await.unwrap();
            if !matches!(received, Received::Work(_)) {
                continue;
            }
            if let Some((_, execution)) = self
                .segment
                .accept_frame(self.worker.lane, received, &frame)
                .unwrap()
            {
                let exchange =
                    Exchange::decode(Bytes::copy_from_slice(&frame)).unwrap();
                let remote =
                    self.bodies.accept(exchange.stream.unwrap()).unwrap();
                // Isolate creator pressure: the receiver admission slot is free,
                // while the body transport deliberately still has both owners.
                drop(execution);
                return (pending, remote);
            }
        }
    }

    fn assert_receiver_is_free(&self) {
        let reservation = self
            .segment
            .reserve_work(self.worker.lane, self.client.lane)
            .unwrap();
        assert!(reservation.is_some());
        drop(reservation);
    }
}

impl Drop for CreatorPressure {
    fn drop(&mut self) {
        self.client.leave();
        let _ = self.segment.inbox().release_lane(self.worker.lane);
        let _ = self.bodies.unlink();
        let _ = self.segment.unlink();
    }
}

#[tokio::test]
async fn creator_wait_handles_both_release_orders_without_holding_receiver_admission()
 {
    for (tag, remote_first) in [("a", true), ("b", false)] {
        let mut case = CreatorPressure::new(tag);
        let (pending, remote) = case.occupy().await;
        let target = case.client.target("example");
        assert!(matches!(
            target.send(Exchange::default()),
            Err(Error::Stream(orbitive::stream::Error::Full { .. }))
        ));
        let mut sending = Box::pin(target.send_wait(Exchange::default()));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(sending.as_mut().poll(&mut cx).is_pending());
        case.assert_receiver_is_free();
        // Control frames still pass through the receiver while the caller parks.
        let command = ControlCommand {
            application: "example".into(),
            name: "status".into(),
            payload: vec![],
            target: None,
            origin: None,
        };
        case.segment
            .inbox()
            .write(case.worker.lane, &command.encode().unwrap())
            .unwrap();
        let mut frame = Vec::new();
        loop {
            if case.receiver.next(&mut frame).await.unwrap() == Received::Frame
            {
                assert_eq!(frame_kind(&frame).unwrap(), FrameKind::Control);
                break;
            }
        }
        if remote_first {
            drop(remote);
            assert!(
                sending.as_mut().poll(&mut cx).is_pending(),
                "the local loan still holds the endpoint"
            );
            drop(pending);
        } else {
            drop(pending);
            assert!(
                sending.as_mut().poll(&mut cx).is_pending(),
                "the peer still holds its claim"
            );
            case.assert_receiver_is_free();
            drop(remote);
        }
        let sent = sending.await.unwrap();
        assert_eq!(
            sent.correlation, 2,
            "failed attempts allocate no correlation and publish no exchange"
        );
    }
}

#[tokio::test]
async fn creator_wait_cancels_without_publication_and_wakes_when_the_client_leaves()
 {
    let mut case = CreatorPressure::new("c");
    let (pending, remote) = case.occupy().await;
    let target = case.client.target("example");
    let mut cancelled = Box::pin(target.send_wait(Exchange::default()));
    assert!(
        cancelled
            .as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    drop(cancelled);
    case.assert_receiver_is_free();
    assert!(case.client.waiting.senders.lock().unwrap().contains_key(&1));
    assert_eq!(case.client.waiting.senders.lock().unwrap().len(), 1);
    let mut sending = Box::pin(target.send_wait(Exchange::default()));
    assert!(
        sending
            .as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    case.client.leave();
    assert!(matches!(sending.await, Err(Error::NoConsumer(_))));
    drop((pending, remote));
}

#[test]
fn the_last_returned_local_half_wakes_the_creator_waiter() {
    use std::sync::atomic::AtomicUsize;
    use std::task::{Wake, Waker};
    #[derive(Default)]
    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let streams = orbitive::stream::Streams::new(
        Arc::new(orbitive::Fleet::join("loan-return", 2).unwrap()),
        orbitive::stream::Incarnation::new(1),
    )
    .unwrap();
    let (origin, ticket) = streams.create().unwrap();
    let _remote = streams.open(ticket).unwrap();
    let (read, write) = origin.split();
    let hot = Arc::new(Hot::default());
    let loan = Arc::new(Loan {
        hot: Arc::clone(&hot),
        ticket,
        halves: Mutex::new((None, None)),
    });
    let read = LentRead { half: Some(read), loan: Arc::clone(&loan) };
    let write = LentWrite { half: Some(write), loan };
    let count = Arc::new(Count::default());
    let waker = Waker::from(Arc::clone(&count));
    let mut returned = Box::pin(hot.returned.notified());
    assert!(
        returned.as_mut().poll(&mut Context::from_waker(&waker)).is_pending()
    );
    drop(read);
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    drop(write);
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    assert_eq!(hot.slots.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn creator_wait_expires_with_the_callers_queue_budget_without_publishing()
{
    let mut case = CreatorPressure::new("d");
    let (_pending, _remote) = case.occupy().await;
    let target = case.client.target("example");
    // An already-expired caller budget tests cancellation without a sleep or
    // an implementation-owned timer. The pipeline owns the real queue budget.
    let deadline = tokio::time::Instant::now();
    assert!(
        tokio::time::timeout_at(
            deadline,
            target.send_wait(Exchange::default())
        )
        .await
        .is_err()
    );
    case.assert_receiver_is_free();
    assert_eq!(case.client.waiting.senders.lock().unwrap().len(), 1);
    assert_eq!(case.client.correlation.load(Ordering::Relaxed), 2);
}

#[derive(Default)]
struct CreatorWakeCount(std::sync::atomic::AtomicUsize);

impl std::task::Wake for CreatorWakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn creator_pressure_has_one_observer_and_head_cancellation_hands_off_readiness()
 {
    let mut case = CreatorPressure::new("e");
    let (pending, remote) = case.occupy().await;
    let target = case.client.target("example");
    let head_count = Arc::new(CreatorWakeCount::default());
    let next_count = Arc::new(CreatorWakeCount::default());
    let head_waker = std::task::Waker::from(Arc::clone(&head_count));
    let next_waker = std::task::Waker::from(Arc::clone(&next_count));
    let mut head = Box::pin(target.send_wait(Exchange::default()));
    let mut next = Box::pin(target.send_wait(Exchange::default()));
    assert!(
        head.as_mut().poll(&mut Context::from_waker(&head_waker)).is_pending()
    );
    assert!(
        next.as_mut().poll(&mut Context::from_waker(&next_waker)).is_pending()
    );
    assert!(matches!(
        target.send(Exchange::default()),
        Err(Error::Stream(orbitive::stream::Error::WouldBlock))
    ));
    assert!(matches!(
        case.client.send("example", Exchange::default()),
        Err(Error::Stream(orbitive::stream::Error::WouldBlock))
    ));
    drop(pending);
    assert!(head_count.0.load(Ordering::SeqCst) > 0);
    assert_eq!(
        next_count.0.load(Ordering::SeqCst),
        0,
        "local capacity wakes only the inspector, not FIFO followers"
    );
    assert!(
        head.as_mut().poll(&mut Context::from_waker(&head_waker)).is_pending()
    );
    drop(remote);
    // Capacity becomes ready before handoff. The next inspector must discover
    // it even if the cancelled head consumed the release notification.
    drop(head);
    assert!(next_count.0.load(Ordering::SeqCst) > 0);
    let sent = next.await.unwrap();
    assert_eq!(sent.correlation, 2);
    assert!(!case.client.creator.contended());
}

#[tokio::test]
async fn creator_rechecks_do_not_repeat_receiver_reservations() {
    let mut case = CreatorPressure::new("f");
    let (_pending, _remote) = case.occupy().await;
    let target = case.client.target("example");
    let inbox = case.segment.inbox();
    let mut head = Box::pin(target.send_wait(Exchange::default()));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(head.as_mut().poll(&mut cx).is_pending());
    // The first direct attempt may roll back. Subsequent creator probes must
    // not touch shared receiver admission. Its real generation records returns.
    inbox.watch_capacity(case.worker.lane, case.client.lane).unwrap();
    let before = inbox.capacity_generation(case.client.lane).unwrap();
    let mut queued = Box::pin(target.send_wait(Exchange::default()));
    for _ in 0..8 {
        case.client.hot.returned.notify_waiters();
        assert!(head.as_mut().poll(&mut cx).is_pending());
        assert!(queued.as_mut().poll(&mut cx).is_pending());
        assert_eq!(
            inbox.capacity_generation(case.client.lane).unwrap(),
            before
        );
    }
    drop(head);
    assert!(queued.as_mut().poll(&mut cx).is_pending());
    assert_eq!(inbox.capacity_generation(case.client.lane).unwrap(), before);
    drop(queued);
    assert!(!case.client.creator.contended());
    assert_eq!(case.client.correlation.load(Ordering::Relaxed), 2);
    case.assert_receiver_is_free();
}

#[tokio::test]
async fn a_full_receiver_is_refused_once_the_creator_turn_frees_and_holds_none()
{
    let case = CreatorPressure::new("g");
    let other =
        case.segment.join_with_capacity("other", "rust", 3, Some(1)).unwrap();
    let held =
        case.segment.reserve_work(case.worker.lane, case.client.lane).unwrap();
    let turn = case.client.creator_turn().await;
    let target = case.client.target("example");
    let mut blocked = Box::pin(target.send_wait(Exchange::default()));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    // Queued behind the creator turn another exchange holds.
    assert!(blocked.as_mut().poll(&mut cx).is_pending());
    drop(turn);
    // Then refused at once: a sender never waits for receiver capacity.
    assert!(matches!(
        blocked.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::CapacityFull))
    ));
    assert!(
        !case.client.creator.contended(),
        "a refusal holds no creator turn"
    );
    let sent = case.client.target("other").send(Exchange::default()).unwrap();
    assert_eq!(sent.correlation, 1);
    drop(blocked);
    drop(held);
    case.segment.inbox().release_lane(other.lane).unwrap();
}

#[tokio::test]
async fn queued_destination_withdrawal_and_close_interrupt_without_creator_release()
 {
    let mut case = CreatorPressure::new("h");
    let (_pending, _remote) = case.occupy().await;
    let other =
        case.segment.join_with_capacity("other", "rust", 3, Some(1)).unwrap();
    let target = case.client.target("example");
    let other_target = case.client.target("other");
    let mut head = Box::pin(target.send_wait(Exchange::default()));
    let mut queued = Box::pin(other_target.send_wait(Exchange::default()));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(head.as_mut().poll(&mut cx).is_pending());
    assert!(queued.as_mut().poll(&mut cx).is_pending());
    case.segment.inbox().release_lane(other.lane).unwrap();
    // Deliver the same notification the directory reader emits, without
    // scheduling that unrelated reader in this acquisition unit test.
    case.client.membership_changed().notify_waiters();
    assert!(matches!(queued.await, Err(Error::NoConsumer(_))));
    let mut next = Box::pin(target.send_wait(Exchange::default()));
    assert!(next.as_mut().poll(&mut cx).is_pending());
    case.client.leave();
    assert!(matches!(next.await, Err(Error::NoConsumer(_))));
    assert!(matches!(head.await, Err(Error::NoConsumer(_))));
    assert!(!case.client.creator.contended());
}

#[tokio::test]
async fn queued_timeout_leaves_the_inspector_and_successor_intact() {
    let mut case = CreatorPressure::new("i");
    let (pending, remote) = case.occupy().await;
    let target = case.client.target("example");
    let mut head = Box::pin(target.send_wait(Exchange::default()));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(head.as_mut().poll(&mut cx).is_pending());
    assert!(
        tokio::time::timeout_at(
            tokio::time::Instant::now(),
            target.send_wait(Exchange::default())
        )
        .await
        .is_err()
    );
    assert!(case.client.creator.contended());
    let mut next = Box::pin(target.send_wait(Exchange::default()));
    assert!(next.as_mut().poll(&mut cx).is_pending());
    drop(head);
    drop((pending, remote));
    assert_eq!(next.await.unwrap().correlation, 2);
    assert!(!case.client.creator.contended());
}

#[tokio::test]
async fn creator_turn_does_not_limit_active_exchanges_to_one() {
    let mut case = CreatorPressure::with_capacity("j", 2);
    let (first, far_first) = case.occupy().await;
    let (second, far_second) = case.occupy().await;
    let target = case.client.target("example");
    let mut a = Box::pin(target.send_wait(Exchange::default()));
    let mut b = Box::pin(target.send_wait(Exchange::default()));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(a.as_mut().poll(&mut cx).is_pending());
    assert!(b.as_mut().poll(&mut cx).is_pending());
    drop((first, far_first, second, far_second));
    let a = a.await.unwrap();
    // Admit the first publication without completing its creator endpoint.
    let mut frame = Vec::new();
    loop {
        let received = case.receiver.next(&mut frame).await.unwrap();
        if !matches!(received, Received::Work(_)) {
            continue;
        }
        if let Some((_, execution)) = case
            .segment
            .accept_frame(case.worker.lane, received, &frame)
            .unwrap()
        {
            drop(execution);
            break;
        }
    }
    let b = b.await.unwrap();
    assert_eq!((a.correlation, b.correlation), (3, 4));
    assert!(!case.client.creator.contended());
}

#[tokio::test]
async fn concurrent_refusals_do_not_cycle_through_creator_turns() {
    let mut case = CreatorPressure::new("k");
    let (pending, remote) = case.occupy().await;
    let held =
        case.segment.reserve_work(case.worker.lane, case.client.lane).unwrap();
    let target = case.client.target("example");
    let mut a = Box::pin(target.send_wait(Exchange::default()));
    let mut b = Box::pin(target.send_wait(Exchange::default()));
    // Establish creator contention before either caller can inspect receiver
    // capacity, as if another application currently owns the creator turn.
    let turn = case.client.creator_turn().await;
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(a.as_mut().poll(&mut cx).is_pending());
    assert!(b.as_mut().poll(&mut cx).is_pending());
    drop((pending, remote, turn));
    // Each is refused once it reaches the turn; neither keeps it.
    let mut refused = 0;
    for _ in 0..4 {
        for sending in [&mut a, &mut b] {
            if matches!(
                sending.as_mut().poll(&mut cx),
                Poll::Ready(Err(Error::CapacityFull))
            ) {
                refused += 1;
            }
        }
        if refused == 2 {
            break;
        }
    }
    assert_eq!(refused, 2);
    assert!(!case.client.creator.contended());
    drop((a, b, held));
    case.assert_receiver_is_free();
    assert_eq!(case.client.correlation.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn creator_probe_discards_a_never_opened_loan_without_reusing_its_ticket()
{
    let mut case = CreatorPressure::new("l");
    let target = case.client.target("example");
    let pending = target.send(Exchange::default()).unwrap();
    let mut frame = Vec::new();
    let ticket = loop {
        let received = case.receiver.next(&mut frame).await.unwrap();
        if !matches!(received, Received::Work(_)) {
            continue;
        }
        if let Some((_, execution)) = case
            .segment
            .accept_frame(case.worker.lane, received, &frame)
            .unwrap()
        {
            let exchange =
                Exchange::decode(Bytes::copy_from_slice(&frame)).unwrap();
            drop(execution);
            break exchange.stream.unwrap();
        }
    };
    let mut next = Box::pin(target.send_wait(Exchange::default()));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(next.as_mut().poll(&mut cx).is_pending());
    drop(pending);
    let sent = next.await.unwrap();
    assert_eq!(sent.correlation, 2);
    assert!(matches!(
        case.bodies.accept(ticket),
        Err(Error::Stream(orbitive::stream::Error::Stale(_)))
    ));
    assert!(!case.client.creator.contended());
}

/// Bounded receivers are filled one at a time from the sender's own home, so a
/// receiver stays busy instead of every receiver getting a thin share; senders
/// with different lanes start at different receivers.
#[tokio::test]
async fn bounded_receivers_fill_from_each_senders_home() {
    let name = format!("ch{:x}", std::process::id() % 0x10_0000);
    let spec = orbitive::link::LinkSpec::new(
        8,
        super::super::LINK_SPEC.inbox_kind,
        orbitive::link::InboxGeometry::new(32, 1024),
        orbitive::stream::StreamSpec::new(
            super::super::LINK_SPEC.streams.kind,
            16,
            1024,
        ),
    );
    let segment = Arc::new(
        ApplicationSegment::for_fleet_with_spec(
            &name,
            spec,
            &Access::OwnerOnly,
        )
        .unwrap(),
    );
    let receivers: Vec<_> = (0..3)
        .map(|incarnation| {
            segment
                .join_with_capacity("example", "rust", incarnation + 1, Some(2))
                .unwrap()
        })
        .collect();
    let first = ApplicationClient::join_segment(
        Arc::clone(&segment),
        "a",
        10,
        None,
        None,
    )
    .unwrap();
    let lanes: Vec<usize> =
        receivers.iter().map(|joined| joined.lane).collect();
    let order = |client: &ApplicationClient| -> Vec<usize> {
        super::rendezvous_order(client.lane(), lanes.clone())
    };
    // Joining clients ask every application for its state with a control
    // frame; count only what the sends below add.
    let before: Vec<u64> = lanes
        .iter()
        .map(|&lane| segment.inbox().depth(lane).unwrap())
        .collect();
    let depth = |lane: usize| {
        let at = lanes.iter().position(|&known| known == lane).unwrap();
        segment.inbox().depth(lane).unwrap() - before[at]
    };

    // A sender fills its home to the bound, then the next in its own
    // rendezvous order, then the last.
    let target = first.target("example");
    let mut sent = Vec::new();
    for receiver in order(&first) {
        for expected in 1..=2 {
            sent.push(target.send(Exchange::default()).unwrap());
            assert_eq!(
                depth(receiver),
                expected,
                "receiver {receiver} fills first"
            );
        }
    }
    assert!(matches!(
        target.send(Exchange::default()),
        Err(Error::CapacityFull)
    ));

    drop(sent);
    let bodies =
        ApplicationBodies::for_segment(&segment, first.lane(), 10).unwrap();
    first.leave();
    for joined in &receivers {
        let _ = segment.inbox().release_lane(joined.lane);
    }
    let _ = bodies.unlink();
    let _ = segment.unlink();
}

/// A refusal is stated once per episode, and the episode ends only when the
/// directory generation moves: repeated refusals under one generation stay
/// silent, and the first one under the next is stated again.
#[test]
fn a_refusal_episode_is_stated_once_per_directory_generation() {
    let episode = Episode::default();
    assert!(episode.opens(7));
    assert!(!episode.opens(7));
    assert!(!episode.opens(7));
    assert!(episode.opens(8));
    assert!(!episode.opens(8));
    // Generation zero is a generation like any other, not "no episode".
    let episode = Episode::default();
    assert!(episode.opens(0));
    assert!(!episode.opens(0));
}
