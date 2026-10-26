use super::*;

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
    let hot = Arc::new(Mutex::new(Hot::default()));
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
    assert!(hot.lock().unwrap().slots.is_empty());
    drop(writer);
    drop((_incoming, outgoing));
    let (mut endpoint, _) = hot.lock().unwrap().slots.pop_front().unwrap();
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
    waiting.insert(1, sender);
    let reply = ReplyWait {
        reply: receiver,
        reset: read.reset_signal(),
        waiting: Arc::clone(&waiting),
        correlation: 1,
        completed: false,
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
            body: None,
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
            body: None,
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
