use super::*;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

fn assert_pending(future: Pin<&mut impl Future>) {
    assert!(future.poll(&mut Context::from_waker(Waker::noop())).is_pending());
}

#[tokio::test]
async fn slow_write_preserves_read_shutdown_and_admitted_records() {
    let (stream, mut peer) = tokio::io::duplex(16);
    let (outbound, queued) = mpsc::channel(1);
    let (events, mut happened) = mpsc::channel(1);
    let (stop, stopped) = oneshot::channel();
    outbound.send(Outbound::Text("x".repeat(64))).await.unwrap();
    let driving = drive(stream, queued, events, 128, async {
        stopped.await.unwrap();
        Close(1001, "server stopping".to_owned(), false)
    });
    tokio::pin!(driving);
    assert_pending(driving.as_mut()); // The first record is partially written.
    outbound.send(Outbound::Binary(vec![1, 2, 3])).await.unwrap();
    let blocked = outbound.send(Outbound::Text("not admitted".to_owned()));
    tokio::pin!(blocked);
    assert_pending(blocked.as_mut());

    write_message(&mut peer, &Message::Text("ping".to_owned())).await.unwrap();
    assert_pending(driving.as_mut());
    assert!(
        matches!(happened.try_recv(), Ok(Event::Message(data, false)) if data == b"ping")
    );

    stop.send(()).unwrap();
    assert_pending(driving.as_mut());
    assert!(outbound.is_closed());
    assert!(blocked.await.is_err());
    assert!(
        matches!(happened.try_recv(), Ok(Event::Close(1001, reason, false)) if reason == "server stopping")
    );

    let receiving = async {
        let mut records = MessageReader::new();
        assert_eq!(
            records.read(&mut peer).await.unwrap(),
            Some(Message::Text("x".repeat(64)))
        );
        assert_eq!(
            records.read(&mut peer).await.unwrap(),
            Some(Message::Binary(vec![1, 2, 3]))
        );
        assert_eq!(
            records.read(&mut peer).await.unwrap(),
            Some(Message::Close {
                code: 1001,
                reason: "server stopping".to_owned()
            })
        );
        assert_eq!(records.read(&mut peer).await.unwrap(), None);
    };
    tokio::join!(driving, receiving);
}

#[tokio::test]
async fn full_event_queue_keeps_pending_message_before_shutdown_close() {
    let (stream, mut peer) = tokio::io::duplex(64);
    let (_outbound, queued) = mpsc::channel(1);
    let (events, mut happened) = mpsc::channel(1);
    let (stop, stopped) = oneshot::channel();
    let driving = drive(stream, queued, events, 128, async {
        stopped.await.unwrap();
        Close(1001, "stopping".to_owned(), false)
    });
    tokio::pin!(driving);
    for text in ["first", "pending", "not admitted"] {
        write_message(&mut peer, &Message::Text(text.to_owned()))
            .await
            .unwrap();
    }
    assert_pending(driving.as_mut());
    stop.send(()).unwrap();
    assert_pending(driving.as_mut());
    let receiving = async {
        for expected in [b"first".as_slice(), b"pending".as_slice()] {
            assert!(
                matches!(happened.recv().await, Some(Event::Message(data, false)) if data == expected)
            );
        }
        assert!(matches!(
            happened.recv().await,
            Some(Event::Close(1001, _, false))
        ));
        assert!(happened.recv().await.is_none());
    };
    tokio::join!(driving, receiving);
}

#[tokio::test]
async fn failed_writer_releases_producers_and_notifies_close() {
    let (stream, peer) = tokio::io::duplex(16);
    let (outbound, queued) = mpsc::channel(1);
    let (events, mut happened) = mpsc::channel(1);
    outbound.send(Outbound::Text("hello".to_owned())).await.unwrap();
    drop(peer);
    let driving = drive(stream, queued, events, 128, std::future::pending());
    tokio::pin!(driving);
    assert!(matches!(
        driving.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(())
    ));
    assert!(outbound.is_closed());
    assert!(matches!(happened.try_recv(), Ok(Event::Close(1006, _, false))));
}
