use super::{Message, read_message, write_message};

#[tokio::test]
async fn consumer_limit_rejects_the_header_before_waiting_for_the_payload() {
    let mut records = super::MessageReader::with_limit(8);
    let mut header = [1, 255, 255, 255, 255].as_slice();
    let error = records.read(&mut header).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

    let expected = Message::Text("12345678".to_owned());
    let mut wire = Vec::new();
    write_message(&mut wire, &expected).await.unwrap();
    let mut records = super::MessageReader::with_limit(8);
    assert_eq!(
        records.read(&mut wire.as_slice()).await.unwrap(),
        Some(expected)
    );
}

/// Every kind survives the record, in order, and the end of the direction
/// between records reads as the end rather than as an error.
#[tokio::test]
async fn messages_survive_the_record_and_end_cleanly() {
    let sent = vec![
        Message::Text("héllo".to_owned()),
        Message::Binary(vec![0, 1, 2, 255]),
        Message::Close { code: 1000, reason: "bye".to_owned() },
    ];
    let mut wire = Vec::new();
    for message in &sent {
        write_message(&mut wire, message).await.unwrap();
    }

    let mut reader = wire.as_slice();
    let mut received = Vec::new();
    while let Some(message) = read_message(&mut reader).await.unwrap() {
        received.push(message);
    }
    assert_eq!(received, sent);
}

/// A read dropped halfway through a record — what `select!` does when the
/// other branch wins — leaves the record for the next read to finish.
#[tokio::test]
async fn a_cancelled_read_keeps_its_place() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    use tokio::io::AsyncWriteExt;

    let message = Message::Text("across two writes".to_owned());
    let mut record = Vec::new();
    write_message(&mut record, &message).await.unwrap();
    let (mut writer, mut reader) = tokio::io::duplex(record.len());
    let mut records = super::MessageReader::new();

    writer.write_all(&record[..3]).await.unwrap();
    {
        let read = std::pin::pin!(records.read(&mut reader));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(read.poll(&mut context), Poll::Pending));
    }
    writer.write_all(&record[3..]).await.unwrap();
    assert_eq!(records.read(&mut reader).await.unwrap(), Some(message));
}
