use super::*;

/// A box of this process takes a direct send and a group send, a full box
/// refuses the first and is passed over by the second, and another process's
/// publication reaches the box its name points at.
#[tokio::test]
async fn boxes_and_groups_in_one_process() {
    let groups = Groups::new(3);
    let a = groups.open("specific", Some(1), None);
    let b = groups.open("specific", None, None);
    assert!(a.starts_with("specific.n3!"), "{a}");

    groups.send(None, &a, Bytes::from_static(b"one")).unwrap();
    assert_eq!(
        groups.send(None, &a, Bytes::from_static(b"two")),
        Err(GroupError::Full)
    );
    assert_eq!(groups.receive(&a).await.unwrap(), Bytes::from_static(b"one"));

    groups.add("room", &a).unwrap();
    groups.add("room", &b).unwrap();
    groups.send(None, &a, Bytes::from_static(b"fill")).unwrap();
    assert_eq!(
        groups.group_send(None, "room", Bytes::from_static(b"all")).unwrap(),
        1
    );
    assert_eq!(groups.receive(&b).await.unwrap(), Bytes::from_static(b"all"));

    // As if another process had published a direct send to `b`.
    let mut payload = vec![OP_SEND];
    payload.extend_from_slice(&(b.len() as u16).to_le_bytes());
    payload.extend_from_slice(b.as_bytes());
    payload.extend_from_slice(b"remote");
    groups.deliver(&payload);
    assert_eq!(
        groups.receive(&b).await.unwrap(),
        Bytes::from_static(b"remote")
    );

    assert_eq!(
        Groups::owner("plain.channel"),
        Err(GroupError::NotProcessSpecific)
    );
    assert_eq!(
        groups.add("room", "specific.n9!1"),
        Err(GroupError::UnknownChannel)
    );
}
