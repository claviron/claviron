use super::*;

#[test]
fn verifies_the_pusher_reference_request() {
    let key =
        PusherAppKey::try_from("278d425bdf160c739803".to_owned()).unwrap();
    let secret =
        PusherAppSecret::try_from("7ad3773142a6692b25b8".to_owned()).unwrap();
    let body = br#"{"name":"foo","channels":["project-3"],"data":"{\"some\":\"data\"}"}"#;
    let query = "auth_key=278d425bdf160c739803&auth_timestamp=1353088179&auth_version=1.0&body_md5=ec365a775a4cd0599faeb73354201b6f&auth_signature=da454824c97ba181a32ccc17a72625ba02771f50b50e1e7430e47a1f3f457e6c";
    authenticate_at(
        "POST",
        "/apps/3/events",
        Some(query),
        body,
        &key,
        &secret,
        1353088179,
    )
    .unwrap();
    assert_eq!(
        authentication_query_at(
            "POST",
            "/apps/3/events",
            body,
            &key,
            &secret,
            1353088179
        ),
        query
    );
}

#[test]
fn rejects_modified_bodies_and_expired_requests() {
    let key =
        PusherAppKey::try_from("278d425bdf160c739803".to_owned()).unwrap();
    let secret =
        PusherAppSecret::try_from("7ad3773142a6692b25b8".to_owned()).unwrap();
    let query = "auth_key=278d425bdf160c739803&auth_timestamp=1353088179&auth_version=1.0&body_md5=ec365a775a4cd0599faeb73354201b6f&auth_signature=da454824c97ba181a32ccc17a72625ba02771f50b50e1e7430e47a1f3f457e6c";
    assert_eq!(
        authenticate_at(
            "POST",
            "/apps/3/events",
            Some(query),
            b"changed",
            &key,
            &secret,
            1353088179
        ),
        Err(AuthenticationError::BodyDigest)
    );
    assert_eq!(
        authenticate_at(
            "POST",
            "/apps/3/events",
            Some(query),
            b"",
            &key,
            &secret,
            1353088780
        ),
        Err(AuthenticationError::Expired)
    );
}

#[test]
fn verifies_private_and_presence_channel_authorization() {
    let key =
        PusherAppKey::try_from("278d425bdf160c739803".to_owned()).unwrap();
    let secret =
        PusherAppSecret::try_from("7ad3773142a6692b25b8".to_owned()).unwrap();
    let private = "278d425bdf160c739803:58df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a4";
    verify_channel_authorization(
        Some(private),
        "1234.1234",
        "private-foobar",
        None,
        &key,
        &secret,
    )
    .unwrap();

    let channel_data = r#"{"user_id":10,"user_info":{"name":"Mr. Channels"}}"#;
    let presence = authorize_channel(
        "1234.1234",
        "presence-foobar",
        Some(channel_data),
        &key,
        &secret,
    );
    verify_channel_authorization(
        Some(&presence),
        "1234.1234",
        "presence-foobar",
        Some(channel_data),
        &key,
        &secret,
    )
    .unwrap();
}
