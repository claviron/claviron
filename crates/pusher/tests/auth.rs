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

#[test]
fn audit_pusher_body_digest_covers_empty_replacements() {
    let key = PusherAppKey::try_from("example-key".to_owned()).unwrap();
    let secret =
        PusherAppSecret::try_from("example-secret".to_owned()).unwrap();
    let now = 1_700_000_000;
    let query = authentication_query_at(
        "POST",
        "/apps/1/events",
        b"signed",
        &key,
        &secret,
        now,
    );
    for (body, expected) in [
        (b"signed".as_slice(), Ok(())),
        (b"".as_slice(), Err(AuthenticationError::BodyDigest)),
        (b"changed".as_slice(), Err(AuthenticationError::BodyDigest)),
    ] {
        assert_eq!(
            authenticate_at(
                "POST",
                "/apps/1/events",
                Some(&query),
                body,
                &key,
                &secret,
                now
            ),
            expected
        );
    }
    let empty_query = authentication_query_at(
        "GET",
        "/apps/1/channels",
        b"",
        &key,
        &secret,
        now,
    );
    assert_eq!(
        authenticate_at(
            "GET",
            "/apps/1/channels",
            Some(&empty_query),
            b"",
            &key,
            &secret,
            now
        ),
        Ok(())
    );
    // A supplied digest is part of the signature, including a digest of empty bytes.
    let mut parameters = BTreeMap::from([
        ("auth_key".into(), key.as_str().to_owned()),
        ("auth_timestamp".into(), now.to_string()),
        ("auth_version".into(), "1.0".into()),
        ("body_md5".into(), hex::encode(Md5::digest(b""))),
    ]);
    let canonical = canonical_query(&parameters);
    let mut signer =
        Hmac::<Sha256>::new_from_slice(secret.expose().as_bytes()).unwrap();
    signer.update(format!("POST\n/apps/1/events\n{canonical}").as_bytes());
    parameters.insert(
        "auth_signature".into(),
        hex::encode(signer.finalize().into_bytes()),
    );
    let query = canonical_query(&parameters);
    // canonical_query omits auth_signature, so append the signed value for transport.
    let query =
        format!("{query}&auth_signature={}", parameters["auth_signature"]);
    assert_eq!(
        authenticate_at(
            "POST",
            "/apps/1/events",
            Some(&query),
            b"",
            &key,
            &secret,
            now
        ),
        Ok(())
    );
    assert_eq!(
        authenticate_at(
            "GET",
            "/apps/1/channels",
            Some(&empty_query),
            b"unexpected",
            &key,
            &secret,
            now
        ),
        Err(AuthenticationError::Missing("body_md5"))
    );
}
