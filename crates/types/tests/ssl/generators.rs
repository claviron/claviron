use super::{generate_private_key, read_private_key_from_der};

#[test]
fn persisted_private_key_round_trips_without_rotation() {
    let original = generate_private_key().unwrap().serialize_der();
    let restored =
        read_private_key_from_der(&original).unwrap().serialize_der();

    assert_eq!(restored, original);
}

#[test]
fn client_issuance_requires_matching_ca_key_and_bounded_validity() {
    use super::*;
    let ca_key = generate_ca_key().unwrap().serialize_pem();
    let ca =
        generate_ca_cert("authority.test", ca_key.as_bytes(), 30, None, None)
            .unwrap();
    let key = generate_private_key().unwrap();
    let key_pem = key.serialize_pem();
    let aliases = vec!["other.test".to_string()];
    let signed = generate_client_cert(
        "client.test",
        key_pem.as_bytes(),
        &ca,
        ca_key.as_bytes(),
        1,
        &aliases,
    )
    .unwrap();
    let der =
        crate::ssl::parsers::concat_chain_bytes_from_pem(&signed).unwrap();
    let cert = crate::ssl::parsers::take_first_cert_from_der(&der).unwrap();
    assert!(!cert.is_ca());
    let eku = cert.extended_key_usage().unwrap().unwrap();
    assert!(eku.value.client_auth);
    assert!(!eku.value.server_auth);
    assert_eq!(cert.public_key().raw, key.subject_public_key_info());
    assert_eq!(
        cert.validity().not_after.timestamp()
            - cert.validity().not_before.timestamp(),
        86400
    );
    assert!(
        generate_client_cert(
            "client.test",
            key_pem.as_bytes(),
            &ca,
            ca_key.as_bytes(),
            0,
            &aliases
        )
        .is_err()
    );
    assert!(
        generate_client_cert(
            "client.test",
            key_pem.as_bytes(),
            &ca,
            ca_key.as_bytes(),
            u32::MAX,
            &aliases
        )
        .is_err()
    );
    assert!(
        generate_client_cert(
            "client.test",
            key_pem.as_bytes(),
            &ca,
            ca_key.as_bytes(),
            31,
            &aliases
        )
        .is_err()
    );
    assert!(
        generate_client_cert(
            "client.test",
            key_pem.as_bytes(),
            &ca,
            key_pem.as_bytes(),
            1,
            &aliases
        )
        .is_err()
    );
    // A correctly paired end-entity certificate is still not a signing CA.
    assert!(
        generate_client_cert(
            "child.test",
            ca_key.as_bytes(),
            &signed,
            key_pem.as_bytes(),
            1,
            &aliases
        )
        .is_err()
    );
}

#[test]
fn subject_uris_require_an_absolute_ascii_identity_without_invalid_escapes() {
    use crate::ssl::SubjectUri;
    for value in [
        "urn:uuid:00000000-0000-4000-8000-000000000007",
        "spiffe://example.test/user/7",
    ] {
        assert_eq!(value.parse::<SubjectUri>().unwrap().as_str(), value);
    }
    for value in [
        "",
        "operator",
        "urn:",
        ":user",
        "1urn:user",
        "urn:user name",
        "urn:üser",
        "urn:user%ZZ",
        "urn:<user>",
    ] {
        assert!(value.parse::<SubjectUri>().is_err(), "{value}");
    }
}
