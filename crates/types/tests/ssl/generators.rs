use super::{generate_private_key, read_private_key_from_der};

#[test]
fn enrollment_csr_proves_key_possession_without_selecting_certificate_claims() {
    use super::*;

    let ca_key = generate_ca_key().unwrap();
    let ca = generate_ca_cert(
        &"authority.test".parse().unwrap(),
        &ca_key,
        30,
        &Default::default(),
    )
    .unwrap();
    let device = generate_private_key().unwrap();
    let csr = generate_server_csr(
        &device,
        &"untrusted.test".parse().unwrap(),
        vec!["untrusted.test".into()],
        &Organization::default(),
    )
    .unwrap();
    let issue = |csr| {
        sign_client_csr(
            &"operator".parse().unwrap(),
            csr,
            &ca,
            &ca_key,
            1,
            vec!["urn:uuid:a15bc548-16e1-4a10-b772-ad00c3b66a52".into()],
        )
    };
    let der = issue(csr.der().as_ref()).unwrap();
    let certificate =
        crate::ssl::parsers::take_first_cert_from_der(&der).unwrap();
    let identity = crate::ssl::peer::peer_identity(&der).unwrap();
    assert_eq!(*identity.common_names, ["operator"].map(Box::<str>::from));
    assert_eq!(
        *identity.subject_uris,
        ["urn:uuid:a15bc548-16e1-4a10-b772-ad00c3b66a52"].map(Box::<str>::from)
    );
    assert_eq!(certificate.public_key().raw, device.subject_public_key_info());
    let eku = certificate.extended_key_usage().unwrap().unwrap();
    assert!(eku.value.client_auth && !eku.value.server_auth);
    let mut tampered = csr.der().as_ref().to_vec();
    *tampered.last_mut().unwrap() ^= 1;
    assert!(issue(&tampered).is_err());
}

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
    let ca_key = generate_ca_key().unwrap();
    let ca = generate_ca_cert(
        &"authority.test".parse().unwrap(),
        &ca_key,
        30,
        &Default::default(),
    )
    .unwrap();
    let key = generate_private_key().unwrap();
    let aliases = vec!["other.test".to_string()];
    let der = generate_client_cert(
        &"client.test".parse().unwrap(),
        &key,
        &ca,
        &ca_key,
        1,
        &aliases,
    )
    .unwrap();
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
            &"client.test".parse().unwrap(),
            &key,
            &ca,
            &ca_key,
            0,
            &aliases
        )
        .is_err()
    );
    assert!(
        generate_client_cert(
            &"client.test".parse().unwrap(),
            &key,
            &ca,
            &ca_key,
            u32::MAX,
            &aliases
        )
        .is_err()
    );
    assert!(
        generate_client_cert(
            &"client.test".parse().unwrap(),
            &key,
            &ca,
            &ca_key,
            31,
            &aliases
        )
        .is_err()
    );
    assert!(
        generate_client_cert(
            &"client.test".parse().unwrap(),
            &key,
            &ca,
            &key,
            1,
            &aliases
        )
        .is_err()
    );
    // A correctly paired end-entity certificate is still not a signing CA.
    assert!(
        generate_client_cert(
            &"child.test".parse().unwrap(),
            &ca_key,
            &der,
            &key,
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
