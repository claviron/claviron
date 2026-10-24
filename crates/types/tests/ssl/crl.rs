use super::*;
use crate::ssl::{generators, parsers};

#[test]
fn crl_updates_preserve_revocations_and_use_authority_expiry() {
    let key = generators::generate_ca_key().unwrap();
    let ca = generators::generate_ca_cert(
        "root.test",
        key.serialize_pem().as_bytes(),
        30,
        None,
        None,
    )
    .unwrap();
    let client = |name| {
        let client_key = generators::generate_private_key().unwrap();
        generators::generate_client_cert(
            name,
            client_key.serialize_pem().as_bytes(),
            &ca,
            key.serialize_pem().as_bytes(),
            1,
            &[] as &[&str],
        )
        .unwrap()
    };
    let first = client("first.test");
    let second = client("second.test");
    let now = OffsetDateTime::now_utc();
    let initial =
        revoke(&first, &ca, key.serialize_pem().as_bytes(), &[], now).unwrap();
    let updated = revoke(
        &second,
        &ca,
        key.serialize_pem().as_bytes(),
        initial.as_bytes(),
        now,
    )
    .unwrap();
    let repeated = revoke(
        &first,
        &ca,
        key.serialize_pem().as_bytes(),
        updated.as_bytes(),
        now,
    )
    .unwrap();
    let der = CertificateRevocationListDer::from_pem_slice(repeated.as_bytes())
        .unwrap();
    let (_, crl) = CertificateRevocationList::from_der(der.as_ref()).unwrap();
    assert_eq!(crl.iter_revoked_certificates().count(), 2);
    assert_eq!(crl.crl_number().unwrap().to_bytes_be(), [3]);
    let ca_der = parsers::sanitize_pem_content_to_der(&ca);
    let authority = parsers::take_first_cert_from_der(&ca_der).unwrap();
    crl.verify_signature(authority.public_key()).unwrap();
    assert_eq!(crl.next_update().unwrap(), authority.validity().not_after);
    assert_eq!(
        crl.iter_revoked_certificates()
            .next()
            .unwrap()
            .revocation_date
            .timestamp(),
        now.unix_timestamp()
    );
    let wrong_key = generators::generate_ca_key().unwrap();
    assert!(
        revoke(
            &first,
            &ca,
            wrong_key.serialize_pem().as_bytes(),
            repeated.as_bytes(),
            now
        )
        .is_err()
    );
    assert!(
        revoke(
            &first,
            &ca,
            key.serialize_pem().as_bytes(),
            b"malformed CRL",
            now
        )
        .is_err()
    );
}
