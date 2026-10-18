use rcgen::{
    BasicConstraints, CertificateParams, CustomExtension, DistinguishedName,
    DnType, IsCa, KeyUsagePurpose,
};

use super::*;

#[test]
fn renewal_preserves_encoded_identity_and_all_extensions_without_rotating_key()
{
    let key = generators::generate_ca_key().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "Fixture CA");
    dn.push(DnType::OrganizationName, "Fixture Organization");
    dn.push(DnType::OrganizationalUnitName, "Trust");
    dn.push(DnType::CountryName, "TR");
    params.distinguished_name = dn;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let mut custom = CustomExtension::from_oid_content(
        &[1, 3, 6, 1, 4, 1, 55555, 1],
        vec![0x05, 0],
    );
    custom.set_criticality(true);
    params.custom_extensions.push(custom);
    let cert = params.self_signed(&key).unwrap();
    let original = cert.der().as_ref();
    let renewed =
        renew_certificate(original, &key.serialize_der(), None, 7).unwrap();
    let old = parsers::take_first_cert_from_der(original).unwrap();
    let new = parsers::take_first_cert_from_der(&renewed).unwrap();
    assert_eq!(new.subject().as_raw(), old.subject().as_raw());
    assert_eq!(new.issuer().as_raw(), old.issuer().as_raw());
    assert_eq!(new.public_key().raw, old.public_key().raw);
    assert_eq!(new.extensions(), old.extensions());
    assert_ne!(new.raw_serial(), old.raw_serial());
    assert_eq!(
        new.validity().not_after.timestamp()
            - new.validity().not_before.timestamp(),
        7 * 86400
    );
    new.verify_signature(None).unwrap();
    assert!(
        renew_certificate(
            original,
            &generators::generate_private_key().unwrap().serialize_der(),
            None,
            7
        )
        .is_err()
    );
    assert!(
        renew_certificate(original, &key.serialize_der(), None, 0).is_err()
    );

    let leaf_key = generators::generate_private_key().unwrap();
    let leaf = generators::generate_client_cert(
        "client.test",
        leaf_key.serialize_pem().as_bytes(),
        cert.pem().as_bytes(),
        key.serialize_pem().as_bytes(),
        1,
        &["alias.test"],
    )
    .unwrap();
    let leaf_der = parsers::concat_chain_bytes_from_pem(&leaf).unwrap();
    let renewed = renew_certificate(
        &leaf_der,
        &leaf_key.serialize_der(),
        Some((original, &key.serialize_der())),
        2,
    )
    .unwrap();
    let old_leaf = parsers::take_first_cert_from_der(&leaf_der).unwrap();
    let new_leaf = parsers::take_first_cert_from_der(&renewed).unwrap();
    assert_eq!(new_leaf.subject().as_raw(), old_leaf.subject().as_raw());
    assert_eq!(new_leaf.extensions(), old_leaf.extensions());
    new_leaf.verify_signature(Some(old.public_key())).unwrap();
    assert!(
        renew_certificate(&leaf_der, &leaf_key.serialize_der(), None, 2)
            .is_err()
    );
}
