use super::*;

fn root() -> (CertificateDer<'static>, rcgen::KeyPair) {
    let key = generators::generate_ca_key().unwrap();
    let cert = generators::generate_ca_cert(
        &"root.test".parse().unwrap(),
        &key,
        30,
        &Default::default(),
    )
    .unwrap();
    (cert, key)
}

/// An import reads the PEM a file holds.
fn pem(certificate: &[u8]) -> Vec<u8> {
    super::super::formatters::certificate_chain_to_pem(certificate)
        .unwrap()
        .into_bytes()
}

#[test]
fn detects_target_role_and_issuer_independent_of_chain_order() {
    let (ca, ca_key) = root();
    let root =
        plan_import(&pem(&ca), ca_key.serialize_pem().as_bytes(), &[]).unwrap();
    assert_eq!(root.kind, CertificateKind::Ca);
    assert_eq!(root.issuer_id, None);
    assert!(root.aliases.is_empty());
    let issuers = vec![IssuerCandidate { id: 7, certs_der: root.certs_der }];
    for kind in [CertificateKind::Server, CertificateKind::Client] {
        let key = generators::generate_private_key().unwrap();
        let leaf = generators::generate_leaf_cert(
            kind,
            &"leaf.test".parse().unwrap(),
            &key,
            &ca,
            &ca_key,
            2,
            &["alias.test".to_string()],
        )
        .unwrap();
        let unordered = [pem(&ca), pem(&leaf), pem(&ca)].concat();
        let plan =
            plan_import(&unordered, key.serialize_pem().as_bytes(), &issuers)
                .unwrap();
        assert_eq!(plan.kind, kind);
        assert_eq!(plan.issuer_id, Some(7));
        assert_eq!(plan.common_name.as_str(), "leaf.test");
        assert!(plan.aliases.contains(&"leaf.test".parse().unwrap()));
        assert!(plan.aliases.contains(&"alias.test".parse().unwrap()));
        assert!(plan.certs_der.starts_with(&leaf));
        let exported_key = private_key_to_pem(&plan.key_der).unwrap();
        let roundtrip =
            generators::read_private_key_from_pem(exported_key.as_bytes())
                .unwrap();
        assert_eq!(
            roundtrip.subject_public_key_info(),
            key.subject_public_key_info()
        );
        assert!(
            plan_import(
                &pem(&leaf),
                ca_key.serialize_pem().as_bytes(),
                &issuers
            )
            .is_err()
        );
        assert!(
            plan_import(&unordered, key.serialize_pem().as_bytes(), &[])
                .is_err()
        );
    }
}

#[test]
fn ca_subjects_are_authority_names_not_host_aliases() {
    let key = generators::generate_ca_key().unwrap();
    let key_pem = key.serialize_pem();
    let ca = generators::generate_ca_cert(
        &"Roma CA".parse().unwrap(),
        &key,
        30,
        &Default::default(),
    )
    .unwrap();
    let root = plan_import(&pem(&ca), key_pem.as_bytes(), &[]).unwrap();
    assert_eq!(root.common_name.as_str(), "Roma CA");
    assert!(root.aliases.is_empty());
    let issuers = [IssuerCandidate { id: 7, certs_der: root.certs_der }];
    let intermediate_key = generators::generate_ca_key().unwrap();
    let intermediate_pem = intermediate_key.serialize_pem();
    let intermediate = pem(&generators::generate_intermediate_ca_cert(
        &intermediate_key,
        &ca,
        &key,
        &"Roma Issuing CA".parse().unwrap(),
        2,
        &super::super::Organization::default(),
        Some(0),
    )
    .unwrap());
    let plan =
        plan_import(&intermediate, intermediate_pem.as_bytes(), &issuers)
            .unwrap();
    assert_eq!(plan.common_name.as_str(), "Roma Issuing CA");
    assert_eq!(plan.issuer_id, Some(7));
    assert!(plan.aliases.is_empty());
    assert!(
        plan_import(&intermediate, intermediate_pem.as_bytes(), &[]).is_err()
    );
}

#[test]
fn rejects_unrelated_chains_and_ambiguous_database_authorities() {
    let (ca, ca_key) = root();
    let plan =
        plan_import(&pem(&ca), ca_key.serialize_pem().as_bytes(), &[]).unwrap();
    let key = generators::generate_private_key().unwrap();
    let leaf = pem(&generators::generate_client_cert(
        &"client.test".parse().unwrap(),
        &key,
        &ca,
        &ca_key,
        2,
        &[] as &[String],
    )
    .unwrap());
    let issuers = vec![
        IssuerCandidate { id: 1, certs_der: plan.certs_der.clone() },
        IssuerCandidate { id: 2, certs_der: plan.certs_der },
    ];
    assert!(
        plan_import(&leaf, key.serialize_pem().as_bytes(), &issuers).is_err()
    );
    let (other, _) = root();
    assert!(
        plan_import(
            &[leaf, pem(&ca), pem(&other)].concat(),
            key.serialize_pem().as_bytes(),
            &issuers[..1]
        )
        .is_err()
    );
    let mut damaged = issuers[0].certs_der.clone();
    *damaged.last_mut().unwrap() ^= 1;
    assert!(
        plan_import(&pem(&damaged), ca_key.serialize_pem().as_bytes(), &[])
            .is_err()
    );
}
