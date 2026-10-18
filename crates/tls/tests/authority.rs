use std::sync::Arc;

use claviron_types::CertificateKind;
use claviron_types::ssl::{generators, parsers};
use rustls::pki_types::UnixTime;

use super::*;

#[test]
fn database_material_issues_verifiable_identities_without_files() {
    let key = generators::generate_ca_key().unwrap();
    let ca = generators::generate_ca_cert(
        "authority.test",
        key.serialize_pem().as_bytes(),
        30,
        None,
        None,
    )
    .unwrap();
    let ca_der = parsers::concat_chain_bytes_from_pem(&ca).unwrap();
    let authority = CertAuthority {
        path: PathBuf::new(),
        roots: vec![CertificateDer::from(ca_der)],
        signer: Some(DatabaseSigner {
            issuer_id: 1,
            certificate_id: 1,
            name: "ca.test".into(),
            key_der: key.serialize_der(),
        }),
    };
    assert_eq!(authority.path(), None);
    let issued = authority
        .issue_certificate(
            CertificateKind::Client,
            "client.test",
            &["alias.test".into()],
            1,
            None,
        )
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(authority.roots()[0].clone()).unwrap();
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
    )
    .build()
    .unwrap();
    verifier
        .verify_client_cert(
            &CertificateDer::from(issued.certs_der),
            &[],
            UnixTime::now(),
        )
        .unwrap();
    let server = authority
        .issue_certificate(CertificateKind::Server, "server.test", &[], 1, None)
        .unwrap();
    assert!(
        parsers::take_first_cert_from_der(&server.certs_der)
            .unwrap()
            .extended_key_usage()
            .unwrap()
            .unwrap()
            .value
            .server_auth
    );
    let intermediate = authority
        .issue_certificate(
            CertificateKind::Ca,
            "intermediate.test",
            &[],
            10,
            Some(0),
        )
        .unwrap();
    let intermediate = CertAuthority {
        path: PathBuf::new(),
        roots: vec![CertificateDer::from(intermediate.certs_der)],
        signer: Some(DatabaseSigner {
            issuer_id: 2,
            certificate_id: 2,
            name: "intermediate.test".into(),
            key_der: intermediate.key_der,
        }),
    };
    let leaf = intermediate
        .issue_certificate(CertificateKind::Client, "client.test", &[], 1, None)
        .unwrap();
    let (_, chain_leaf) =
        x509_parser::parse_x509_certificate(&leaf.certs_der).unwrap();
    let len = chain_leaf.as_raw().len();
    verifier
        .verify_client_cert(
            &CertificateDer::from(leaf.certs_der[..len].to_vec()),
            &[intermediate.roots()[0].clone()],
            UnixTime::now(),
        )
        .unwrap();
    assert!(
        intermediate
            .issue_certificate(
                CertificateKind::Ca,
                "nested.test",
                &[],
                1,
                Some(0)
            )
            .is_err()
    );
    assert!(
        CertAuthority::new(PathBuf::new(), vec![])
            .issue_certificate(
                CertificateKind::Client,
                "client.test",
                &[],
                1,
                None
            )
            .is_err()
    );
}
