use std::sync::Arc;

use claviron_types::CertificateKind;
use claviron_types::ssl::{generators, parsers};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::UnixTime;
use std::path::PathBuf;

use super::*;

#[test]
fn signer_material_issues_verifiable_identities_without_files() {
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
    let authority = CertSigner {
        authority: CertAuthority::new(
            PathBuf::new(),
            vec![CertificateDer::from(ca_der)],
        ),
        key_der: key.serialize_der(),
    };
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
    roots.add(authority.authority().roots()[0].clone()).unwrap();
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
    let subject =
        "urn:uuid:00000000-0000-4000-8000-000000000007".parse().unwrap();
    let identified = authority
        .issue_client_certificate("display-name.test", &[], Some(&subject), 1)
        .unwrap();
    let peer = claviron_types::ssl::peer::peer_identity(&identified.certs_der)
        .unwrap();
    assert_eq!(peer.common_names, ["display-name.test"]);
    assert_eq!(peer.subject_uris, [subject.to_string()]);
    verifier
        .verify_client_cert(
            &CertificateDer::from(identified.certs_der),
            &[],
            UnixTime::now(),
        )
        .unwrap();
    let mut catalog = claviron_types::permission::PermissionCatalog::new();
    catalog.register_name("service.read").unwrap();
    let scoped = authority
        .issue_client_certificate_with_permissions(
            "scoped.test",
            &[],
            Some(&subject),
            1,
            &catalog,
            &["service.read".into()],
        )
        .unwrap();
    let peer =
        claviron_types::ssl::peer::peer_identity(&scoped.certs_der).unwrap();
    let permissions = claviron_types::ssl::permission::from_uris(
        peer.subject_uris.iter().map(String::as_str),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        permissions.iter().map(|name| name.as_str()).collect::<Vec<_>>(),
        ["service.read"]
    );
    verifier
        .verify_client_cert(
            &CertificateDer::from(scoped.certs_der),
            &[],
            UnixTime::now(),
        )
        .unwrap();
    assert!(
        authority
            .issue_client_certificate_with_permissions(
                "scoped.test",
                &[],
                None,
                1,
                &catalog,
                &["unknown".into()]
            )
            .is_err()
    );
    assert!(
        authority
            .issue_client_certificate(
                "scoped.test",
                &["urn:claviron:permissions:v1:service.read".into()],
                None,
                1
            )
            .is_err()
    );
    let reserved = "urn:claviron:permissions:v1:service.read".parse().unwrap();
    assert!(
        authority
            .issue_client_certificate("scoped.test", &[], Some(&reserved), 1)
            .is_err()
    );
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
    let intermediate = CertSigner {
        authority: CertAuthority::new(
            PathBuf::new(),
            vec![CertificateDer::from(intermediate.certs_der)],
        ),
        key_der: intermediate.key_der,
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
            &[intermediate.authority().roots()[0].clone()],
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
        (CertSigner {
            authority: CertAuthority::new(PathBuf::new(), vec![]),
            key_der: vec![]
        })
        .issue_certificate(CertificateKind::Client, "client.test", &[], 1, None)
        .is_err()
    );
}
