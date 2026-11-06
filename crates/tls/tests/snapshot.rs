use claviron_hosting::{Certificate, CertificateAlias, CertificateKind};
use claviron_types::ssl::generators;
use time::OffsetDateTime;

use super::*;

/// The CN of a certificate need not be a host name; its aliases are what it
/// serves. Skipping on the CN would lose them at every boot and full reload.
#[test]
fn a_cn_that_is_not_a_host_name_still_serves_its_aliases() {
    use std::str::FromStr;

    let cn = "Example Server";
    assert!(Sni::from_str(cn).is_err());

    let ca_key = generators::generate_ca_key().unwrap();
    let ca_cert = generators::generate_ca_cert(
        &"authority.test".parse().unwrap(),
        &ca_key,
        30,
        &Default::default(),
    )
    .unwrap();
    let key = generators::generate_private_key().unwrap();
    let cert = generators::generate_leaf_cert(
        CertificateKind::Server,
        &"server.example.test".parse().unwrap(),
        &key,
        &ca_cert,
        &ca_key,
        1,
        &["server.example.test"],
    )
    .unwrap();
    let now = OffsetDateTime::now_utc();
    let hosted = HostedCertificate {
        certificate: Certificate {
            id: 1,
            kind: CertificateKind::Server,
            issuer_id: None,
            cn: cn.into(),
            certs_der: cert.to_vec(),
            key_der: key.serialize_der(),
            expires_at: now + time::Duration::days(1),
            issued_at: now,
            updated_at: None,
            revoked_at: None,
            enabled: true,
        },
        aliases: vec![CertificateAlias {
            id: 1,
            certificate_id: 1,
            hostname: "server.example.test".into(),
            created_at: now,
        }],
    };

    let mut snapshot = Snapshot::new();
    assert_eq!(snapshot.populate_from_hosted(hosted), 1);
    assert!(snapshot.cert_map.contains_key("server.example.test"));
    assert_eq!(
        snapshot.owners.get("server.example.test").map(String::as_str),
        Some("example server"),
    );
}
