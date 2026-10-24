use super::*;
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, SanType};

fn certificate(with_uris: bool) -> Vec<u8> {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "operator, one");
    params.distinguished_name = dn;
    if with_uris {
        params.subject_alt_names = vec![
            SanType::URI("urn:example:subject:one".try_into().unwrap()),
            SanType::URI("urn:example:other:two".try_into().unwrap()),
            SanType::DnsName("example.test".try_into().unwrap()),
        ];
    }
    params.self_signed(&KeyPair::generate().unwrap()).unwrap().der().to_vec()
}

#[test]
fn peer_identity_preserves_structured_cn_uri_candidates_and_exact_credential() {
    let der = certificate(true);
    let peer = peer_identity(&der).unwrap();
    assert_eq!(peer.common_names, ["operator, one"]);
    assert_eq!(
        peer.subject_uris,
        ["urn:example:subject:one", "urn:example:other:two"]
    );
    assert_eq!(&*peer.certificate, der);
    assert!(!format!("{peer:?}").contains("certificate:"));
}

#[test]
fn peer_identity_cn_only_does_not_invent_a_subject_or_authorization() {
    let peer = peer_identity(&certificate(false)).unwrap();
    assert_eq!(peer.common_names, ["operator, one"]);
    assert!(peer.subject_uris.is_empty());
    assert!(peer_identity(b"malformed certificate").is_err());
}
