use super::*;

#[test]
fn preserves_subject_identity_without_host_normalization() {
    let cn: CommonName = "Roma CA".parse().unwrap();
    assert_eq!(cn.as_str(), "Roma CA");
    assert!(crate::Host::from_str(cn.as_str()).is_err());
    let yaml = serde_yaml::to_string(&cn).unwrap();
    assert_eq!(serde_yaml::from_str::<CommonName>(&yaml).unwrap(), cn);
    assert!(CommonName::from_str(" ").is_err());
    assert!(CommonName::from_str("Roma\nCA").is_err());
    assert_eq!(CommonName::from_str("Özel CA").unwrap().as_str(), "Özel CA");
}

#[test]
fn reads_from_a_certificate_without_copying_and_accepts_any_host() {
    use crate::ssl::generators::{generate_ca_cert, generate_ca_key};
    use crate::ssl::parsers::{
        concat_chain_bytes_from_pem, take_first_cert_from_der,
    };

    let key = generate_ca_key().unwrap().serialize_pem();
    let name: CommonName = "Roma CA".parse().unwrap();
    let pem = generate_ca_cert(&name, key.as_bytes(), 1, &Default::default())
        .unwrap();
    let der = concat_chain_bytes_from_pem(&pem).unwrap();
    let certificate = take_first_cert_from_der(&der).unwrap();

    // The text is a slice of the certificate's own bytes.
    let read = CommonName::read(certificate.subject()).unwrap().unwrap();
    assert_eq!(read, "Roma CA");
    assert!(der.as_ptr_range().contains(&read.as_ptr()));
    assert_eq!(CommonName::try_from(certificate.subject()).unwrap(), name);

    let host: crate::Host = "app.example.test".parse().unwrap();
    assert_eq!(CommonName::from(&host).as_str(), "app.example.test");
}
