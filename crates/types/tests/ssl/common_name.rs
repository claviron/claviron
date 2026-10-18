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
