use super::*;

#[test]
fn client_authority_selects_exactly_one_source() {
    for (yaml, source) in [
        ("authority: {local: fixture}", "local: fixture"),
        ("authority: {cn: 'Fixture CA'}", "cn: 'Fixture CA'"),
    ] {
        let policy: ClientAuthPolicy = serde_yaml::from_str(yaml).unwrap();
        let expected: AuthorityEntry = serde_yaml::from_str(source).unwrap();
        assert_eq!(policy.authority, Some(expected));
    }
    assert!(
        serde_yaml::from_str::<ClientAuthPolicy>("ca_bundle: ca.pem").is_ok()
    );
    for yaml in [
        "authority: {local: ../fixture}",
        "authority: {local: fixture, cn: 'Fixture CA'}",
        "authority: {local: fixture}\nca_bundle: ca.pem",
        "authority: fixture",
    ] {
        assert!(
            serde_yaml::from_str::<ClientAuthPolicy>(yaml).is_err(),
            "{yaml}"
        );
    }
}
