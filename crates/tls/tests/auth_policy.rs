use super::*;
use crate::handshake::HandshakePolicy;

fn policy(yaml: &str) -> Result<HandshakePolicy, String> {
    serde_yaml::from_str(yaml).map_err(|error| error.to_string())
}

#[test]
fn client_auth_is_present_with_exactly_one_verifier_or_absent() {
    assert_eq!(policy("{}").unwrap().client_auth, None);

    for (yaml, source) in [
        ("authority: {local: fixture}", "local: fixture"),
        ("authority: {cn: 'Fixture CA'}", "cn: 'Fixture CA'"),
    ] {
        let auth: ClientAuth = serde_yaml::from_str(yaml).unwrap();
        let expected: AuthorityEntry = serde_yaml::from_str(source).unwrap();
        assert_eq!(auth.trust(), &ClientTrust::Authority(expected));
    }
    let auth: ClientAuth = serde_yaml::from_str(
        "{ca_bundle: ca.pem, organization: Example, unit: Operations}",
    )
    .unwrap();
    assert_eq!(
        auth,
        ClientAuth {
            trust: ClientTrust::Bundle(Path::new("ca.pem").into()),
            subject: Some(PolicyConfig::new("Example", "Operations")),
        }
    );

    for (yaml, error) in [
        ("client_auth: {}", "client_auth needs a verifier"),
        (
            "client_auth: {organization: Example, unit: Operations}",
            "client_auth needs a verifier",
        ),
        (
            "client_auth: {authority: {local: fixture}, ca_bundle: ca.pem}",
            "client_auth selects both authority and ca_bundle",
        ),
        (
            "client_auth: {authority: fixture}",
            "client_auth.authority must select local or cn",
        ),
        (
            "client_auth: {ca_bundle: ca.pem, organization: Example}",
            "organization and unit must be configured together",
        ),
        (
            "client_auth: {mode: required, ca_bundle: ca.pem}",
            "unknown field `mode`",
        ),
        (
            "client_auth: required",
            "expected a client_auth mapping that selects authority or ca_bundle",
        ),
    ] {
        let refused = policy(yaml).unwrap_err();
        assert!(refused.contains(error), "{yaml}: {refused}");
    }
}
