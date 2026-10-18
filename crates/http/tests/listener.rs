use std::collections::BTreeMap;

use super::*;

#[test]
fn identity_is_stable_and_case_sensitive() {
    let id: ListenerId = "public-tls.v4_1".parse().unwrap();
    assert_eq!(id.as_str(), "public-tls.v4_1");
    assert_eq!(id.to_string(), "public-tls.v4_1");
    assert_eq!(id, id.clone());
    assert_ne!(
        "Public".parse::<ListenerId>().unwrap(),
        "public".parse().unwrap()
    );
    let yaml = serde_yaml::to_string(&id).unwrap();
    assert_eq!(serde_yaml::from_str::<ListenerId>(&yaml).unwrap(), id);
}

#[test]
fn invalid_names_are_rejected_at_rust_and_yaml_boundaries() {
    for raw in [
        "",
        " ",
        " public",
        "public ",
        "public/tls",
        "public:443",
        "-public",
        "ü",
        "a\nb",
    ] {
        assert!(raw.parse::<ListenerId>().is_err(), "accepted {raw:?}");
        let yaml = serde_yaml::to_string(raw).unwrap();
        assert!(
            serde_yaml::from_str::<ListenerId>(&yaml).is_err(),
            "accepted {raw:?}"
        );
    }
}

#[test]
fn declaration_keys_and_selectors_use_the_same_type() {
    let declarations: BTreeMap<ListenerId, u16> =
        serde_yaml::from_str("local: 8080\npublic-tls: 443").unwrap();
    let selector: ListenerId = serde_yaml::from_str("local").unwrap();
    assert_eq!(declarations.get(&selector), Some(&8080));
    assert!(
        serde_yaml::from_str::<BTreeMap<ListenerId, u16>>("bad/name: 8080")
            .is_err()
    );
}

#[test]
fn closed_protocol_choices_are_validated_by_serde() {
    assert_eq!(
        serde_yaml::from_str::<ListenerTransport>("tcp").unwrap(),
        ListenerTransport::Tcp
    );
    assert_eq!(
        serde_yaml::from_str::<ListenerTransport>("quic").unwrap(),
        ListenerTransport::Quic
    );
    assert_eq!(
        serde_yaml::from_str::<HttpProtocol>("h2").unwrap(),
        HttpProtocol::H2
    );
    assert!(serde_yaml::from_str::<ListenerTransport>("udp").is_err());
    assert!(serde_yaml::from_str::<HttpProtocol>("https").is_err());
}
