use super::TransportPolicy;

#[test]
fn accepts_upgrade_and_connect_spellings() {
    let aliases: TransportPolicy =
        serde_yaml::from_str("h1: { websocket: true }\nh2: { connect: true }")
            .unwrap();
    let canonical: TransportPolicy = serde_yaml::from_str(
        "h1: { upgrade: true }\nh2: { connect_protocol: true }",
    )
    .unwrap();
    assert!(aliases.h1.upgrade && aliases.h2.connect_protocol);
    assert_eq!(aliases, canonical);
}

#[cfg(feature = "http3")]
#[test]
fn webtransport_requires_an_opt_in_and_h3() {
    for key in ["webtransport", "web_transport", "wt"] {
        let policy: TransportPolicy = serde_yaml::from_str(&format!(
            "protocols: [h3]\nh3: {{ {key}: true }}"
        ))
        .unwrap();
        assert!(policy.allows_webtransport());
    }
    for yaml in [
        "protocols: [h3]",
        "protocols: [h3]\nh3: { wt: false }",
        "protocols: [h2, h1]\nh3: { wt: true }",
    ] {
        let policy: TransportPolicy = serde_yaml::from_str(yaml).unwrap();
        assert!(!policy.allows_webtransport());
    }
}
