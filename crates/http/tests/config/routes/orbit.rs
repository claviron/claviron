use super::*;

#[test]
fn fleet_and_application_are_independent() {
    let route: OrbitRouteConfig = serde_yaml::from_str(
        "fleet_name: apps\napplication: example-application-with-a-long-name\n",
    )
    .unwrap();
    assert_eq!(route.fleet_name.as_str(), "apps");
    assert_eq!(route.application, "example-application-with-a-long-name");
    assert_eq!(route.balance, LinkBalance::First);
}

#[test]
fn balance_is_explicit_and_unknown_values_are_rejected() {
    for (name, expected) in [
        ("first", LinkBalance::First),
        ("round_robin", LinkBalance::RoundRobin),
    ] {
        let yaml =
            format!("fleet: apps\napplication: example\nbalance: {name}\n");
        let route: OrbitRouteConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(route.balance, expected);
        assert_eq!(serde_yaml::to_string(&expected).unwrap().trim(), name);
    }
    assert!(
        serde_yaml::from_str::<OrbitRouteConfig>(
            "fleet: apps\napplication: example\nbalance: least_cpu\n"
        )
        .is_err()
    );
    let removed = serde_yaml::from_str::<OrbitRouteConfig>(
        "fleet: apps\napplication: example\npolicy: round_robin\n",
    )
    .unwrap_err()
    .to_string();
    assert!(removed.contains("balance: first | round_robin"), "{removed}");
}

#[test]
fn destination_fleet_is_required_and_validated() {
    for text in [
        "application: example\n",
        "fleet_name: ''\napplication: example\n",
        "fleet_name: too-long-name\napplication: example\n",
        "fleet_name: /tmp/apps\napplication: example\n",
        "fleet_name: apps\napplication: example\nuser: example\n",
    ] {
        assert!(serde_yaml::from_str::<OrbitRouteConfig>(text).is_err());
    }
}
