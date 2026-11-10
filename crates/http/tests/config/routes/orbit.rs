use super::*;

#[test]
fn fleet_and_application_are_independent() {
    let route: OrbitRouteConfig = serde_yaml::from_str(
        "fleet_name: apps\napplication: example-application-with-a-long-name\n",
    )
    .unwrap();
    assert_eq!(route.fleet_name.as_str(), "apps");
    assert_eq!(route.application, "example-application-with-a-long-name");
    assert_eq!(route.policy, LinkPolicy::First);
}

#[test]
fn selection_policy_is_explicit_and_unknown_policies_are_rejected() {
    for (name, expected) in
        [("first", LinkPolicy::First), ("round_robin", LinkPolicy::RoundRobin)]
    {
        let yaml =
            format!("fleet: apps\napplication: example\npolicy: {name}\n");
        let route: OrbitRouteConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(route.policy, expected);
        assert_eq!(serde_yaml::to_string(&expected).unwrap().trim(), name);
    }
    assert!(
        serde_yaml::from_str::<OrbitRouteConfig>(
            "fleet: apps\napplication: example\npolicy: least_cpu\n"
        )
        .is_err()
    );
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
