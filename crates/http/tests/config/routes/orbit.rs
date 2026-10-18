use super::*;

#[test]
fn fleet_and_application_are_independent() {
    let route: OrbitRouteConfig = serde_yaml::from_str(
        "fleet_name: apps\napplication: example-application-with-a-long-name\n",
    )
    .unwrap();
    assert_eq!(route.fleet_name.as_str(), "apps");
    assert_eq!(route.application, "example-application-with-a-long-name");
    assert!(route.path.is_none());
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
