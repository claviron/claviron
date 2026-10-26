use super::*;

#[test]
fn fleet_broadcast_uses_the_selected_route_not_an_unrelated_root_owner() {
    let host: HostConfig = serde_yaml::from_str(
        "root: public\nroutes:\n  /app:\n    broadcast: { target: fleet }\n  /app/private:\n    path: private\n",
    )
    .unwrap();
    assert!(!is_fleet(&host));
    assert!(is_fleet_route(&host, "/app/example-key"));
    assert!(!is_fleet_route(&host, "/app/private/example-key"));
    assert!(!is_fleet_route(&host, "/apple/example-key"));
    assert!(!is_fleet_route(&host, "/login"));

    let root: HostConfig =
        serde_yaml::from_str("root: public\nbroadcast: { target: fleet }\n")
            .unwrap();
    assert!(is_fleet_route(&root, "/app/example-key"));
}
