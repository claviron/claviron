use super::*;

#[test]
fn fleet_broadcast_uses_the_selected_route_not_an_unrelated_root_owner() {
    let host: HostConfig = serde_yaml::from_str(
        "root: public\nroutes:\n  /app:\n    broadcast: { target: fleet }\n  /app/private:\n    directory: private\n",
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

#[test]
fn a_retained_binding_admits_only_what_its_scope_still_declares() {
    let host: HostConfig = serde_yaml::from_str(
        "root: public\nroutes:\n  /app:\n    broadcast: { target: fleet }\n",
    )
    .unwrap();
    let path = "/app/example-key";
    let source = directive(selected(&host, path)).cloned().unwrap();
    assert!(still_declared(&host, path, &source));

    // The registries keep the binding after a reload removed the route, or
    // put a more specific route without it in front.
    for changed in [
        "root: public\n",
        "root: public\nroutes:\n  /app:\n    broadcast: { target: fleet }\n  /app/example-key:\n    directory: private\n",
    ] {
        let changed: HostConfig = serde_yaml::from_str(changed).unwrap();
        assert!(!still_declared(&changed, path, &source));
    }
    // The declaration's other name is the same declaration.
    let renamed: HostConfig = serde_yaml::from_str(
        "root: public\nroutes:\n  /app:\n    pusher: { target: fleet }\n",
    )
    .unwrap();
    assert!(is_fleet_route(&renamed, path));
    assert!(still_declared(&renamed, path, &source));
}
