use claviron_http::ext::PathPrefix;

use super::{configured_route, mounted_websocket_path};

fn prefix(value: &str) -> PathPrefix {
    serde_yaml::from_str(value).unwrap()
}

#[test]
fn native_socket_selection_rejects_removed_and_masked_lua_endpoints() {
    let host: crate::HostConfig = serde_yaml::from_str(
        "lua: {application: demo, websocket: {path: /socket}}\nroutes:\n  /socket: {static: {}}\n",
    ).unwrap();
    assert!(configured_route(&host, "/socket", "demo").is_none());
    let removed = crate::HostConfig::new("public");
    assert!(configured_route(&removed, "/socket", "demo").is_none());
}

#[test]
fn websocket_path_is_relative_to_the_application_mount() {
    let mount = prefix("/api");
    let socket = prefix("/socket");
    assert_eq!(
        mounted_websocket_path(Some(&mount), &socket).as_ref(),
        "/api/socket"
    );
    assert_eq!(mounted_websocket_path(None, &socket).as_ref(), "/socket");

    let root = prefix("/");
    assert_eq!(mounted_websocket_path(Some(&mount), &root).as_ref(), "/api");
}
