use claviron_http::ext::PathPrefix;

use super::mounted_websocket_path;

fn prefix(value: &str) -> PathPrefix {
    serde_yaml::from_str(value).unwrap()
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
