use super::PusherAdapter;

#[test]
fn accepts_path_limits_http_proxy_to_pusher_namespaces() {
    for (path, accepted) in [
        ("/app/example", true),
        ("/apps/example", true),
        ("/apps/example/events", true),
        ("/apps/example/channels/one", true),
        ("/app/", false),
        ("/app/example/extra", false),
        ("/apps/", false),
        ("/apps/example/", false),
        ("/application/example", false),
        ("/assets/app.js", false),
        ("app/example", false),
    ] {
        assert_eq!(PusherAdapter::accepts_path(path), accepted, "{path}");
    }
}
