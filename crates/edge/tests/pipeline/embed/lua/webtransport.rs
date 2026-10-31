use super::*;

#[test]
fn lua_webtransport_rejects_removed_or_reassigned_socket_bindings() {
    let host: HostConfig = serde_yaml::from_str(
        "root: public\nlua: { path: /lua, application: demo, websocket: { path: /socket } }",
    ).unwrap();
    assert!(configured_route(&host, "/lua/socket", "demo").is_some());
    assert!(configured_route(&host, "/lua/socket/extra", "demo").is_none());
    assert!(configured_route(&host, "/lua/socket", "other").is_none());
    let removed: HostConfig = serde_yaml::from_str("root: public").unwrap();
    assert!(configured_route(&removed, "/lua/socket", "demo").is_none());
}

#[test]
fn lua_webtransport_respects_more_specific_route_ownership() {
    let host: HostConfig = serde_yaml::from_str(
        "root: public\nlua: { path: /lua, application: demo, websocket: { path: /socket } }\nroutes:\n  /lua/socket:\n    lua: { application: other, websocket: { path: / } }",
    ).unwrap();
    assert!(configured_route(&host, "/lua/socket", "demo").is_none());
    assert!(configured_route(&host, "/lua/socket", "other").is_some());
}

#[test]
fn lua_webtransport_uses_the_effective_routes_origin_policy() {
    use claviron_http::config::routes::lua::LuaSocketOriginPolicy;

    let host: HostConfig = serde_yaml::from_str(
        "root: public\nlua: { path: /lua, application: demo, origin: any, websocket: { path: /socket } }\nroutes:\n  /lua/socket:\n    lua: { application: demo, origin: same-origin, websocket: { path: / } }",
    ).unwrap();
    let route = configured_route(&host, "/lua/socket", "demo").unwrap();
    assert_eq!(route.origin, LuaSocketOriginPolicy::SameOrigin);
}
