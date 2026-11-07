use claviron_http::ext::PathPrefix;

use super::*;

/// A scope that redirects serves nothing: no owner and no file fallback are
/// prepared behind it, whatever the site declares.
#[test]
fn a_redirecting_scope_prepares_no_owner_and_no_fallback() {
    let state = AppState::standalone("redirect-plan", ".");
    let site: Site = serde_yaml::from_str(
        "redirect: {to: /new, code: 307}\nindex: [index.html]\n",
    )
    .unwrap();
    let root = PathPrefix::root();
    let plan = BackendPlan::prepare(
        &RouteContext {
            state: &state,
            host: None,
            config: &site,
            route: &site,
            prefix: &root,
        },
        &Default::default(),
    );
    assert!(plan.steps.is_empty());
    assert!(plan.fallback.is_none());
}

#[cfg(feature = "orbit")]
#[test]
fn an_orbit_route_without_the_application_link_fails_to_compile() {
    let state = AppState::standalone("orbit-unreachable", ".");
    let site: Site =
        serde_yaml::from_str("orbit: {fleet_name: first, application: root}\n")
            .unwrap();
    let error = compile_site(
        &state,
        Arc::new(site),
        SiteTerms {
            listener: None,
            policy: Some(HttpPolicy::unlimited()),
            transport: &TransportPolicy::default(),
        },
        None,
    )
    .err()
    .expect("nothing can reach this route");
    assert!(error.to_string().contains("application link"), "{error}");
}
