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
        &NoHooks,
    );
    assert!(plan.steps.is_empty());
    assert!(plan.fallback.is_none());
}
