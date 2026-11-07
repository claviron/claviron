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
        &NoHooks,
    );
    assert!(plan.steps.is_empty());
    assert!(plan.fallback.is_none());
}

#[cfg(feature = "orbit")]
struct Unanswered;

#[cfg(feature = "orbit")]
impl DispatchStep for Unanswered {
    fn call(
        &self,
        _request: http::Request<claviron_http::RouteBody>,
        _context: Option<crate::HostContext<'_>>,
    ) -> crate::DispatchCall {
        unreachable!("this site never compiles")
    }
}

#[cfg(feature = "orbit")]
impl BackendHandler for Unanswered {
    fn contract(&self) -> crate::BackendContract {
        crate::BackendContract {
            identity: crate::BackendIdentity::None,
            reply: crate::BackendReply::Owner,
        }
    }
}

#[cfg(feature = "orbit")]
#[test]
fn an_orbit_route_without_the_application_link_fails_to_compile() {
    let state = AppState::standalone("orbit-unreachable", ".");
    let site: Site =
        serde_yaml::from_str("orbit: {fleet_name: first, application: root}\n")
            .unwrap();
    let error = compile_with_owner(
        &state,
        Arc::new(site),
        HttpPolicy::unlimited(),
        Arc::new(Unanswered),
    )
    .err()
    .expect("nothing can reach this route");
    assert!(error.to_string().contains("application link"), "{error}");
}
