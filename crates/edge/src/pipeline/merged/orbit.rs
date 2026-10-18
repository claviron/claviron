//! Cold preparation of orbit dispatch; host configuration stays in composition.

use axum::Router;
use claviron_http::config::routes::OrbitRouteConfig;
use claviron_runtime::AppState;

pub(in crate::pipeline::merged) fn prepare(
    router: Router,
    state: &AppState,
    orbit: &[OrbitRouteConfig],
) -> Router {
    let mut applications = orbit.to_vec();
    applications.sort_by(|a, b| match (&a.path, &b.path) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => a.as_str().len().cmp(&b.as_str().len()),
    });
    let mut router = router;
    for application in applications {
        router = crate::pipeline::build_orbit_application_router(
            router,
            state,
            application.path,
            application.fleet_name,
            application.application,
            application.requests,
        );
    }
    router
}
