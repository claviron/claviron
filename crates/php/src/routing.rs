//! Cold preparation of PHP mounts from resolved runtime declarations.
//! Backend execution stays with embed/FCGI; the caller supplies their builders.

use axum::Router;
use claviron_runtime::AppState;

use crate::PhpRuntime;

/// Stack PHP mounts in stable prefix order, most-specific outermost.
/// The backend builder is called only during router construction.
pub fn build_router<F>(
    router: Router,
    state: &AppState,
    mut runtimes: Vec<PhpRuntime>,
    mut mount: F,
) -> Router
where
    F: FnMut(Router, &AppState, PhpRuntime) -> Router,
{
    runtimes.sort_by(|a, b| match (&a.path, &b.path) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => a.as_str().len().cmp(&b.as_str().len()),
    });
    runtimes
        .into_iter()
        .fold(router, |router, runtime| mount(router, state, runtime))
}
