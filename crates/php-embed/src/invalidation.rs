#[cfg(any(feature = "apcu-intercept", feature = "opcache-intercept"))]
pub mod bus;
pub mod ipc;

/// The fleet component of a segment name: the Orbit fleet this process
/// joined, or the instance name for a process that never joined one, which
/// is what keeps standalone tests on segments of their own.
pub(crate) fn segment_fleet(state: &claviron_runtime::AppState) -> String {
    use claviron_orbit::OrbitStateExt;
    state.fleet().map_or_else(
        || state.name().to_owned(),
        |fleet| fleet.name().to_owned(),
    )
}
