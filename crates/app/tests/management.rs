use super::*;
use crate::{ApplicationName, ApplicationRuntime};

fn assert_event<E: Event>() {}

#[test]
fn lifecycle_observations_are_typed_process_events() {
    assert_event::<ApplicationStarted>();
    assert_event::<ApplicationStopped>();
    assert_event::<ApplicationFailed>();

    let id = ApplicationId::new(
        ApplicationRuntime::new("js").unwrap(),
        ApplicationName::new("shop").unwrap(),
    );
    assert_eq!(
        ApplicationFailed { id, generation: 4, error: "boot failed".into() }
            .error,
        "boot failed"
    );
}
