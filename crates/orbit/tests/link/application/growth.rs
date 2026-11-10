use super::*;
use crate::link::application::{Access, Watched};

/// An owner that never creates or joins the fleet: it waits for the Link its
/// first receiver creates, watches without a lane, and grows once per
/// episode until it says the worker arrived.
#[tokio::test(flavor = "current_thread")]
async fn an_owner_waits_for_the_link_and_grows_once_per_episode() {
    let name = format!("fg{:x}", std::process::id());
    let waiting = {
        let name = name.clone();
        tokio::spawn(
            async move { ApplicationSegment::adopt_when_created(&name).await },
        )
    };
    tokio::task::yield_now().await;

    let spec = orbitive::link::LinkSpec::new(
        8,
        crate::link::application::LINK_SPEC.inbox_kind,
        orbitive::link::InboxGeometry::new(8, 1024),
        orbitive::stream::StreamSpec::new(
            crate::link::application::LINK_SPEC.streams.kind,
            4,
            1024,
        ),
    );
    let fleet = ApplicationSegment::for_fleet_with_spec(
        &name,
        spec,
        &Access::OwnerOnly,
    )
    .unwrap();
    struct Cleanup(ApplicationSegment);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.0.unlink();
        }
    }
    let fleet = Cleanup(fleet);
    fleet.0.inbox().claim_lane(1, "example", "rust", 1).unwrap();
    // A receiver opens its body streams; with them the fleet is whole.
    let bodies = crate::link::application::ApplicationBodies::for_segment(
        &fleet.0, 1, 1,
    )
    .unwrap();

    let owner = waiting.await.unwrap().unwrap();
    assert_eq!(owner.spec(), spec, "the shape the receiver declared");
    let mut watcher = owner.watch("task").unwrap();
    let policy = GrowthPolicy { min: 1, max: 4, step: 1 };
    let mut growth = FleetGrowth::publish(&owner, Some(policy));

    // A sender found no receiver to run its exchange now.
    assert!(fleet.0.inbox().report_pressure());
    assert_eq!(watcher.next_change().await.unwrap(), Watched::Pressure);
    assert_eq!(growth.pressure(&mut watcher), Some(policy));
    assert!(!fleet.0.inbox().report_pressure(), "one wake per episode");

    growth.arrived(&mut watcher);
    assert!(fleet.0.inbox().report_pressure(), "a new episode can open");
    let _ = bodies.unlink();
}
