use super::*;
use crate::link::application::{Access, Watched};

/// An owner that never creates or joins the fleet: it adopts the existing
/// one, watches without a lane, and grows once per episode.
#[tokio::test(flavor = "current_thread")]
async fn an_adopting_owner_grows_once_per_episode_until_the_serving_receivers_change()
 {
    let name = format!("fg{:x}", std::process::id());
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

    let owner = ApplicationSegment::adopt(&name, &Access::OwnerOnly)
        .unwrap()
        .expect("the fleet exists");
    let mut watcher = owner.watch("task").unwrap();
    let policy = GrowthPolicy { min: 1, max: 4, step: 1 };
    let mut growth = FleetGrowth::publish(&owner, "example", Some(policy));

    // A sender found no receiver to run its exchange now.
    assert!(fleet.0.inbox().report_pressure());
    assert_eq!(watcher.next_change().await.unwrap(), Watched::Pressure);
    let grow = growth.pressure(&owner, &mut watcher).expect("an episode");
    assert_eq!(grow, Grow { policy, serving: 1 });
    assert!(!fleet.0.inbox().report_pressure(), "one wake per episode");

    // A caller joining does not end it; the new worker's lane does.
    fleet.0.inbox().claim_lane(2, "caller", "edge", 2).unwrap();
    assert!(matches!(
        watcher.next_change().await.unwrap(),
        Watched::Directory(_)
    ));
    growth.directory_changed(&owner, &mut watcher);
    assert!(!fleet.0.inbox().report_pressure(), "still the same episode");
    fleet.0.inbox().claim_lane(3, "example", "rust", 3).unwrap();
    assert!(matches!(
        watcher.next_change().await.unwrap(),
        Watched::Directory(_)
    ));
    growth.directory_changed(&owner, &mut watcher);
    assert!(fleet.0.inbox().report_pressure(), "a new episode can open");
    let _ = bodies.unlink();
}
