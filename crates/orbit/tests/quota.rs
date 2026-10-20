use super::*;

fn config() -> QuotaConfig {
    QuotaConfig {
        units: NonZeroU64::new(1).unwrap(),
        period: Duration::from_secs(3600),
        burst: NonZeroU64::new(1).unwrap(),
        table: QuotaTable {
            slots: NonZeroUsize::new(8).unwrap(),
            shards: NonZeroUsize::new(1).unwrap(),
            key_bytes: NonZeroUsize::new(64).unwrap(),
            probes: NonZeroUsize::new(8).unwrap(),
        },
        cas_attempts: NonZeroUsize::new(64).unwrap(),
        store: QuotaBacking::LocalGuard {},
    }
}

#[test]
fn local_constructors_are_independent_and_clones_share_debt() {
    let first = Quota::new(config()).unwrap();
    let clone = first.clone();
    let other = Quota::new(config()).unwrap();
    let key = Key::new(b"test", b"subject").unwrap();
    let cost = NonZeroU64::new(1).unwrap();
    assert_eq!(first.try_consume(key, cost).unwrap(), Decision::Allowed);
    assert!(matches!(
        clone.try_consume(key, cost).unwrap(),
        Decision::Limited { .. }
    ));
    assert_eq!(other.try_consume(key, cost).unwrap(), Decision::Allowed);
}

#[test]
fn local_wrapper_forwards_the_committed_snapshot() {
    let store = Quota::new(config()).unwrap();
    let key = Key::new(b"test", b"subject").unwrap();
    let cost = NonZeroU64::new(1).unwrap();
    let admission = store.try_consume_with_snapshot(key, cost).unwrap();
    assert_eq!(admission.decision, Decision::Allowed);
    assert_eq!(admission.snapshot, Some(Snapshot { limit: 1, remaining: 0 }));
    let limited = store.try_consume_with_snapshot(key, cost).unwrap();
    assert!(matches!(limited.decision, Decision::Limited { .. }));
    assert_eq!(limited.snapshot, None);
}

#[test]
fn backing_selection_is_explicit_and_strictly_parsed() {
    let guard: QuotaBacking = serde_yaml::from_str("mode: fleet_guard\nresource: {fleet: example, kind: 0}\nowners: 4\nrecovery_attempts: 0\n").unwrap();
    let strict: QuotaBacking = serde_yaml::from_str("mode: fleet_quota\nresource: {fleet: example, kind: 0}\nowners: 4\nrecovery_attempts: 0\n").unwrap();
    assert!(matches!(guard, QuotaBacking::FleetGuard { .. }));
    assert!(matches!(strict, QuotaBacking::FleetQuota { .. }));
    assert!(
        serde_yaml::from_str::<QuotaBacking>("mode: fleet_guard\n").is_err()
    );
    assert!(
        serde_yaml::from_str::<QuotaBacking>("mode: local_guard\nowners: 4\n")
            .is_err()
    );
    assert!(serde_yaml::from_str::<QuotaBacking>("mode: fleet_quota\nresource: {fleet: invalid.namespace, kind: 0}\nowners: 4\nrecovery_attempts: 0\n").is_err());
}

#[test]
fn invalid_geometry_or_period_is_rejected_before_opening() {
    let mut definition = config();
    definition.period = Duration::ZERO;
    assert!(matches!(definition.validate(), Err(Error::InvalidPeriod)));
    definition.period = Duration::from_secs(1);
    definition.table.shards = NonZeroUsize::new(3).unwrap();
    assert!(matches!(definition.validate(), Err(Error::InvalidGeometry)));
}

#[cfg(unix)]
struct ShmFixture {
    _reservation: tempfile::TempDir,
    resource: QuotaResource,
}

#[cfg(unix)]
impl ShmFixture {
    fn new() -> Self {
        // Atomically reserve an eight-character test-only namespace. Keeping
        // the directory alive prevents another fixture from choosing it.
        let directory = tempfile::Builder::new()
            .prefix("q")
            .rand_bytes(7)
            .tempdir()
            .unwrap();
        let fleet = directory
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        Self {
            _reservation: directory,
            resource: QuotaResource { fleet, kind: 0 },
        }
    }

    fn config(
        &self,
        guard: bool,
        slots: usize,
    ) -> QuotaConfig {
        let mut config = config();
        config.table.slots = NonZeroUsize::new(slots).unwrap();
        config.table.probes = NonZeroUsize::new(slots).unwrap();
        let owners = NonZeroUsize::new(4).unwrap();
        config.store = if guard {
            QuotaBacking::FleetGuard {
                resource: self.resource,
                owners,
                recovery_attempts: 0,
            }
        } else {
            QuotaBacking::FleetQuota {
                resource: self.resource,
                owners,
                recovery_attempts: 0,
            }
        };
        config
    }

    fn cleanup(&self) -> std::io::Result<()> {
        use orbitive::shm;
        let uid = unsafe { libc::geteuid() };
        let _hold =
            shm::try_lock_fleet_exclusive(self.resource.fleet.as_str(), uid)?
                .ok_or_else(|| {
                std::io::Error::other("quota test still has live SHM owners")
            })?;
        let name = shm::ring_segment_name(
            self.resource.fleet.as_str(),
            self.resource.kind,
        );
        // No contents are read: map only to unlink this fixture's exact object.
        match shm::ShmRegion::open_existing_read_write(
            &name,
            1,
            uid,
            Default::default(),
        ) {
            Ok(region) => region.unlink()?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        std::fs::remove_file(shm::fleet_lock_path_for_uid(
            self.resource.fleet.as_str(),
            uid,
        ))?;
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for ShmFixture {
    fn drop(&mut self) {
        let result = self.cleanup();
        if !std::thread::panicking() {
            result.expect("isolated quota fixture cleanup");
        }
        // The directory remains reserved until after SHM/lock cleanup.
    }
}

#[cfg(unix)]
#[test]
fn both_fleet_store_choices_share_debt_across_opens_and_preserve_it_on_reopen()
{
    for guard in [true, false] {
        let fixture = ShmFixture::new();
        let config = fixture.config(guard, 8);
        let first = Quota::new(config.clone()).unwrap();
        let second = Quota::new(config.clone()).unwrap();
        let key = Key::new(b"test", b"subject").unwrap();
        let cost = NonZeroU64::new(1).unwrap();
        let accepted = first.try_consume_with_snapshot(key, cost).unwrap();
        assert_eq!(accepted.decision, Decision::Allowed);
        assert_eq!(
            accepted.snapshot,
            Some(Snapshot { limit: 1, remaining: 0 })
        );
        assert!(matches!(
            second.try_consume(key, cost).unwrap(),
            Decision::Limited { .. }
        ));
        drop(second);
        drop(first);
        let reopened = Quota::new(config).unwrap();
        assert!(matches!(
            reopened.try_consume(key, cost).unwrap(),
            Decision::Limited { .. }
        ));
        assert!(
            Quota::new(fixture.config(!guard, 8)).is_err(),
            "opening cannot change retention or reset debt"
        );
        assert!(matches!(
            reopened.try_consume(key, cost).unwrap(),
            Decision::Limited { .. }
        ));
    }
}

#[cfg(unix)]
#[test]
fn fleet_guard_reclaims_live_debt_while_fleet_quota_refuses_a_new_identity() {
    for guard in [true, false] {
        let fixture = ShmFixture::new();
        let store = Quota::new(fixture.config(guard, 1)).unwrap();
        let cost = NonZeroU64::new(1).unwrap();
        let first = Key::new(b"test", b"first").unwrap();
        let second = Key::new(b"test", b"second").unwrap();
        assert_eq!(store.try_consume(first, cost).unwrap(), Decision::Allowed);
        let result = store.try_consume(second, cost);
        if guard {
            assert_eq!(result.unwrap(), Decision::Allowed);
        } else {
            assert!(matches!(result, Err(Error::CapacityFull)));
            assert!(matches!(
                store.try_consume(first, cost).unwrap(),
                Decision::Limited { .. }
            ));
        }
    }
}
