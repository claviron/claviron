use claviron_runtime::{RunMode, WorkerId};

use super::TaskScope;

#[test]
fn workers_scope_includes_standalone_but_not_master() {
    assert!(TaskScope::Workers.includes(RunMode::Standalone));
    assert!(TaskScope::Workers.includes(RunMode::Worker(WorkerId::new(1))));
    assert!(!TaskScope::Workers.includes(RunMode::Master));
}

#[test]
fn fleet_scope_includes_every_runtime_role() {
    assert!(TaskScope::Fleet.includes(RunMode::Standalone));
    assert!(TaskScope::Fleet.includes(RunMode::Master));
    assert!(TaskScope::Fleet.includes(RunMode::Worker(WorkerId::new(1))));
}
