use super::*;
use claviron_runtime::{RunMode, WorkerId};

#[test]
fn scope_selects_roles_and_standalone_combines_them() {
    for scope in [Scope::Master, Scope::Worker, Scope::Any] {
        assert!(scope.includes(RunMode::Standalone));
        assert_eq!(scope.includes(RunMode::Master), scope != Scope::Worker);
        assert_eq!(
            scope.includes(RunMode::Worker(WorkerId::new(1))),
            scope != Scope::Master,
        );
    }
    assert_eq!(Scope::Fleet, Scope::Any);
    assert_eq!(Scope::Workers, Scope::Worker);
}
