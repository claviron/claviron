use std::any::TypeId;

use claviron_runtime::registry::AnyComponent;

use super::VhostResolver;
use crate::HttpConfigProvider;

/// Same failure as `claviron-tls`: alias hydration reads
/// `HostingService.server_id`, and an EARLY component that does not depend on
/// hosting boots ahead of a NORMAL one. Both dependencies are asserted because
/// losing either compiles.
#[test]
fn vhost_resolver_declares_both_of_its_boot_dependencies() {
    let resolver = VhostResolver::default();
    let dependencies =
        AnyComponent::<claviron_runtime::AppState>::dependencies(&resolver);
    assert!(
        dependencies.contains(&TypeId::of::<HttpConfigProvider>()),
        "VhostResolver must depend on HttpConfigProvider"
    );
    assert!(
        dependencies
            .contains(&TypeId::of::<claviron_hosting::HostingService>()),
        "VhostResolver must depend on HostingService"
    );
}
