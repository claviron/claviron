use std::any::TypeId;

use claviron_runtime::AppState;
use claviron_runtime::registry::Resolve;

use super::*;

/// This dependency is the whole fix for a boot failure that compiled
/// perfectly: `merge_db_certs` reads `HostingService.server_id`,
/// `boot_priority` cannot express that (it only breaks ties between
/// components already free of edges), and without it an EARLY resolver beat a
/// NORMAL `HostingService` and boot died with "HostingService is not
/// initialized". Dropping it would compile and would reintroduce that.
#[test]
fn cert_resolver_depends_on_hosting() {
    let mut dependencies = Vec::new();
    <<CertificateProvider as Component<AppState>>::Deps as Resolve<AppState>>::dependencies(&mut dependencies);
    assert!(
        dependencies
            .contains(&TypeId::of::<claviron_hosting::HostingService>()),
        "CertificateProvider must depend on HostingService"
    );
    dependencies.clear();
    <<CertResolverProvider as Component<AppState>>::Deps as Resolve<
        AppState,
    >>::dependencies(&mut dependencies);
    assert!(dependencies.contains(&TypeId::of::<CertificateProvider>()));
}
