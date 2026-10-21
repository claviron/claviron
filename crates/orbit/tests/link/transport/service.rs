use super::*;

#[test]
fn destination_access_is_explicit_immutable_and_local_to_that_link() {
    let directory = tempfile::tempdir().unwrap();
    let service = TransportService::default();
    let destination = FleetName::new("example");
    let other = FleetName::new("other");
    let access = Access::Group(
        crate::link::GroupAccess::new(
            unsafe { libc::geteuid() },
            unsafe { libc::getegid() },
            directory.path().join("ipc"),
        )
        .unwrap(),
    );
    service.register_access(destination, access.clone()).unwrap();
    service.register_access(destination, access.clone()).unwrap();
    assert_eq!(service.access_for(destination).unwrap(), access);
    assert_eq!(service.access_for(other).unwrap(), Access::OwnerOnly);
    assert!(service.register_access(destination, Access::OwnerOnly).is_err());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
