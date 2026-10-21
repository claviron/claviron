use super::*;
use crate::link::{InboxGeometry, StreamSpec};

#[test]
fn open_without_executor_is_rejected_before_joining() {
    // No SHM is opened: the reader's executor requirement is checked first.
    let spec = LinkSpec::new(
        2,
        27,
        InboxGeometry::new(2, 256),
        StreamSpec::new(28, 2, 256),
    );
    let result =
        Connection::open("example", spec, "example-service", "source", 1);
    assert!(matches!(
        result,
        Err(Error::Malformed("a Link inbox reader requires a Tokio runtime"))
    ));
}

#[test]
fn group_open_without_executor_is_rejected_before_provisioning() {
    let directory = tempfile::tempdir().unwrap();
    let access = Access::Group(
        crate::link::GroupAccess::new(
            unsafe { libc::geteuid() },
            unsafe { libc::getegid() },
            directory.path().join("ipc"),
        )
        .unwrap(),
    );
    let spec = LinkSpec::new(
        2,
        27,
        InboxGeometry::new(2, 256),
        StreamSpec::new(28, 2, 256),
    );
    let result = Connection::open_with_access(
        "example",
        spec,
        "example-service",
        "source",
        1,
        &access,
    );
    assert!(matches!(
        result,
        Err(Error::Malformed("a Link inbox reader requires a Tokio runtime"))
    ));
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
