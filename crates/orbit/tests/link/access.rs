use super::*;
use crate::link::{InboxGeometry, StreamSpec};

#[test]
fn owner_only_is_the_default_and_missing_group_inspection_creates_nothing() {
    assert_eq!(Access::default(), Access::OwnerOnly);
    let directory = tempfile::tempdir().unwrap();
    let access = Access::Group(
        GroupAccess::new(
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
    assert!(!access.exists("example", spec).unwrap());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
