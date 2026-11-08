use super::*;

struct Descriptor(std::fs::File);
impl SharedDescriptor for Descriptor {
    fn duplicate(&self) -> io::Result<OwnedFd> {
        self.0.as_fd().try_clone_to_owned()
    }
}

#[test]
fn child_descriptors_duplicate_the_owner_and_keep_close_on_exec() {
    let resources = ProcessResources::default();
    let owner = Arc::new(Descriptor(std::fs::File::open("/dev/null").unwrap()));
    resources.share("listener", owner.clone()).unwrap();
    assert!(resources.share("listener", owner.clone()).is_err());
    let child = resources.for_child().unwrap();
    assert!(resources.share("late", owner.clone()).is_err());
    let manifest = child.manifest();
    assert_eq!(manifest[0].0.as_ref(), "listener");
    assert_ne!(manifest[0].1, owner.0.as_raw_fd());
    let flags = unsafe { libc::fcntl(manifest[0].1, libc::F_GETFD) };
    assert_ne!(flags & libc::FD_CLOEXEC, 0);
    drop(child);
    assert!(owner.0.metadata().is_ok());
}

#[test]
fn an_invalid_inherited_batch_is_not_partially_installed() {
    let resources = ProcessResources::default();
    let fd = || std::fs::File::open("/dev/null").unwrap().into();
    resources.inherit(vec![("retained".into(), fd())]).unwrap();
    assert!(
        resources
            .inherit(vec![("new".into(), fd()), ("retained".into(), fd())])
            .is_err()
    );
    assert!(resources.inherited("new").is_err());
    assert!(resources.inherited("retained").is_ok());
}

#[test]
fn retiring_a_retained_descriptor_does_not_close_a_generation_copy() {
    let resources = ProcessResources::default();
    let fd: OwnedFd = std::fs::File::open("/dev/null").unwrap().into();
    resources.inherit(vec![("listener".into(), fd)]).unwrap();
    let generation = resources.inherited("listener").unwrap();
    resources.unshare("listener");
    assert!(resources.inherited("listener").is_err());
    assert!(std::fs::File::from(generation).metadata().is_ok());
}
