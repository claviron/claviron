use super::*;

fn current_user() -> User {
    User::from_uid(geteuid()).unwrap().expect("effective user account")
}

#[test]
fn declared_effective_identity_requires_no_switch() {
    let user = current_user();
    let gid = getegid();
    let group = Group::from_gid(gid).unwrap().expect("effective group account");
    let mut spec: CommandSpec = serde_yaml::from_str(&format!(
        "program: example\nuser: {}\ngroup: {}",
        user.uid, group.name
    ))
    .unwrap();

    spec.prepare_identity("example", false).unwrap();

    let identity = spec.identity.unwrap();
    assert_eq!(identity.uid, user.uid);
    assert_eq!(identity.gid, gid);
    assert!(!identity.switches());
    assert!(identity.groups.is_none());
}

#[test]
fn different_identity_is_refused_without_root() {
    let user = current_user();
    let caller = Uid::from_raw(if user.uid.as_raw() == 1 { 2 } else { 1 });
    let mut spec: CommandSpec =
        serde_yaml::from_str("program: example").unwrap();
    spec.user = Some(IdentitySelector::Name(user.name.clone().into()));

    let error =
        CommandIdentity::resolve("daily", &spec, false, caller, user.gid)
            .unwrap_err();

    assert!(
        error.contains(&format!("task 'daily': command.user '{}'", user.name))
    );
    assert!(error.contains("requires root"));
}

#[test]
fn link_worker_refuses_different_identity_before_switching() {
    let user = current_user();
    let caller = Uid::from_raw(u32::from(user.uid.is_root()));
    let spec: CommandSpec =
        serde_yaml::from_str(&format!("program: example\nuser: {}", user.uid))
            .unwrap();

    let error =
        CommandIdentity::resolve("fleet-worker", &spec, true, caller, user.gid)
            .unwrap_err();

    assert!(error.contains(&format!(
        "task 'fleet-worker': command.user '{}'",
        user.uid
    )));
    assert!(error.contains("fleet is owner-only"));
    assert!(error.contains("requires fleet group access"));
}
