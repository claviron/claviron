use super::*;
use crate::loader::snapshot::VhostSnapshot;

fn table() -> Arc<VhostTable> {
    Arc::new(serde_yaml::from_str("public.test: {root: public}\nprivate.test: {root: private, tls: {client_auth: {mode: required}}}").unwrap())
}

fn bind(
    policy: &Arc<VhostTable>,
    names: &Arc<VhostTable>,
) -> VhostBindings {
    VhostBindings::new(policy.clone(), names.clone())
}

#[test]
fn live_alias_add_move_delete_do_not_rewrite_an_accepted_binding() {
    let policy = table();
    let before = bind(&policy, &policy);
    let mut next = VhostSnapshot::from_table((*policy).clone());
    next.upsert_alias(
        "alias.test".parse().unwrap(),
        "public.test".parse().unwrap(),
    )
    .unwrap();
    let added = Arc::new(next.into_table());
    let accepted = bind(&policy, &added);
    assert!(before.resolve_sni(Some("alias.test")).is_err());
    assert_eq!(
        accepted
            .resolve_sni(Some("alias.test"))
            .unwrap()
            .vhost_key
            .0
            .to_string(),
        "public.test"
    );

    let mut next = VhostSnapshot::from_table((*added).clone());
    next.upsert_alias(
        "alias.test".parse().unwrap(),
        "private.test".parse().unwrap(),
    )
    .unwrap();
    let moved = Arc::new(next.into_table());
    let private =
        bind(&policy, &moved).resolve_sni(Some("alias.test")).unwrap();
    assert_eq!(private.vhost_key.0.to_string(), "private.test");
    assert!(private.vhost_config.tls.client_auth.is_required());
    assert!(
        !accepted
            .resolve_sni(Some("alias.test"))
            .unwrap()
            .vhost_config
            .tls
            .client_auth
            .is_required()
    );

    let mut next = VhostSnapshot::from_table((*moved).clone());
    assert!(next.delete_alias(&"alias.test".parse().unwrap()));
    let removed = Arc::new(next.into_table());
    assert!(bind(&policy, &removed).resolve_sni(Some("alias.test")).is_err());
    assert!(accepted.resolve_sni(Some("alias.test")).is_ok());
}

#[test]
fn canonical_auth_change_fails_closed_until_transport_policy_rebuild() {
    let policy = table();
    let accepted = bind(&policy, &policy);
    let mut next = VhostSnapshot::from_table((*policy).clone());
    next.upsert_host(
        "public.test".parse().unwrap(),
        Arc::new(
            serde_yaml::from_str(
                "root: public\naliases: [new.test]\ntls: {client_auth: {mode: required}}"
            )
            .unwrap()
        )
    );
    let updated = Arc::new(next.into_table());
    let stale = bind(&policy, &updated);
    assert!(stale.resolve_sni(Some("public.test")).is_err());
    assert!(stale.resolve_sni(Some("new.test")).is_err());
    assert!(
        stale.resolve_sni(Some("private.test")).is_ok(),
        "unmodified canonical remains available"
    );
    assert!(
        !accepted
            .resolve_sni(Some("public.test"))
            .unwrap()
            .vhost_config
            .tls
            .client_auth
            .is_required()
    );
    assert!(
        bind(&updated, &updated)
            .resolve_sni(Some("new.test"))
            .unwrap()
            .vhost_config
            .tls
            .client_auth
            .is_required()
    );
}

#[test]
fn delete_recreate_of_same_name_cannot_reuse_old_authentication() {
    let policy = table();
    let mut next = VhostSnapshot::from_table((*policy).clone());
    assert!(next.delete_host(&"public.test".parse().unwrap()));
    let removed = Arc::new(next.into_table());
    assert!(bind(&policy, &removed).resolve_sni(Some("public.test")).is_err());
    let mut next = VhostSnapshot::from_table((*removed).clone());
    next.upsert_host(
        "public.test".parse().unwrap(),
        Arc::new(serde_yaml::from_str("root: public").unwrap()),
    );
    let recreated = Arc::new(next.into_table());
    assert!(
        bind(&policy, &recreated).resolve_sni(Some("public.test")).is_err()
    );
    assert!(
        bind(&recreated, &recreated).resolve_sni(Some("public.test")).is_ok()
    );
}

#[test]
fn fallback_requires_its_own_current_policy_epoch_and_invalid_sni_rejects() {
    let policy =
        Arc::new(serde_yaml::from_str("default: {root: public}").unwrap());
    let current = bind(&policy, &policy);
    assert_eq!(
        current
            .resolve_sni(Some("unknown.test"))
            .unwrap()
            .vhost_key
            .0
            .to_string(),
        "default"
    );
    assert!(current.resolve_sni(None).is_err());
    assert!(current.resolve_sni(Some("bad name")).is_err());
    let replaced =
        Arc::new(serde_yaml::from_str("default: {root: private}").unwrap());
    assert!(
        bind(&policy, &replaced).resolve_sni(Some("unknown.test")).is_err()
    );
}

/// The handshake picks a TLS config by `handshake_key`; routing comes from
/// `resolve_sni`. They must accept and refuse the same names on the same
/// view, or a handshake could select one host's config while the
/// connection routes to another.
fn agrees(
    bindings: &VhostBindings,
    sni: Option<&str>,
) {
    let routed =
        bindings.resolve_sni(sni).ok().map(|ctx| ctx.vhost_key.0.to_string());
    let selected = bindings.handshake_key(sni).map(ToString::to_string);
    assert_eq!(selected, routed, "handshake and routing disagree for {sni:?}");
}

#[test]
fn handshake_key_selects_exactly_what_routing_binds() {
    let policy = table();
    let names = [
        "public.test",
        "private.test",
        "alias.test",
        "missing.test",
        "not a host",
    ];

    let current = bind(&policy, &policy);
    for name in names {
        agrees(&current, Some(name));
    }
    agrees(&current, None);

    let mut next = VhostSnapshot::from_table((*policy).clone());
    next.upsert_alias(
        "alias.test".parse().unwrap(),
        "private.test".parse().unwrap(),
    )
    .unwrap();
    let moved = bind(&policy, &Arc::new(next.into_table()));
    for name in names {
        agrees(&moved, Some(name));
    }
    assert_eq!(
        moved.handshake_key(Some("alias.test")).map(ToString::to_string),
        Some("private.test".to_owned())
    );
}

#[test]
fn handshake_key_fails_closed_on_a_canonical_change_like_routing() {
    let policy = table();
    let mut next = VhostSnapshot::from_table((*policy).clone());
    next.upsert_host(
        "public.test".parse().unwrap(),
        Arc::new(
            serde_yaml::from_str(
                "root: public\ntls: {client_auth: {mode: required}}",
            )
            .unwrap(),
        ),
    );
    let updated = bind(&policy, &Arc::new(next.into_table()));

    assert!(updated.handshake_key(Some("public.test")).is_none());
    agrees(&updated, Some("public.test"));
    agrees(&updated, Some("private.test"));
}
