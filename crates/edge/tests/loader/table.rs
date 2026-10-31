use super::*;

#[test]
fn incompatible_declared_alias_types_are_rejected() {
    for source in [
        "app.test: {aliases: ['127.0.0.1']}",
        "'127.0.0.1': {aliases: [app.test]}",
    ] {
        let error = serde_yaml::from_str::<VhostTable>(source).err().unwrap();
        assert!(error.to_string().contains("incompatible alias"), "{error}");
    }
}

#[test]
#[cfg(feature = "server")]
fn aliases_share_compiled_acl_and_old_tables_keep_their_rules() {
    let table: VhostTable = serde_yaml::from_str(
        "app.test: {root: public, aliases: [alias.test], acl: {'/': {policy: deny}}}"
    )
    .unwrap();
    let canonical = "app.test".parse().unwrap();
    let alias = "alias.test".parse().unwrap();
    let rules = table.record(&canonical).unwrap().policy.as_ref().unwrap();
    assert!(Arc::ptr_eq(
        rules,
        table.record(&alias).unwrap().policy.as_ref().unwrap()
    ));
    let old = table.clone();
    let mut candidate = table;
    candidate.remove_host(&canonical);
    candidate.insert_canonical(
        canonical.clone(),
        Arc::new(serde_yaml::from_str("root: public").unwrap()),
    );
    let uri = "/".parse().unwrap();
    let ip = "203.0.113.1".parse().unwrap();
    assert!(candidate.record(&canonical).unwrap().policy.is_none());
    assert!(
        old.record(&canonical)
            .unwrap()
            .policy
            .as_ref()
            .unwrap()
            .check(&uri, ip)
            .is_err()
    );
}

#[test]
#[cfg(feature = "server")]
fn host_selectors_are_validated_against_named_listener_definitions() {
    let http = serde_yaml::from_str::<claviron_http::config::HttpYaml>(
        "listeners: {public: {transport: tcp, protocols: h1, listen: '127.0.0.1:80', requests: {max_body_size: off, body_read_timeout: off}}}"
    )
    .unwrap()
    .resolve()
    .unwrap();
    for selector in ["public", "[public]", "[]"] {
        let table: VhostTable = serde_yaml::from_str(&format!(
            "app.test: {{root: public, listeners: {selector}}}"
        ))
        .unwrap();
        assert!(table.validate_listeners(Some(&http)).is_ok());
    }
    for selector in ["unknown", "[public, public]"] {
        let table: VhostTable = serde_yaml::from_str(&format!(
            "app.test: {{root: public, listeners: {selector}}}"
        ))
        .unwrap();
        assert!(table.validate_listeners(Some(&http)).is_err());
    }
    let omitted: VhostTable =
        serde_yaml::from_str("app.test: {root: public}").unwrap();
    assert!(omitted.validate_listeners(None).is_ok());
}

/// Minimal smoke test for the YAML → `VhostTable` round-trip. No DB
/// involvement — just verifies that multiple hosts (and a declared
/// alias) parse into the expected canonical / alias counts.
#[test]
fn deserialize_multiple_hosts() {
    let yaml = r#"
default:
  root: public

app.test:
  root: public
  alias:
    - alias.test

stream.test:
  root: /sites/default/public
"#;

    let table: VhostTable =
        serde_yaml::from_str(yaml).expect("parse vhost table");

    assert_eq!(table.host_count(), 3, "expected 3 canonical hosts");
    assert_eq!(table.alias_count(), 1, "expected 1 alias row (alias.test)");

    let canonical = Host::from_str("app.test").expect("parse canonical");
    assert!(
        table.canonical_config(&canonical).is_some(),
        "app.test must be canonical"
    );

    let alias = Host::from_str("alias.test").expect("parse alias");
    let resolved = table.resolve(&alias).expect("alias must resolve");
    assert_eq!(resolved.0, canonical, "alias resolves to app.test");
}
