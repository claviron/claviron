use super::TaskArgs;

#[test]
fn audit_task_arguments_reject_typos_wrong_types_and_duplicates() {
    assert!(
        syn::parse_str::<TaskArgs>(r#"name = "example", scpoe = "master""#)
            .err()
            .unwrap()
            .to_string()
            .contains("unknown task argument")
    );
    for name in ["name", "scope", "dispatch", "feature", "crate_path", "config"]
    {
        for value in ["7", "false", "'x'"] {
            let prefix =
                if name == "name" { "" } else { "name = \"example\", " };
            assert!(
                syn::parse_str::<TaskArgs>(&format!(
                    "{prefix}{name} = {value}"
                ))
                .is_err(),
                "{name} must reject {value}"
            );
        }
        let definition = format!("{name} = \"example\", {name} = \"again\"");
        let error = syn::parse_str::<TaskArgs>(&definition).err().unwrap();
        assert!(error.to_string().contains("duplicate task argument"));
    }
}

#[test]
fn audit_task_arguments_preserve_defaults_and_explicit_strings() {
    let defaults = syn::parse_str::<TaskArgs>(r#"name = "example""#).unwrap();
    assert!(defaults.scope.is_none());
    assert!(defaults.dispatch.is_none());
    let args = syn::parse_str::<TaskArgs>(r#"name = "example", scope = "master", dispatch = "one", feature = "optional", crate_path = "claviron", config = "scope: master", "#).unwrap();
    assert_eq!(args.scope.as_deref(), Some("master"));
    assert_eq!(args.dispatch.as_deref(), Some("one"));
    assert_eq!(args.feature.as_deref(), Some("optional"));
    assert_eq!(args.crate_path.as_deref(), Some("claviron"));
    assert_eq!(args.config.as_deref(), Some("scope: master"));
}
