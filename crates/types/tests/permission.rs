use super::*;

struct Read;
impl PermissionDefinition for Read {
    const NAME: &'static str = "service.v2.read";
}
struct SameRead;
impl PermissionDefinition for SameRead {
    const NAME: &'static str = "service.v2.read";
}
struct Write;
impl PermissionDefinition for Write {
    const NAME: &'static str = "service.write";
}

#[test]
fn definitions_convert_to_the_same_name_value_as_parsed_permissions() {
    use std::collections::{BTreeSet, HashSet};

    let typed: Permission = Read.into();
    let other_definition = Permission::from(SameRead);
    let parsed = Read::NAME.parse::<Permission>().unwrap();

    assert_eq!(typed, parsed);
    assert_eq!(other_definition, parsed);
    assert_eq!(typed.cmp(&parsed), std::cmp::Ordering::Equal);
    assert_ne!(typed, Permission::from(Write));
    assert_ne!(typed, Permission::new("SERVICE.v2.read").unwrap());

    let ordered = BTreeSet::from([
        typed.clone(),
        other_definition.clone(),
        parsed.clone(),
    ]);
    let hashed = HashSet::from([typed, other_definition, parsed]);
    assert_eq!(ordered.len(), 1);
    assert_eq!(hashed.len(), 1);

    let mut catalog = PermissionCatalog::new();
    catalog.register_name(Read::NAME).unwrap();
    let permissions = catalog.validate([Read::NAME]).unwrap();
    assert!(permissions.contains_name(&Read.into()));
}

#[test]
fn names_are_exact_validated_values() {
    for valid in ["service.v2.read", "9", "READ", "a_b-c.d"] {
        assert_eq!(Permission::new(valid).unwrap().as_str(), valid);
    }
    for invalid in
        ["", ".read", "-read", "_read", "a b", "a/b", "a:*", "é", "a\n", "a\0"]
    {
        assert_eq!(Permission::new(invalid), Err(PermissionError::InvalidName));
    }
    assert_ne!(
        Permission::new("Read").unwrap(),
        Permission::new("read").unwrap()
    );
    assert_eq!(
        Permission::defined::<Read>().unwrap(),
        Permission::defined::<SameRead>().unwrap()
    );
}

#[test]
fn catalog_validates_the_whole_selection_without_mutating_definitions() {
    let mut catalog = PermissionCatalog::new();
    catalog
        .register::<Read>()
        .unwrap()
        .register::<SameRead>()
        .unwrap()
        .register::<Write>()
        .unwrap();
    let selected = catalog.validate([Read::NAME, Read::NAME]).unwrap();
    assert_eq!(catalog.definitions().len(), 2);
    assert_eq!(selected.iter().len(), 1);
    assert!(selected.contains::<Read>());
    assert!(selected.contains::<SameRead>());
    assert!(!selected.contains::<Write>());
    assert!(catalog.validate(["service"]).is_err());
    assert!(catalog.validate([Read::NAME, "unknown"]).is_err());
    assert!(catalog.validate(["service.*"]).is_err());
    assert!(PermissionCatalog::new().validate([Read::NAME]).is_err());
    assert!(catalog.validate(Vec::<String>::new()).unwrap().is_empty());
}
