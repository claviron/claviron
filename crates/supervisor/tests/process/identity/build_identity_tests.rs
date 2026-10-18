use super::BuildIdentity;

#[test]
fn build_identity_parses_what_the_build_script_records() {
    let clean = BuildIdentity::parse("016ff434", "0.1.0");
    assert_eq!(clean.revision(), Some(0x016f_f434));
    assert!(!clean.dirty());
    assert_eq!(clean.to_string(), "016ff434");

    let dirty = BuildIdentity::parse("016ff434-dirty", "0.1.0");
    assert_eq!(dirty.revision(), Some(0x016f_f434));
    assert!(dirty.dirty());
    assert_eq!(dirty.to_string(), "016ff434-dirty");
    assert!(
        !clean.same_source(dirty),
        "a dirty build is not the committed one"
    );

    let unknown = BuildIdentity::parse("unknown", "0.1.0");
    assert_eq!(unknown.revision(), None);
    assert_eq!(unknown.to_string(), "unknown");
    assert!(unknown.same_source(BuildIdentity::from_wire(0, false)));

    // What this crate was actually built as is well-formed.
    let current = BuildIdentity::current();
    assert!(!current.crate_version().is_empty());
    assert!(current.to_string() == "unknown" || current.to_string().len() >= 8);
}
