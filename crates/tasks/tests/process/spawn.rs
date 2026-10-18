use super::{expand_path, expand_segment};

/// Cargo sets this for every test binary it runs, so the expectation is
/// knowable without mutating the environment — `set_var` is both unsafe
/// and shared with every other test in this binary.
const SET: &str = "CARGO_PKG_NAME";
const UNSET: &str = "CLAVIRON_TASKS_PATH_EXPANSION_ABSENT";

fn set_value() -> String {
    std::env::var(SET).expect("cargo sets CARGO_PKG_NAME for test binaries")
}

#[test]
fn expands_both_spellings_of_a_reference() {
    let value = set_value();
    assert_eq!(
        expand_segment(&format!("/opt/${SET}/bin")),
        Some(format!("/opt/{value}/bin"))
    );
    assert_eq!(
        expand_segment(&format!("/opt/${{{SET}}}/bin")),
        Some(format!("/opt/{value}/bin"))
    );
}

/// The reason segments are resolved one at a time: `$HOME/.local/bin`
/// with no `HOME` must not become `/.local/bin`, an absolute path to
/// somewhere nobody named.
#[test]
fn a_segment_naming_an_unset_variable_is_dropped_whole() {
    assert_eq!(expand_segment(&format!("${UNSET}/.local/bin")), None);
    assert_eq!(
        expand_path(&format!("${UNSET}/.local/bin:/usr/bin")),
        "/usr/bin"
    );
}

/// A stray `::` or a trailing colon must not put the current directory
/// on a supervised process's search path.
#[test]
fn empty_segments_never_survive() {
    assert_eq!(expand_path("/usr/bin::/bin:"), "/usr/bin:/bin");
    assert_eq!(expand_path(&format!("${UNSET}")), "");
}

#[test]
fn text_that_is_not_a_reference_is_left_alone() {
    assert_eq!(
        expand_path("/usr/local/bin:/usr/bin"),
        "/usr/local/bin:/usr/bin"
    );
    // A bare `$` is a literal dollar, and an unclosed `${` is not a
    // reference at all — neither is an excuse to drop the segment.
    assert_eq!(
        expand_segment("/opt/cost$/bin"),
        Some("/opt/cost$/bin".to_owned())
    );
    assert_eq!(
        expand_segment("/opt/${unclosed/bin"),
        Some("/opt/${unclosed/bin".to_owned())
    );
}

/// The shipped default must survive its own expansion: everything after
/// the user prefix is literal, so the system locations are always there
/// whether or not `HOME` is.
#[test]
fn the_default_always_keeps_its_system_locations() {
    let expanded = expand_path(&crate::config::default_command_path());
    for system in ["/usr/local/bin", "/usr/bin", "/bin"] {
        assert!(
            expanded.split(':').any(|segment| segment == system),
            "{system} missing from {expanded}"
        );
    }
    assert!(!expanded.contains('$'), "unresolved reference left in {expanded}");
}
