use super::*;

#[test]
fn passes_through_when_no_placeholders() {
    let out = expand_env("plain $5 untouched").unwrap();
    assert_eq!(out, "plain $5 untouched");
}

#[test]
fn substitutes_single_var() {
    unsafe { env::set_var("CLAV_T_FOO", "bar") };
    assert_eq!(expand_env("a=${CLAV_T_FOO}").unwrap(), "a=bar");
    unsafe { env::remove_var("CLAV_T_FOO") };
}

#[test]
fn substitutes_multiple_vars() {
    unsafe {
        env::set_var("CLAV_T_A", "1");
        env::set_var("CLAV_T_B", "2");
    }
    assert_eq!(
        expand_env("${CLAV_T_A}-${CLAV_T_B}-${CLAV_T_A}").unwrap(),
        "1-2-1"
    );
    unsafe {
        env::remove_var("CLAV_T_A");
        env::remove_var("CLAV_T_B");
    }
}

#[test]
fn errors_on_missing_var() {
    let err = expand_env("x=${DEFINITELY_NOT_SET_94731}").unwrap_err();
    assert!(err.contains("DEFINITELY_NOT_SET_94731"), "got: {err}");
}

#[test]
fn errors_dedupe_missing() {
    let err = expand_env("${MISS_X1}-${MISS_X2}-${MISS_X1}").unwrap_err();
    assert!(err.contains("MISS_X1"));
    assert!(err.contains("MISS_X2"));
    assert_eq!(err.matches("MISS_X1").count(), 1, "dedupe failed: {err}");
}

#[test]
fn errors_on_unclosed_brace() {
    let err = expand_env("oops ${UNCLOSED").unwrap_err();
    assert!(err.contains("unclosed"));
}

#[test]
fn errors_on_invalid_var_name() {
    let err = expand_env("${1FOO}").unwrap_err();
    assert!(err.contains("invalid"));
}

#[test]
fn drops_pure_comment_lines() {
    // `${UNSET_DOC_REFERENCE_VAR}` inside a YAML comment must not
    // trigger missing-var errors. The comment line is dropped
    // entirely from the output (yaml parser ignores comments
    // anyway, and dropping keeps the intermediate string clean).
    unsafe { env::set_var("CLAV_T_REAL", "ok") };
    let yaml =
        "# docs say ${UNSET_DOC_REFERENCE_VAR} for env\nkey: ${CLAV_T_REAL}\n";
    let out = expand_env(yaml).unwrap();
    assert_eq!(out, "key: ok\n");
    unsafe { env::remove_var("CLAV_T_REAL") };
}

#[test]
fn comment_with_leading_whitespace_dropped() {
    let yaml = "  # ${UNSET_X} in commented-out value\nfoo: bar\n";
    let out = expand_env(yaml).unwrap();
    assert_eq!(out, "foo: bar\n");
}
