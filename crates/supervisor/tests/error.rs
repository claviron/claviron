use super::*;

#[test]
fn bootstrap_keeps_the_registry_error() {
    let result: claviron_runtime::registry::Result<()> = Err(
        claviron_runtime::registry::Error::msg("listener registration failed"),
    );
    let error = result.map_err(Error::Bootstrap).unwrap_err();
    assert!(matches!(
        error,
        Error::Bootstrap(claviron_runtime::registry::Error::Other(_))
    ));
    assert_eq!(
        error.to_string(),
        "bootstrap error: listener registration failed"
    );
    assert!(matches!(Error::bootstrap("invalid worker"), Error::Bootstrap(_)));
}
