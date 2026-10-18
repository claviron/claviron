use super::*;

#[test]
fn identity_is_normalized_and_runtime_qualified() {
    let id = ApplicationId::new(
        ApplicationRuntime::new(" php ").unwrap(),
        ApplicationName::new(" storefront ").unwrap(),
    );
    assert_eq!(id.to_string(), "php:storefront");
}

#[test]
fn identity_rejects_empty_components() {
    assert_eq!(
        ApplicationRuntime::new("  ").unwrap_err(),
        ApplicationIdentityError::EmptyRuntime
    );
    assert_eq!(
        ApplicationName::new("").unwrap_err(),
        ApplicationIdentityError::EmptyName
    );
}
