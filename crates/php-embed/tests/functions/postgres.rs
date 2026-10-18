use super::transaction_statement;

#[test]
fn builds_postgres_transaction_settings_without_interpolation() {
    assert_eq!(
        transaction_statement(Some("serializable"), Some(true)).unwrap(),
        "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY"
    );
    assert!(transaction_statement(Some("snapshot"), None).is_err());
}
