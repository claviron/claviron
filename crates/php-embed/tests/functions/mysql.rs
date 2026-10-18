use super::{Value, transaction_statement, types};

#[test]
fn mysql_transaction_settings_are_separate_statements() {
    assert_eq!(
        transaction_statement(Some("serializable"), Some(true)).unwrap(),
        [
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            "START TRANSACTION READ ONLY",
        ]
    );
    assert!(transaction_statement(Some("snapshot; COMMIT"), None).is_err());
    assert_eq!(
        transaction_statement(None, None).unwrap(),
        ["START TRANSACTION"]
    );
}

#[test]
fn unsigned_values_do_not_overflow_php_integers() {
    assert!(matches!(types::unsigned(i64::MAX as u64), Value::I64(i64::MAX)));
    assert!(
        matches!(types::unsigned(u64::MAX), Value::String(value) if value == "18446744073709551615")
    );
}
