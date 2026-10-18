use super::*;
use sqlx::Connection;

#[test]
fn begin_mode_is_a_closed_sqlite_vocabulary() {
    assert_eq!(transaction_statement("DEFERRED").unwrap(), "BEGIN DEFERRED");
    assert_eq!(transaction_statement("IMMEDIATE").unwrap(), "BEGIN IMMEDIATE");
    assert_eq!(transaction_statement("EXCLUSIVE").unwrap(), "BEGIN EXCLUSIVE");
    assert!(transaction_statement("DEFERRED; DROP TABLE entries").is_err());
}

#[test]
fn extended_constraint_codes_preserve_sqlstate_and_detail() {
    assert_eq!(
        describe_database(2067, "UNIQUE constraint failed: entries.name"),
        "SQLSTATE[23000]: Integrity constraint violation: 2067 UNIQUE constraint failed: entries.name"
    );
    assert_eq!(
        describe_database(5, "database is locked"),
        "SQLSTATE[HY000]: General error: 5 database is locked"
    );
}

#[tokio::test]
async fn sqlite_storage_classes_insert_id_and_savepoints() {
    // An isolated SQLx driver fixture, not a PHP engine or application boot.
    let mut connection =
        SqliteConnection::connect("sqlite::memory:").await.unwrap();
    execute_unprepared_on(
        &mut connection,
        "CREATE TABLE entries(id INTEGER PRIMARY KEY, value)",
    )
    .await
    .unwrap();
    let inserted = execute_on(
        &mut connection,
        "INSERT INTO entries(value) VALUES (?)",
        vec![Parameter::Bytes(vec![0, 255])],
    )
    .await
    .unwrap();
    assert_eq!(inserted.insert_id, 1);
    assert_eq!(inserted.affected_rows, 1);
    let result = execute_on(&mut connection, "SELECT value FROM entries UNION ALL SELECT 9223372036854775807 UNION ALL SELECT 'text' UNION ALL SELECT 0.5 UNION ALL SELECT NULL", vec![]).await.unwrap();
    assert!(
        matches!(&result.rows[0][0], Value::Bytes(value) if value == &[0, 255])
    );
    assert!(matches!(result.rows[1][0], Value::I64(i64::MAX)));
    assert!(
        matches!(&result.rows[2][0], Value::String(value) if value == "text")
    );
    assert!(matches!(result.rows[3][0], Value::F64(0.5)));
    assert!(matches!(result.rows[4][0], Value::Null));

    execute_unprepared_on(&mut connection, "BEGIN; INSERT INTO entries(value) VALUES ('outer'); SAVEPOINT trans2; INSERT INTO entries(value) VALUES ('inner'); ROLLBACK TO SAVEPOINT trans2; COMMIT").await.unwrap();
    let count =
        execute_on(&mut connection, "SELECT COUNT(*) FROM entries", vec![])
            .await
            .unwrap();
    assert!(matches!(count.rows[0][0], Value::I64(2)));
}
