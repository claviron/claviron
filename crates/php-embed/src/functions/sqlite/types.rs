//! SQLite values follow each cell's storage class, not the declared column
//! affinity: one column can contain integers, text and blobs in different rows.

use sqlx::query::Query;
use sqlx::sqlite::{SqliteArguments, SqliteRow};
use sqlx::{Row, Sqlite, TypeInfo, ValueRef};

use super::{Parameter, Value};

pub(super) fn bind<'q>(
    query: Query<'q, Sqlite, SqliteArguments>,
    parameter: Parameter,
) -> Query<'q, Sqlite, SqliteArguments> {
    match parameter {
        Parameter::Null => query.bind(Option::<String>::None),
        Parameter::Boolean(value) => query.bind(value),
        Parameter::I32(value) => query.bind(value),
        Parameter::I64(value) => query.bind(value),
        Parameter::F64(value) => query.bind(value),
        Parameter::String(value) => query.bind(value),
        Parameter::Bytes(value) => query.bind(value),
        Parameter::Json(value) => query.bind(value.to_string()),
    }
}

pub(super) fn decode(
    row: &SqliteRow,
    index: usize,
    column: &str,
    _declared_kind: &str,
) -> Result<Value, sqlx::Error> {
    let raw = row.try_get_raw(index)?;
    if raw.is_null() {
        return Ok(Value::Null);
    }
    Ok(match raw.type_info().name() {
        "INTEGER" | "INT4" | "BOOLEAN" => {
            Value::I64(row.try_get_unchecked(index)?)
        }
        "REAL" => Value::F64(row.try_get_unchecked(index)?),
        "TEXT" => Value::String(row.try_get_unchecked(index)?),
        "BLOB" => Value::Bytes(row.try_get_unchecked(index)?),
        kind => {
            return Err(sqlx::Error::Decode(
                format!("unsupported SQLite column {column}: {kind}").into(),
            ));
        }
    })
}
