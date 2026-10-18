//! PDO-shaped MySQL values. DECIMAL and large unsigned integers remain strings;
//! financial precision must never pass through an f64 conversion.

use sqlx::mysql::types::MySqlTime;
use sqlx::mysql::{MySqlArguments, MySqlRow};
use sqlx::query::Query;
use sqlx::{MySql, Row, ValueRef};
use time::{Date, PrimitiveDateTime};

use super::{Parameter, Value};

pub(super) fn bind(
    query: Query<'_, MySql, MySqlArguments>,
    parameter: Parameter,
) -> Query<'_, MySql, MySqlArguments> {
    match parameter {
        Parameter::Null => query.bind(Option::<String>::None),
        Parameter::Boolean(value) => query.bind(value),
        Parameter::I32(value) => query.bind(value),
        Parameter::I64(value) => query.bind(value),
        Parameter::F64(value) => query.bind(value),
        Parameter::String(value) => query.bind(value),
        Parameter::Bytes(value) => query.bind(value),
        Parameter::Json(value) => query.bind(sqlx::types::Json(value)),
    }
}

pub(super) fn decode(
    row: &MySqlRow,
    index: usize,
    column: &str,
    kind: &str,
) -> Result<Value, sqlx::Error> {
    if row.try_get_raw(index)?.is_null() {
        return Ok(Value::Null);
    }
    Ok(match kind {
        "BOOLEAN" => Value::Boolean(row.try_get(index)?),
        "TINYINT" => Value::I32(i32::from(row.try_get::<i8, _>(index)?)),
        "SMALLINT" => Value::I32(i32::from(row.try_get::<i16, _>(index)?)),
        "MEDIUMINT" | "INT" => Value::I32(row.try_get(index)?),
        "BIGINT" => Value::I64(row.try_get(index)?),
        "TINYINT UNSIGNED" => {
            Value::I32(i32::from(row.try_get::<u8, _>(index)?))
        }
        "SMALLINT UNSIGNED" => {
            Value::I32(i32::from(row.try_get::<u16, _>(index)?))
        }
        "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
            Value::I64(i64::from(row.try_get::<u32, _>(index)?))
        }
        "BIGINT UNSIGNED" => unsigned(row.try_get(index)?),
        "FLOAT" => Value::F64(f64::from(row.try_get::<f32, _>(index)?)),
        "DOUBLE" => Value::F64(row.try_get(index)?),
        // MySQL's binary protocol encodes DECIMAL as text already. Reading
        // that text directly preserves scale as well as value.
        "DECIMAL" | "NEWDECIMAL" => {
            Value::String(row.try_get_unchecked(index)?)
        }
        "DATE" => Value::String(row.try_get::<Date, _>(index)?.to_string()),
        "DATETIME" | "TIMESTAMP" => {
            let value: PrimitiveDateTime = row.try_get(index)?;
            Value::String(format!(
                "{} {:02}:{:02}:{:02}.{:06}",
                value.date(),
                value.hour(),
                value.minute(),
                value.second(),
                value.microsecond()
            ))
        }
        "TIME" => {
            Value::String(row.try_get::<MySqlTime, _>(index)?.to_string())
        }
        "YEAR" => Value::I32(i32::from(row.try_get::<u16, _>(index)?)),
        "JSON" => Value::String(
            row.try_get::<sqlx::types::Json<serde_json::Value>, _>(index)?
                .0
                .to_string(),
        ),
        "BIT" => Value::Bytes(row.try_get_unchecked(index)?),
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB"
        | "LONGBLOB" => Value::Bytes(row.try_get(index)?),
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT"
        | "LONGTEXT" | "ENUM" | "SET" => Value::String(row.try_get(index)?),
        _ => {
            return Err(sqlx::Error::Decode(
                format!("unsupported MySQL column {column}: {kind}").into(),
            ));
        }
    })
}

pub(super) fn unsigned(value: u64) -> Value {
    match i64::try_from(value) {
        Ok(value) => Value::I64(value),
        Err(_) => Value::String(value.to_string()),
    }
}
