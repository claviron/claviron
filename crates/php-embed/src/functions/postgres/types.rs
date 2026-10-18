//! PostgreSQL type coercion between PHP values and the wire protocol.
//!
//! PDO sends every bound parameter with an unspecified type OID and lets
//! PostgreSQL infer the type from the statement. sqlx sends typed binary
//! parameters instead, so a naive bridge encodes `int` as `INT8` and every
//! string as `TEXT`, which PostgreSQL rejects for `timestamp`, `uuid`,
//! `numeric` and enum columns.
//!
//! This module keeps sqlx's typed binary encoding but takes the target type
//! from the statement description, so the server still decides what each
//! placeholder is. Decoding mirrors what PDO hands to PHP on PHP 8.1+:
//! native `bool`/`int`/`float` for the numeric and boolean types, and the
//! PostgreSQL *text* representation for everything else (timestamps, uuid,
//! numeric, json, arrays), because Eloquent casts parse strings.

use sqlx::encode::{Encode, IsNull};
use sqlx::error::BoxDynError;
use sqlx::postgres::types::{Oid, PgInterval, PgTimeTz};
use sqlx::postgres::{PgArgumentBuffer, PgArguments, PgRow, PgTypeInfo};
use sqlx::query::Query;
use sqlx::types::{BigDecimal, Json, Uuid};
use sqlx::{Postgres, Row, Type, TypeInfo, ValueRef};
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

use super::{Parameter, Value};

type BoundQuery<'q> = Query<'q, Postgres, PgArguments>;

/// A value encoded as its PostgreSQL *text* representation while declaring a
/// concrete type OID.
///
/// Only valid for types whose binary input format is the text form: the text
/// family, `json`, enums, `citext`, `ltree`, `xml`. It is the fallback for
/// user-defined types the bridge does not model, which is how enum columns
/// keep working without a compile-time list of every enum in the database.
struct PgText {
    value: String,
    oid: Oid,
}

impl Type<Postgres> for PgText {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_oid(Oid(0))
    }

    fn compatible(_: &PgTypeInfo) -> bool {
        true
    }
}

impl Encode<'_, Postgres> for PgText {
    fn encode_by_ref(
        &self,
        buffer: &mut PgArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        buffer.extend_from_slice(self.value.as_bytes());
        Ok(IsNull::No)
    }

    fn produces(&self) -> Option<PgTypeInfo> {
        Some(PgTypeInfo::with_oid(self.oid))
    }
}

/// `NULL` carrying the placeholder's own type OID, so PostgreSQL never has to
/// guess the type of a null parameter.
struct PgNull {
    oid: Oid,
}

impl Type<Postgres> for PgNull {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_oid(Oid(0))
    }

    fn compatible(_: &PgTypeInfo) -> bool {
        true
    }
}

impl Encode<'_, Postgres> for PgNull {
    fn encode_by_ref(
        &self,
        _buffer: &mut PgArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        Ok(IsNull::Yes)
    }

    fn produces(&self) -> Option<PgTypeInfo> {
        Some(PgTypeInfo::with_oid(self.oid))
    }
}

fn oid_of(target: Option<&PgTypeInfo>) -> Oid {
    target.and_then(PgTypeInfo::oid).unwrap_or(Oid(0))
}

/// Normalised type name: `sqlx` spells arrays either `_INT4` or `INT4[]`
/// depending on how the type was resolved, and casing is not guaranteed.
fn type_key(target: Option<&PgTypeInfo>) -> Option<String> {
    target.map(|info| info.name().to_ascii_uppercase())
}

fn element_key(name: &str) -> Option<&str> {
    name.strip_suffix("[]").or_else(|| name.strip_prefix('_'))
}

// ── Parameter encoding ───────────────────────────────────────────────

/// Binds one PHP parameter against the type PostgreSQL reported for that
/// placeholder. `target` is `None` when the statement could not be described,
/// in which case the natural PHP type is used.
pub(super) fn bind<'q>(
    query: BoundQuery<'q>,
    parameter: Parameter,
    target: Option<&PgTypeInfo>,
) -> Result<BoundQuery<'q>, BoxDynError> {
    let oid = oid_of(target);
    if matches!(parameter, Parameter::Null) {
        return Ok(query.bind(PgNull { oid }));
    }
    let Some(name) = type_key(target) else {
        return bind_natural(query, parameter);
    };

    if let Some(element) = element_key(&name) {
        return bind_array(query, parameter, element);
    }

    match name.as_str() {
        "BOOL" => Ok(query.bind(as_bool(parameter)?)),
        "INT2" => Ok(query.bind(as_i64(parameter).and_then(|value| {
            i16::try_from(value)
                .map_err(|_| encode_error("value does not fit in int2"))
        })?)),
        "INT4" | "OID" => {
            Ok(query.bind(as_i64(parameter).and_then(|value| {
                i32::try_from(value)
                    .map_err(|_| encode_error("value does not fit in int4"))
            })?))
        }
        "INT8" => Ok(query.bind(as_i64(parameter)?)),
        "FLOAT4" => Ok(query.bind(as_f64(parameter)? as f32)),
        "FLOAT8" => Ok(query.bind(as_f64(parameter)?)),
        "NUMERIC" => Ok(query.bind(as_numeric(parameter)?)),
        "UUID" => Ok(query.bind(as_uuid(parameter)?)),
        "DATE" => Ok(query.bind(as_date(parameter)?)),
        "TIME" => Ok(query.bind(as_time(parameter)?)),
        "TIMETZ" => Ok(query.bind(as_time_tz(parameter)?)),
        "TIMESTAMP" => Ok(query.bind(as_timestamp(parameter)?)),
        "TIMESTAMPTZ" => Ok(query.bind(as_timestamptz(parameter)?)),
        "INTERVAL" => Ok(query.bind(as_interval(parameter)?)),
        "BYTEA" => Ok(query.bind(as_bytes(parameter)?)),
        "JSON" | "JSONB" => Ok(query.bind(Json(as_json(parameter)?))),
        // Text family and everything the bridge does not model (enums,
        // citext, ltree, domains over text): send the text form under the
        // placeholder's own OID.
        _ => Ok(query.bind(PgText { value: as_string(parameter)?, oid })),
    }
}

/// Used when PostgreSQL could not describe the placeholder.
///
/// A string goes out with no type (OID 0), the way PDO sends every parameter:
/// the server infers `timestamptz`, `numeric` or `uuid` from where the
/// placeholder sits. Typing it `text` here would turn a describe failure into
/// an unrelated "operator does not exist: timestamptz >= text" and hide the
/// error the statement really has.
fn bind_natural<'q>(
    query: BoundQuery<'q>,
    parameter: Parameter,
) -> Result<BoundQuery<'q>, BoxDynError> {
    Ok(match parameter {
        Parameter::Null => query.bind(PgNull { oid: Oid(0) }),
        Parameter::Boolean(value) => query.bind(value),
        Parameter::I32(value) => query.bind(value),
        Parameter::I64(value) => query.bind(value),
        Parameter::F64(value) => query.bind(value),
        Parameter::String(value) => query.bind(PgText { value, oid: Oid(0) }),
        Parameter::Bytes(value) => query.bind(value),
        Parameter::Json(value) => query.bind(Json(value)),
    })
}

fn bind_array<'q>(
    query: BoundQuery<'q>,
    parameter: Parameter,
    element: &str,
) -> Result<BoundQuery<'q>, BoxDynError> {
    let items = as_array(parameter)?;
    match element {
        "BOOL" => Ok(query.bind(map_array(items, as_bool)?)),
        "INT2" => Ok(query.bind(map_array(items, |item| {
            as_i64(item).and_then(|value| {
                i16::try_from(value)
                    .map_err(|_| encode_error("value does not fit in int2"))
            })
        })?)),
        "INT4" => Ok(query.bind(map_array(items, |item| {
            as_i64(item).and_then(|value| {
                i32::try_from(value)
                    .map_err(|_| encode_error("value does not fit in int4"))
            })
        })?)),
        "INT8" => Ok(query.bind(map_array(items, as_i64)?)),
        "FLOAT8" => Ok(query.bind(map_array(items, as_f64)?)),
        "NUMERIC" => Ok(query.bind(map_array(items, as_numeric)?)),
        "UUID" => Ok(query.bind(map_array(items, as_uuid)?)),
        "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "CHAR" => {
            Ok(query.bind(map_array(items, as_string)?))
        }
        _ => Err(encode_error(&format!(
            "unsupported PostgreSQL array element type '{element}'; pass the array literal as a \
             string instead"
        ))),
    }
}

fn map_array<T>(
    items: Vec<Parameter>,
    convert: impl Fn(Parameter) -> Result<T, BoxDynError>,
) -> Result<Vec<Option<T>>, BoxDynError> {
    items
        .into_iter()
        .map(|item| match item {
            Parameter::Null => Ok(None),
            item => convert(item).map(Some),
        })
        .collect()
}

fn as_array(parameter: Parameter) -> Result<Vec<Parameter>, BoxDynError> {
    match parameter {
        Parameter::Json(serde_json::Value::Array(values)) => {
            values.into_iter().map(json_to_parameter).collect()
        }
        _ => Err(encode_error(
            "PostgreSQL array parameters must be passed as a PHP array",
        )),
    }
}

fn json_to_parameter(
    value: serde_json::Value
) -> Result<Parameter, BoxDynError> {
    Ok(match value {
        serde_json::Value::Null => Parameter::Null,
        serde_json::Value::Bool(value) => Parameter::Boolean(value),
        serde_json::Value::String(value) => Parameter::String(value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Parameter::I64(value)
            } else if let Some(value) = value.as_f64() {
                Parameter::F64(value)
            } else {
                Parameter::String(value.to_string())
            }
        }
        value => Parameter::Json(value),
    })
}

fn encode_error(message: &str) -> BoxDynError {
    message.to_owned().into()
}

fn as_bool(parameter: Parameter) -> Result<bool, BoxDynError> {
    match parameter {
        Parameter::Boolean(value) => Ok(value),
        Parameter::I32(value) => Ok(value != 0),
        Parameter::I64(value) => Ok(value != 0),
        Parameter::String(value) => {
            match value.trim().to_ascii_lowercase().as_str() {
                "t" | "true" | "y" | "yes" | "on" | "1" => Ok(true),
                "f" | "false" | "n" | "no" | "off" | "0" | "" => Ok(false),
                other => Err(encode_error(&format!(
                    "cannot bind '{other}' as a PostgreSQL boolean"
                ))),
            }
        }
        _ => {
            Err(encode_error("cannot bind this value as a PostgreSQL boolean"))
        }
    }
}

fn as_i64(parameter: Parameter) -> Result<i64, BoxDynError> {
    match parameter {
        Parameter::Boolean(value) => Ok(i64::from(value)),
        Parameter::I32(value) => Ok(i64::from(value)),
        Parameter::I64(value) => Ok(value),
        Parameter::F64(value) if value.fract() == 0.0 => Ok(value as i64),
        Parameter::String(value) => value.trim().parse::<i64>().map_err(|_| {
            encode_error(&format!(
                "cannot bind '{value}' as a PostgreSQL integer"
            ))
        }),
        _ => {
            Err(encode_error("cannot bind this value as a PostgreSQL integer"))
        }
    }
}

fn as_f64(parameter: Parameter) -> Result<f64, BoxDynError> {
    match parameter {
        Parameter::I32(value) => Ok(f64::from(value)),
        Parameter::I64(value) => Ok(value as f64),
        Parameter::F64(value) => Ok(value),
        Parameter::String(value) => value.trim().parse::<f64>().map_err(|_| {
            encode_error(&format!(
                "cannot bind '{value}' as a PostgreSQL float"
            ))
        }),
        _ => Err(encode_error("cannot bind this value as a PostgreSQL float")),
    }
}

fn as_numeric(parameter: Parameter) -> Result<BigDecimal, BoxDynError> {
    use std::str::FromStr;

    let text = match parameter {
        Parameter::I32(value) => value.to_string(),
        Parameter::I64(value) => value.to_string(),
        Parameter::F64(value) => value.to_string(),
        Parameter::String(value) => value,
        _ => {
            return Err(encode_error(
                "cannot bind this value as PostgreSQL numeric",
            ));
        }
    };
    BigDecimal::from_str(text.trim()).map_err(|_| {
        encode_error(&format!("cannot bind '{text}' as PostgreSQL numeric"))
    })
}

fn as_uuid(parameter: Parameter) -> Result<Uuid, BoxDynError> {
    let text = as_string(parameter)?;
    Uuid::parse_str(text.trim()).map_err(|_| {
        encode_error(&format!("cannot bind '{text}' as a PostgreSQL uuid"))
    })
}

fn as_bytes(parameter: Parameter) -> Result<Vec<u8>, BoxDynError> {
    match parameter {
        Parameter::Bytes(value) => Ok(value),
        Parameter::String(value) => Ok(value.into_bytes()),
        _ => Err(encode_error("cannot bind this value as PostgreSQL bytea")),
    }
}

fn as_json(parameter: Parameter) -> Result<serde_json::Value, BoxDynError> {
    match parameter {
        Parameter::Json(value) => Ok(value),
        Parameter::String(value) => {
            serde_json::from_str(&value).map_err(|error| {
                encode_error(&format!("invalid JSON parameter: {error}"))
            })
        }
        Parameter::Boolean(value) => Ok(serde_json::Value::Bool(value)),
        Parameter::I32(value) => Ok(serde_json::Value::from(value)),
        Parameter::I64(value) => Ok(serde_json::Value::from(value)),
        Parameter::F64(value) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| encode_error("JSON floats must be finite")),
        Parameter::Bytes(_) | Parameter::Null => {
            Err(encode_error("cannot bind this value as PostgreSQL json"))
        }
    }
}

fn as_string(parameter: Parameter) -> Result<String, BoxDynError> {
    Ok(match parameter {
        Parameter::String(value) => value,
        Parameter::Boolean(value) => if value { "t" } else { "f" }.to_owned(),
        Parameter::I32(value) => value.to_string(),
        Parameter::I64(value) => value.to_string(),
        Parameter::F64(value) => value.to_string(),
        Parameter::Json(value) => value.to_string(),
        Parameter::Bytes(value) => String::from_utf8(value).map_err(|_| {
            encode_error("binary parameter is not valid UTF-8 text")
        })?,
        Parameter::Null => {
            return Err(encode_error("null handled before string coercion"));
        }
    })
}

// ── Date/time parsing ────────────────────────────────────────────────
//
// Laravel binds dates as 'Y-m-d H:i:s' (PostgresGrammar::getDateFormat), and
// applications also pass ISO-8601. Both are parsed here by hand: the `time`
// crate's format descriptions are not reachable through sqlx's re-export, and
// a small parser keeps the accepted shapes explicit.

struct DateParts {
    year: i32,
    month: u8,
    day: u8,
}

struct TimeParts {
    hour: u8,
    minute: u8,
    second: u8,
    micros: u32,
}

fn split_datetime(text: &str) -> (&str, Option<&str>, Option<&str>) {
    let text = text.trim();
    let (date, rest) = match text.find(['T', ' ']) {
        Some(index) => (&text[..index], Some(text[index + 1..].trim())),
        None => (text, None),
    };
    let Some(rest) = rest else {
        return (date, None, None);
    };
    // Offset starts at the first '+'/'-'/'Z' that follows the time, never at
    // index 0 of the time itself.
    let offset_at = rest
        .char_indices()
        .skip(1)
        .find(|(_, character)| matches!(character, '+' | '-' | 'Z' | 'z'))
        .map(|(index, _)| index);
    match offset_at {
        Some(index) => (date, Some(&rest[..index]), Some(&rest[index..])),
        None => (date, Some(rest), None),
    }
}

fn parse_date_parts(text: &str) -> Result<DateParts, BoxDynError> {
    let mut parts = text.split('-');
    let year = parts.next().and_then(|value| value.parse::<i32>().ok());
    let month = parts.next().and_then(|value| value.parse::<u8>().ok());
    let day = parts.next().and_then(|value| value.parse::<u8>().ok());
    match (year, month, day, parts.next()) {
        (Some(year), Some(month), Some(day), None) => {
            Ok(DateParts { year, month, day })
        }
        _ => Err(encode_error(&format!(
            "'{text}' is not a valid PostgreSQL date"
        ))),
    }
}

fn parse_time_parts(text: &str) -> Result<TimeParts, BoxDynError> {
    let (clock, fraction) = match text.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (text, None),
    };
    let mut parts = clock.split(':');
    let hour = parts.next().and_then(|value| value.parse::<u8>().ok());
    let minute = parts.next().and_then(|value| value.parse::<u8>().ok());
    let second = parts.next().map_or(Some(0), |value| value.parse::<u8>().ok());
    let micros = match fraction {
        None => 0,
        Some(fraction) => {
            let digits: String =
                fraction.chars().filter(char::is_ascii_digit).collect();
            let mut padded = format!("{digits:0<6}");
            padded.truncate(6);
            padded.parse::<u32>().unwrap_or(0)
        }
    };
    match (hour, minute, second, parts.next()) {
        (Some(hour), Some(minute), Some(second), None) => {
            Ok(TimeParts { hour, minute, second, micros })
        }
        _ => Err(encode_error(&format!(
            "'{text}' is not a valid PostgreSQL time"
        ))),
    }
}

fn build_date(parts: &DateParts) -> Result<Date, BoxDynError> {
    let month = Month::try_from(parts.month).map_err(|_| {
        encode_error(&format!("month {} is out of range", parts.month))
    })?;
    Date::from_calendar_date(parts.year, month, parts.day)
        .map_err(|error| encode_error(&format!("invalid date: {error}")))
}

fn build_time(parts: &TimeParts) -> Result<Time, BoxDynError> {
    Time::from_hms_micro(parts.hour, parts.minute, parts.second, parts.micros)
        .map_err(|error| encode_error(&format!("invalid time: {error}")))
}

fn parse_offset(text: &str) -> Result<UtcOffset, BoxDynError> {
    if matches!(text, "Z" | "z") {
        return Ok(UtcOffset::UTC);
    }
    let (sign, rest) = match text.split_at(1) {
        ("+", rest) => (1i8, rest),
        ("-", rest) => (-1i8, rest),
        _ => {
            return Err(encode_error(&format!(
                "'{text}' is not a valid UTC offset"
            )));
        }
    };
    let digits: Vec<&str> = rest.split(':').collect();
    let hours =
        digits.first().and_then(|value| value.parse::<i8>().ok()).unwrap_or(0);
    let minutes =
        digits.get(1).and_then(|value| value.parse::<i8>().ok()).unwrap_or(0);
    UtcOffset::from_hms(sign * hours, sign * minutes, 0)
        .map_err(|error| encode_error(&format!("invalid UTC offset: {error}")))
}

fn as_date(parameter: Parameter) -> Result<Date, BoxDynError> {
    let text = as_string(parameter)?;
    let (date, _, _) = split_datetime(&text);
    build_date(&parse_date_parts(date)?)
}

fn as_time(parameter: Parameter) -> Result<Time, BoxDynError> {
    let text = as_string(parameter)?;
    let (first, second, _) = split_datetime(&text);
    build_time(&parse_time_parts(second.unwrap_or(first))?)
}

fn as_time_tz(parameter: Parameter) -> Result<PgTimeTz, BoxDynError> {
    let text = as_string(parameter)?;
    let (first, second, offset) = split_datetime(&text);
    let time = build_time(&parse_time_parts(second.unwrap_or(first))?)?;
    let offset = offset.map_or(Ok(UtcOffset::UTC), parse_offset)?;
    Ok(PgTimeTz { time, offset })
}

fn as_timestamp(
    parameter: Parameter
) -> Result<PrimitiveDateTime, BoxDynError> {
    let text = as_string(parameter)?;
    let (date, time, _) = split_datetime(&text);
    let date = build_date(&parse_date_parts(date)?)?;
    let time = match time {
        Some(time) => build_time(&parse_time_parts(time)?)?,
        None => Time::MIDNIGHT,
    };
    Ok(PrimitiveDateTime::new(date, time))
}

/// A naive timestamp bound to a `timestamptz` placeholder is taken as UTC.
///
/// PostgreSQL would resolve it against the session `TimeZone` instead. Claviron
/// binds it explicitly so the value does not depend on server-side session
/// state, which matches the usual Laravel setup (`app.timezone = UTC`).
/// Applications that store local times should bind an offset in the string.
fn as_timestamptz(parameter: Parameter) -> Result<OffsetDateTime, BoxDynError> {
    let text = as_string(parameter)?;
    let (date, time, offset) = split_datetime(&text);
    let date = build_date(&parse_date_parts(date)?)?;
    let time = match time {
        Some(time) => build_time(&parse_time_parts(time)?)?,
        None => Time::MIDNIGHT,
    };
    let offset = offset.map_or(Ok(UtcOffset::UTC), parse_offset)?;
    Ok(PrimitiveDateTime::new(date, time).assume_offset(offset))
}

fn as_interval(parameter: Parameter) -> Result<PgInterval, BoxDynError> {
    let text = as_string(parameter)?;
    // Only the ISO-like `HH:MM:SS[.ffffff]` and `<n> <unit>` forms Laravel
    // produces are accepted; anything else should be cast in SQL.
    let mut months = 0i32;
    let mut days = 0i32;
    let mut micros = 0i64;
    let mut tokens = text.split_whitespace().peekable();
    while let Some(token) = tokens.next() {
        if token.contains(':') {
            let parts = parse_time_parts(token)?;
            micros += i64::from(parts.hour) * 3_600_000_000
                + i64::from(parts.minute) * 60_000_000
                + i64::from(parts.second) * 1_000_000
                + i64::from(parts.micros);
            continue;
        }
        let Ok(amount) = token.parse::<i64>() else {
            return Err(encode_error(&format!(
                "'{text}' is not a supported interval"
            )));
        };
        let unit = tokens
            .next()
            .unwrap_or("")
            .trim_end_matches('s')
            .to_ascii_lowercase();
        match unit.as_str() {
            "year" => months += (amount * 12) as i32,
            "mon" | "month" => months += amount as i32,
            "week" => days += (amount * 7) as i32,
            "day" => days += amount as i32,
            "hour" => micros += amount * 3_600_000_000,
            "minute" | "min" => micros += amount * 60_000_000,
            "second" | "sec" => micros += amount * 1_000_000,
            other => {
                return Err(encode_error(&format!(
                    "unsupported interval unit '{other}'"
                )));
            }
        }
    }
    Ok(PgInterval { months, days, microseconds: micros })
}

// ── Result decoding ──────────────────────────────────────────────────

/// Decodes one column into the shape PHP expects.
///
/// Native `bool`/`int`/`float` for the numeric and boolean types (what PDO
/// returns on PHP 8.1+ with emulation off), and the PostgreSQL text form for
/// everything else — including `json`/`jsonb`, which Eloquent's `array` and
/// `AsArrayObject` casts decode themselves.
pub(super) fn decode(
    row: &PgRow,
    index: usize,
    column: &str,
    type_name: &str,
) -> Result<Value, sqlx::Error> {
    if row.try_get_raw(index)?.is_null() {
        return Ok(Value::Null);
    }
    let name = type_name.to_ascii_uppercase();

    if let Some(element) = element_key(&name) {
        return decode_array(row, index, column, element);
    }

    Ok(match name.as_str() {
        "BOOL" => Value::Boolean(row.try_get(index)?),
        "INT2" => Value::I32(i32::from(row.try_get::<i16, _>(index)?)),
        "INT4" => Value::I32(row.try_get(index)?),
        "INT8" => Value::I64(row.try_get(index)?),
        "OID" => Value::I64(i64::from(row.try_get::<Oid, _>(index)?.0)),
        "FLOAT4" => Value::F64(f64::from(row.try_get::<f32, _>(index)?)),
        "FLOAT8" => Value::F64(row.try_get(index)?),
        "NUMERIC" => {
            Value::String(row.try_get::<BigDecimal, _>(index)?.to_string())
        }
        "BYTEA" => Value::Bytes(row.try_get(index)?),
        // PDO hands JSON to PHP as text; Eloquent casts decode it.
        "JSON" | "JSONB" => Value::String(
            row.try_get::<Json<serde_json::Value>, _>(index)?.0.to_string(),
        ),
        "UUID" => Value::String(row.try_get::<Uuid, _>(index)?.to_string()),
        "DATE" => Value::String(format_date(row.try_get::<Date, _>(index)?)),
        "TIME" => Value::String(format_time(row.try_get::<Time, _>(index)?)),
        "TIMETZ" => {
            let value = row.try_get::<PgTimeTz, _>(index)?;
            Value::String(format!(
                "{}{}",
                format_time(value.time),
                format_offset(value.offset)
            ))
        }
        "TIMESTAMP" => {
            let value = row.try_get::<PrimitiveDateTime, _>(index)?;
            Value::String(format!(
                "{} {}",
                format_date(value.date()),
                format_time(value.time())
            ))
        }
        "TIMESTAMPTZ" => {
            let value = row.try_get::<OffsetDateTime, _>(index)?;
            Value::String(format!(
                "{} {}{}",
                format_date(value.date()),
                format_time(value.time()),
                format_offset(value.offset())
            ))
        }
        "INTERVAL" => Value::String(format_interval(
            &row.try_get::<PgInterval, _>(index)?,
        )),
        "CHAR" | "NAME" | "TEXT" | "VARCHAR" | "BPCHAR" | "UNKNOWN"
        | "CITEXT" | "XML" => Value::String(row.try_get(index)?),
        // User-defined types the bridge does not model — enums above all —
        // arrive as their text form in the binary protocol.
        _ => decode_text_fallback(row, index, column, type_name)?,
    })
}

fn decode_text_fallback(
    row: &PgRow,
    index: usize,
    column: &str,
    type_name: &str,
) -> Result<Value, sqlx::Error> {
    let raw = row.try_get_raw(index)?;
    let bytes = raw.as_bytes().map_err(sqlx::Error::Decode)?;
    match std::str::from_utf8(bytes) {
        Ok(value) => Ok(Value::String(value.to_owned())),
        Err(_) => Err(sqlx::Error::Decode(
            format!(
                "unsupported PostgreSQL column type '{type_name}' for column '{column}'; cast it \
                 to text in the query"
            )
            .into()
        ))
    }
}

fn decode_array(
    row: &PgRow,
    index: usize,
    column: &str,
    element: &str,
) -> Result<Value, sqlx::Error> {
    let items: Vec<Option<String>> = match element {
        "BOOL" => row
            .try_get::<Vec<Option<bool>>, _>(index)?
            .into_iter()
            .map(|item| {
                item.map(|value| if value { "t" } else { "f" }.to_owned())
            })
            .collect(),
        "INT2" => stringify(row.try_get::<Vec<Option<i16>>, _>(index)?),
        "INT4" => stringify(row.try_get::<Vec<Option<i32>>, _>(index)?),
        "INT8" => stringify(row.try_get::<Vec<Option<i64>>, _>(index)?),
        "FLOAT4" => stringify(row.try_get::<Vec<Option<f32>>, _>(index)?),
        "FLOAT8" => stringify(row.try_get::<Vec<Option<f64>>, _>(index)?),
        "NUMERIC" => {
            stringify(row.try_get::<Vec<Option<BigDecimal>>, _>(index)?)
        }
        "UUID" => stringify(row.try_get::<Vec<Option<Uuid>>, _>(index)?),
        "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "CHAR" => {
            row.try_get::<Vec<Option<String>>, _>(index)?
        }
        _ => {
            return Err(sqlx::Error::Decode(
                format!(
                    "unsupported PostgreSQL array element '{element}' for column '{column}'; cast \
                     it to text in the query"
                )
                .into()
            ));
        }
    };
    Ok(Value::String(render_array(&items)))
}

fn stringify<T: ToString>(items: Vec<Option<T>>) -> Vec<Option<String>> {
    items.into_iter().map(|item| item.map(|value| value.to_string())).collect()
}

/// Renders a PostgreSQL array literal the way the text protocol would, so PHP
/// sees exactly what PDO would have handed it.
fn render_array(items: &[Option<String>]) -> String {
    let mut output = String::from("{");
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        match item {
            None => output.push_str("NULL"),
            Some(value) => {
                let needs_quotes = value.is_empty()
                    || value.eq_ignore_ascii_case("null")
                    || value.contains([',', '{', '}', '"', '\\', ' ']);
                if needs_quotes {
                    output.push('"');
                    for character in value.chars() {
                        if matches!(character, '"' | '\\') {
                            output.push('\\');
                        }
                        output.push(character);
                    }
                    output.push('"');
                } else {
                    output.push_str(value);
                }
            }
        }
    }
    output.push('}');
    output
}

fn format_date(value: Date) -> String {
    format!(
        "{:04}-{:02}-{:02}",
        value.year(),
        u8::from(value.month()),
        value.day()
    )
}

fn format_time(value: Time) -> String {
    let micros = value.microsecond();
    let base = format!(
        "{:02}:{:02}:{:02}",
        value.hour(),
        value.minute(),
        value.second()
    );
    if micros == 0 { base } else { format!("{base}.{micros:06}") }
}

fn format_offset(value: UtcOffset) -> String {
    let (hours, minutes, _) = value.as_hms();
    let sign = if hours < 0 || minutes < 0 { '-' } else { '+' };
    format!("{sign}{:02}:{:02}", hours.abs(), minutes.abs())
}

fn format_interval(value: &PgInterval) -> String {
    let mut parts = Vec::new();
    if value.months != 0 {
        parts.push(format!("{} mons", value.months));
    }
    if value.days != 0 {
        parts.push(format!("{} days", value.days));
    }
    let micros = value.microseconds;
    if micros != 0 || parts.is_empty() {
        let sign = if micros < 0 { "-" } else { "" };
        let micros = micros.abs();
        let seconds = micros / 1_000_000;
        let fraction = micros % 1_000_000;
        let clock = format!(
            "{sign}{:02}:{:02}:{:02}",
            seconds / 3600,
            (seconds % 3600) / 60,
            seconds % 60
        );
        parts.push(if fraction == 0 {
            clock
        } else {
            format!("{clock}.{fraction:06}")
        });
    }
    parts.join(" ")
}

#[cfg(test)]
#[path = "../../../tests/functions/postgres/types.rs"]
mod tests;
