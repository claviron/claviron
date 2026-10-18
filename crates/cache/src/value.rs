use std::error::Error as StdError;
use std::fmt;

use rkyv::rancor::Error as RkyvError;
use rkyv::{Archive, Deserialize, Serialize};
use serde_json::{Map, Number, Value};

const MAGIC_V1: &[u8; 6] = b"CLVCV1";
const MAGIC_V2: &[u8; 6] = b"CLVCV2";
const TYPE_NULL: u8 = 0;
const TYPE_BOOL: u8 = 1;
const TYPE_I64: u8 = 2;
const TYPE_U64: u8 = 3;
const TYPE_F64: u8 = 4;
const TYPE_STRING: u8 = 5;
const TYPE_BYTES: u8 = 6;
const TYPE_JSON: u8 = 7;

pub type ValueResult<T> = std::result::Result<T, ValueError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueError(String);

impl ValueError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ValueError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl StdError for ValueError {}

/// Stable value envelope shared by cache frontends.
///
/// Stores remain byte-oriented. Unheadered bytes decode as
/// [`CacheValue::Bytes`] for compatibility with existing raw entries.
#[derive(Clone, Debug, PartialEq)]
pub enum CacheValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Json(CacheJson),
}

/// A validated read of a cache envelope, borrowing its immutable byte buffer.
///
/// Strings, binary values and current composite archives are read in place.
/// The buffer owner must outlive this view; no references into cache locks or
/// overwriteable ring slots escape. Only the former JSON-text envelope needs
/// an owned tree. Call [`Self::into_owned`] when an independent value is needed.
#[derive(Debug)]
pub enum CacheValueRef<'a> {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(&'a str),
    Bytes(&'a [u8]),
    Json(&'a ArchivedCacheJson),
    LegacyJson(CacheJson),
}

impl CacheValueRef<'_> {
    pub fn into_owned(self) -> ValueResult<CacheValue> {
        Ok(match self {
            Self::Null => CacheValue::Null,
            Self::Bool(value) => CacheValue::Bool(value),
            Self::I64(value) => CacheValue::I64(value),
            Self::U64(value) => CacheValue::U64(value),
            Self::F64(value) => CacheValue::F64(value),
            Self::String(value) => CacheValue::String(value.to_owned()),
            Self::Bytes(value) => CacheValue::Bytes(value.to_vec()),
            Self::Json(value) => CacheValue::Json(
                rkyv::deserialize::<CacheJson, RkyvError>(value).map_err(
                    |error| {
                        ValueError::new(format!(
                            "cache value archive decode failed: {error}"
                        ))
                    },
                )?,
            ),
            Self::LegacyJson(value) => CacheValue::Json(value),
        })
    }
}

/// A language-neutral JSON-shaped cache tree.
///
/// New composite values are archived with rkyv instead of being rendered to
/// JSON text. The type remains restricted to JSON semantics so cache adapters
/// agree on numbers, string object keys, arrays, and objects.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[rkyv(derive(Debug))]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
pub enum CacheJson {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Array(#[rkyv(omit_bounds)] Vec<CacheJson>),
    Object(#[rkyv(omit_bounds)] Vec<(String, CacheJson)>),
}

impl CacheJson {
    pub fn from_json(value: Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Bool(value),
            Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    Self::I64(value)
                } else if let Some(value) = value.as_u64() {
                    Self::U64(value)
                } else if let Some(value) = value.as_f64() {
                    Self::F64(value)
                } else {
                    unreachable!(
                        "serde_json numbers are representable as i64, u64, or f64"
                    )
                }
            }
            Value::String(value) => Self::String(value),
            Value::Array(values) => {
                Self::Array(values.into_iter().map(Self::from_json).collect())
            }
            Value::Object(values) => Self::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, Self::from_json(value)))
                    .collect(),
            ),
        }
    }

    pub fn to_json(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(value) => Value::Bool(*value),
            Self::I64(value) => Value::Number(Number::from(*value)),
            Self::U64(value) => Value::Number(Number::from(*value)),
            Self::F64(value) => Number::from_f64(*value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Self::String(value) => Value::String(value.clone()),
            Self::Array(values) => {
                Value::Array(values.iter().map(Self::to_json).collect())
            }
            Self::Object(values) => Value::Object(
                values
                    .iter()
                    .map(|(key, value)| (key.clone(), value.to_json()))
                    .collect(),
            ),
        }
    }
}

impl CacheValue {
    pub fn from_json(value: Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Bool(value),
            Value::Number(value) => number_to_cache_value(value),
            Value::String(value) => Self::String(value),
            Value::Array(_) | Value::Object(_) => {
                Self::Json(CacheJson::from_json(value))
            }
        }
    }

    pub fn to_json(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(value) => Value::Bool(*value),
            Self::I64(value) => Value::Number(Number::from(*value)),
            Self::U64(value) => Value::Number(Number::from(*value)),
            Self::F64(value) => Number::from_f64(*value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Self::String(value) => Value::String(value.clone()),
            Self::Bytes(value) => {
                let mut object = Map::new();
                object.insert(
                    "type".to_string(),
                    Value::String("bytes".to_string()),
                );
                object.insert(
                    "value".to_string(),
                    Value::Array(
                        value
                            .iter()
                            .map(|byte| Value::Number(Number::from(*byte)))
                            .collect(),
                    ),
                );
                Value::Object(object)
            }
            Self::Json(value) => value.to_json(),
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::I64(_) => "i64",
            Self::U64(_) => "u64",
            Self::F64(_) => "f64",
            Self::String(_) => "string",
            Self::Bytes(_) => "bytes",
            Self::Json(_) => "json",
        }
    }

    pub fn encode(&self) -> ValueResult<Vec<u8>> {
        let mut out = Vec::new();

        match self {
            Self::Null => {
                out.extend_from_slice(MAGIC_V1);
                out.push(TYPE_NULL);
            }
            Self::Bool(value) => {
                out.extend_from_slice(MAGIC_V1);
                out.push(TYPE_BOOL);
                out.push(u8::from(*value));
            }
            Self::I64(value) => {
                out.extend_from_slice(MAGIC_V1);
                out.push(TYPE_I64);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Self::U64(value) => {
                out.extend_from_slice(MAGIC_V1);
                out.push(TYPE_U64);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Self::F64(value) => {
                out.extend_from_slice(MAGIC_V1);
                out.push(TYPE_F64);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Self::String(value) => {
                out.extend_from_slice(MAGIC_V1);
                out.push(TYPE_STRING);
                out.extend_from_slice(value.as_bytes());
            }
            Self::Bytes(value) => {
                out.extend_from_slice(MAGIC_V1);
                out.push(TYPE_BYTES);
                out.extend_from_slice(value);
            }
            Self::Json(value) => {
                out.extend_from_slice(MAGIC_V2);
                out.extend_from_slice(
                    &rkyv::to_bytes::<RkyvError>(value).map_err(|error| {
                        ValueError::new(format!(
                            "cache value archive encode failed: {error}"
                        ))
                    })?,
                );
            }
        }

        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> ValueResult<Self> {
        Self::view(bytes)?.into_owned()
    }

    /// Validate and borrow an envelope without allocating a decoded tree.
    ///
    /// Composite reads use rkyv's checked access, never an unchecked cast.
    /// The archive format is unaligned, so a sliced buffer or envelope prefix
    /// does not impose an alignment requirement on the storage driver.
    pub fn view(bytes: &[u8]) -> ValueResult<CacheValueRef<'_>> {
        if bytes.starts_with(MAGIC_V2) {
            let payload = &bytes[MAGIC_V2.len()..];
            return rkyv::access::<ArchivedCacheJson, RkyvError>(payload)
                .map(CacheValueRef::Json)
                .map_err(|error| {
                    ValueError::new(format!(
                        "cache value archive decode failed: {error}"
                    ))
                });
        }

        if bytes.len() < MAGIC_V1.len() + 1 || !bytes.starts_with(MAGIC_V1) {
            return Ok(CacheValueRef::Bytes(bytes));
        }

        let tag = bytes[MAGIC_V1.len()];
        let payload = &bytes[MAGIC_V1.len() + 1..];
        match tag {
            TYPE_NULL if payload.is_empty() => Ok(CacheValueRef::Null),
            TYPE_BOOL if payload.len() == 1 => {
                Ok(CacheValueRef::Bool(payload[0] != 0))
            }
            TYPE_I64 if payload.len() == 8 => {
                Ok(CacheValueRef::I64(i64::from_le_bytes(
                    payload.try_into().expect("checked i64 cache value length"),
                )))
            }
            TYPE_U64 if payload.len() == 8 => {
                Ok(CacheValueRef::U64(u64::from_le_bytes(
                    payload.try_into().expect("checked u64 cache value length"),
                )))
            }
            TYPE_F64 if payload.len() == 8 => {
                Ok(CacheValueRef::F64(f64::from_le_bytes(
                    payload.try_into().expect("checked f64 cache value length"),
                )))
            }
            TYPE_STRING => std::str::from_utf8(payload)
                .map(CacheValueRef::String)
                .map_err(|error| {
                    ValueError::new(format!(
                        "cache value string decode failed: {error}"
                    ))
                }),
            TYPE_BYTES => Ok(CacheValueRef::Bytes(payload)),
            TYPE_JSON => serde_json::from_slice(payload)
                .map(CacheJson::from_json)
                .map(CacheValueRef::LegacyJson)
                .map_err(|error| {
                    ValueError::new(format!(
                        "legacy cache value json decode failed: {error}"
                    ))
                }),
            _ => Err(ValueError::new("invalid cache value envelope")),
        }
    }
}

fn number_to_cache_value(value: Number) -> CacheValue {
    if let Some(value) = value.as_i64() {
        CacheValue::I64(value)
    } else if let Some(value) = value.as_u64() {
        CacheValue::U64(value)
    } else if let Some(value) = value.as_f64() {
        CacheValue::F64(value)
    } else {
        CacheValue::Json(CacheJson::from_json(Value::Number(value)))
    }
}

#[cfg(test)]
#[path = "../tests/value.rs"]
mod tests;
