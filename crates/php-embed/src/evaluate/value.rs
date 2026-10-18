use std::collections::HashSet;

use ext_php_rs::types::{ArrayKey, Zval};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

/// Owned PHP data. No Zend allocation, reference or resource leaves execution.
#[derive(Clone, Debug, PartialEq)]
pub enum PhpValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<(PhpArrayKey, PhpValue)>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PhpArrayKey {
    Int(i64),
    String(String),
}

impl Serialize for PhpArrayKey {
    fn serialize<S: Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match self {
            Self::Int(value) => serializer.serialize_i64(*value),
            Self::String(value) => serializer.serialize_str(value),
        }
    }
}

impl Serialize for PhpValue {
    fn serialize<S: Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Int(value) => serializer.serialize_i64(*value),
            Self::Float(value) => serializer.serialize_f64(*value),
            Self::String(value) => serializer.serialize_str(value),
            Self::Bytes(value) => serializer.serialize_bytes(value),
            Self::Array(entries) => {
                // PHP lists have contiguous, ordered integer keys from zero.
                // Empty PHP arrays, like json_encode([]), are lists as well.
                if entries.iter().enumerate().all(|(index, (key, _))| {
                    matches!(key, PhpArrayKey::Int(value) if usize::try_from(*value) == Ok(index))
                }) {
                    let mut sequence = serializer.serialize_seq(Some(entries.len()))?;
                    for (_, value) in entries { sequence.serialize_element(value)?; }
                    sequence.end()
                } else {
                    let mut map = serializer.serialize_map(Some(entries.len()))?;
                    for (key, value) in entries { map.serialize_entry(key, value)?; }
                    map.end()
                }
            }
        }
    }
}

/// Decode while Zend's request heap still exists. Recursive arrays cannot be
/// represented as an owned tree; objects/resources are not configuration data.
pub(super) fn decode(
    value: &Zval,
    max_depth: usize,
) -> Result<PhpValue, String> {
    decode_inner(value, max_depth, &mut HashSet::new())
}

fn decode_inner(
    value: &Zval,
    remaining: usize,
    ancestors: &mut HashSet<usize>,
) -> Result<PhpValue, String> {
    let value = value.dereference();
    if value.is_null() {
        return Ok(PhpValue::Null);
    }
    if let Some(value) = value.bool() {
        return Ok(PhpValue::Bool(value));
    }
    if let Some(value) = value.long() {
        return Ok(PhpValue::Int(value));
    }
    if let Some(value) = value.double() {
        return Ok(PhpValue::Float(value));
    }
    if let Some(value) = value.binary_slice::<u8>() {
        return Ok(match std::str::from_utf8(value) {
            Ok(value) => PhpValue::String(value.to_owned()),
            Err(_) => PhpValue::Bytes(value.to_vec()),
        });
    }
    if let Some(array) = value.array() {
        let remaining = remaining
            .checked_sub(1)
            .ok_or("PHP value exceeds the declared maximum array depth")?;
        let identity = std::ptr::from_ref(array) as usize;
        if !ancestors.insert(identity) {
            return Err("PHP value contains a recursive array".into());
        }
        let mut entries = Vec::with_capacity(array.len());
        for (key, value) in array {
            let key = match key {
                ArrayKey::Long(value) => PhpArrayKey::Int(value),
                ArrayKey::String(value) => PhpArrayKey::String(value),
                ArrayKey::Str(value) => PhpArrayKey::String(value.to_owned()),
                ArrayKey::ZendString(value) => PhpArrayKey::String(
                    value
                        .as_str()
                        .map_err(|error| error.to_string())?
                        .to_owned(),
                ),
            };
            entries.push((key, decode_inner(value, remaining, ancestors)?));
        }
        ancestors.remove(&identity);
        return Ok(PhpValue::Array(entries));
    }
    Err("PHP must return data (null, boolean, number, string or array), not an object/resource".into())
}
