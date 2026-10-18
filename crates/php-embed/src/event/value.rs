use std::collections::HashSet;

use claviron_app::{EventArrayKey, EventValue};
use ext_php_rs::types::{ArrayKey, ZendHashTable, Zval};

const MAX_DEPTH: usize = 64;

pub(crate) fn from_zval(value: &Zval) -> Result<EventValue, String> {
    from_zval_inner(value, 0, &mut HashSet::new())
}

fn from_zval_inner(
    value: &Zval,
    depth: usize,
    ancestors: &mut HashSet<usize>,
) -> Result<EventValue, String> {
    if depth > MAX_DEPTH {
        return Err(format!(
            "PHP event payload exceeds maximum nesting depth {MAX_DEPTH}"
        ));
    }

    let value = value.dereference();
    if value.is_null() {
        return Ok(EventValue::Null);
    }
    if let Some(value) = value.bool() {
        return Ok(EventValue::Bool(value));
    }
    if let Some(value) = value.long() {
        return Ok(EventValue::Int(value));
    }
    if let Some(value) = value.double() {
        return Ok(EventValue::Float(value));
    }
    if let Some(value) = value.binary_slice::<u8>() {
        return Ok(EventValue::String(value.to_vec()));
    }
    if let Some(array) = value.array() {
        let identity = std::ptr::from_ref(array) as usize;
        if !ancestors.insert(identity) {
            return Err(
                "PHP event payload contains a recursive array".to_owned()
            );
        }

        let mut entries = Vec::with_capacity(array.len());
        for (key, value) in array {
            let key = match key {
                ArrayKey::Long(value) => EventArrayKey::Int(value),
                ArrayKey::String(value) => EventArrayKey::String(value),
                ArrayKey::Str(value) => EventArrayKey::String(value.to_owned()),
                ArrayKey::ZendString(value) => EventArrayKey::String(
                    value
                        .as_str()
                        .map_err(|_| {
                            "PHP event array keys must be valid UTF-8"
                                .to_owned()
                        })?
                        .to_owned(),
                ),
            };
            entries.push((key, from_zval_inner(value, depth + 1, ancestors)?));
        }
        ancestors.remove(&identity);
        return Ok(EventValue::Array(entries));
    }

    Err("unsupported PHP event payload; expected null, bool, int, float, string, or array"
        .to_owned())
}

pub(crate) fn set_zval(
    source: &EventValue,
    value: &mut Zval,
) -> Result<(), String> {
    match source {
        EventValue::Null => value.set_null(),
        EventValue::Bool(inner) => value.set_bool(*inner),
        EventValue::Int(inner) => value.set_long(*inner),
        EventValue::Float(inner) => value.set_double(*inner),
        EventValue::String(inner) => value.set_binary(inner.clone()),
        EventValue::Array(entries) => {
            let mut array = ZendHashTable::with_capacity(
                entries.len().try_into().unwrap_or(u32::MAX),
            );
            for (key, entry) in entries {
                let mut zval = Zval::new();
                set_zval(entry, &mut zval)?;
                let key = match key {
                    EventArrayKey::Int(value) => ArrayKey::Long(*value),
                    EventArrayKey::String(value) => {
                        ArrayKey::String(value.clone())
                    }
                };
                array.insert(key, zval).map_err(|error| error.to_string())?;
            }
            value.set_hashtable(array);
        }
    }
    Ok(())
}
