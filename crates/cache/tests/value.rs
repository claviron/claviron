use serde_json::json;

use super::{
    ArchivedCacheJson, CacheJson, CacheValue, CacheValueRef, MAGIC_V1,
    MAGIC_V2, TYPE_JSON,
};

#[test]
fn cache_value_roundtrips_scalars() {
    let values = [
        CacheValue::Null,
        CacheValue::Bool(true),
        CacheValue::I64(-7),
        CacheValue::U64(42),
        CacheValue::F64(1.25),
        CacheValue::String("hello".to_string()),
        CacheValue::Bytes(vec![1, 2, 3]),
    ];

    for value in values {
        let encoded = value.encode().expect("encode");
        assert_eq!(CacheValue::decode(&encoded).expect("decode"), value);
    }
}

#[test]
fn cache_value_roundtrips_composite_values_through_rkyv() {
    let value = CacheValue::from_json(json!({
        "enabled": true,
        "items": [1, -2, 3.5, null, "done"],
        "nested": {"answer": 42}
    }));

    let encoded = value.encode().expect("encode");
    assert!(encoded.starts_with(MAGIC_V2));
    assert_eq!(CacheValue::decode(&encoded).expect("decode"), value);
}

#[test]
fn cache_value_decodes_legacy_json_envelopes() {
    let mut encoded = Vec::from(MAGIC_V1);
    encoded.push(TYPE_JSON);
    encoded.extend_from_slice(br#"{"enabled":true,"items":[1,"done"]}"#);

    let decoded = CacheValue::decode(&encoded).expect("decode legacy json");
    assert_eq!(
        decoded.to_json(),
        json!({"enabled": true, "items": [1, "done"]})
    );
}

#[test]
fn cache_value_rejects_malformed_rkyv_envelopes() {
    let mut encoded = Vec::from(MAGIC_V2);
    encoded.extend_from_slice(b"not an archive");

    assert!(CacheValue::decode(&encoded).is_err());
    assert!(CacheValue::view(&encoded).is_err());
}

#[test]
fn cache_value_decodes_rkyv_from_an_unaligned_slice() {
    let value = CacheValue::Json(CacheJson::Array(vec![
        CacheJson::String("hello".to_string()),
        CacheJson::U64(42),
    ]));
    let encoded = value.encode().expect("encode");
    let mut unaligned = Vec::with_capacity(encoded.len() + 1);
    unaligned.push(0);
    unaligned.extend_from_slice(&encoded);

    assert_eq!(CacheValue::decode(&unaligned[1..]).expect("decode"), value);
}

#[test]
fn cache_value_views_strings_and_bytes_in_the_original_buffer() {
    let encoded = CacheValue::String("borrowed".to_owned()).encode().unwrap();
    let CacheValueRef::String(text) = CacheValue::view(&encoded).unwrap()
    else {
        panic!("expected string view");
    };
    assert_eq!(text, "borrowed");
    assert_eq!(text.as_ptr(), encoded[MAGIC_V1.len() + 1..].as_ptr());

    let encoded = CacheValue::Bytes(vec![0, 255, 1]).encode().unwrap();
    let CacheValueRef::Bytes(data) = CacheValue::view(&encoded).unwrap() else {
        panic!("expected binary view");
    };
    assert_eq!(data, [0, 255, 1]);
    assert_eq!(data.as_ptr(), encoded[MAGIC_V1.len() + 1..].as_ptr());

    let raw = b"raw bytes";
    let CacheValueRef::Bytes(data) = CacheValue::view(raw).unwrap() else {
        panic!("expected raw binary view");
    };
    assert_eq!(data.as_ptr(), raw.as_ptr());
}

#[test]
fn cache_value_views_nested_archives_without_deserializing() {
    let value = CacheValue::Json(CacheJson::Object(vec![(
        "nested-key".to_owned(),
        CacheJson::Array(vec![CacheJson::String(
            "a long archived string stored outside the inline representation"
                .to_owned(),
        )]),
    )]));
    let encoded = value.encode().unwrap();
    let mut unaligned = vec![0];
    unaligned.extend_from_slice(&encoded);
    let buffer = &unaligned[1..];
    let view = CacheValue::view(buffer).unwrap();
    let CacheValueRef::Json(ArchivedCacheJson::Object(entries)) = &view else {
        panic!("expected archived object");
    };
    let entry = &entries[0];
    assert_eq!(entry.0.as_str(), "nested-key");
    let ArchivedCacheJson::Array(values) = &entry.1 else {
        panic!("expected archived array");
    };
    let ArchivedCacheJson::String(text) = &values[0] else {
        panic!("expected archived string");
    };
    let start = buffer.as_ptr() as usize;
    let end = start + buffer.len();
    for (ptr, len) in [
        (entries.as_ptr() as usize, std::mem::size_of_val(&entries[0])),
        (values.as_ptr() as usize, std::mem::size_of_val(&values[0])),
        (entry.0.as_str().as_ptr() as usize, entry.0.len()),
        (text.as_str().as_ptr() as usize, text.len()),
    ] {
        assert!(
            ptr >= start && ptr + len <= end,
            "view must borrow its buffer"
        );
    }
    assert_eq!(view.into_owned().unwrap(), value);
}
