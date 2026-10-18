use super::*;

#[test]
fn codec_preserves_array_keys_and_binary_strings() {
    let value = EventValue::Array(vec![
        (EventArrayKey::Int(7), EventValue::String(vec![0, 0xff])),
        (
            EventArrayKey::String("ready".into()),
            EventValue::Array(vec![(
                EventArrayKey::Int(0),
                EventValue::Bool(true),
            )]),
        ),
    ]);
    assert_eq!(EventValue::decode(&value.encode()).unwrap(), value);
}

#[test]
fn codec_rejects_trailing_and_truncated_data() {
    assert!(EventValue::decode(&[NULL, TRUE]).is_err());
    assert!(EventValue::decode(&[STRING, 2, 0, 0, 0, b'a']).is_err());
}
