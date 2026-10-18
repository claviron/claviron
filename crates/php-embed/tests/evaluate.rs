use super::{PhpArrayKey, PhpValue};

#[test]
fn returned_php_data_is_owned_and_serializable() {
    let value = PhpValue::Array(vec![
        (PhpArrayKey::String("limit".into()), PhpValue::Int(3)),
        (
            PhpArrayKey::String("options".into()),
            PhpValue::Array(vec![
                (PhpArrayKey::Int(0), PhpValue::Bool(true)),
                (PhpArrayKey::Int(1), PhpValue::Null),
            ]),
        ),
    ]);
    let encoded = serde_yaml::to_string(&value).unwrap();
    assert!(encoded.contains("limit: 3"));
    assert!(encoded.contains("- true"));
    assert!(encoded.contains("- null"));
}

#[test]
fn sparse_arrays_keep_their_keys() {
    let value = PhpValue::Array(vec![(
        PhpArrayKey::Int(9),
        PhpValue::String("nine".into()),
    )]);
    assert!(serde_yaml::to_string(&value).unwrap().contains("9: nine"));
}

#[test]
fn empty_php_array_is_a_list() {
    assert_eq!(
        serde_yaml::to_string(&PhpValue::Array(Vec::new())).unwrap().trim(),
        "[]"
    );
}
