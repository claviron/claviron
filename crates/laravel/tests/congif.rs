use super::{LaravelConfigParser, config_value};
use claviron_php_embed::evaluate::{PhpArrayKey, PhpValue};
use congif::{ConfigParser, ConfigValue, Variables};

#[test]
fn php_source_is_not_text_interpolated() {
    let parser =
        LaravelConfigParser::with_runner(".".into(), "runner.php".into(), 8);
    let source = "<?php return ['value' => env('KEY'), 'literal' => '${KEY}'];";
    let variables = Variables::from([("KEY".into(), "not-PHP-code".into())]);
    assert_eq!(parser.expand_variables(source, &variables).unwrap(), source);
    assert_eq!(parser.extensions(), &["php"]);
}

#[test]
fn returned_tree_preserves_numbers_lists_and_integer_map_keys() {
    let value = PhpValue::Array(vec![
        (PhpArrayKey::String("limit".into()), PhpValue::Int(42)),
        (PhpArrayKey::String("nullable".into()), PhpValue::Null),
        (
            PhpArrayKey::String("list".into()),
            PhpValue::Array(vec![
                (PhpArrayKey::Int(0), PhpValue::Bool(true)),
                (PhpArrayKey::Int(1), PhpValue::String("value".into())),
            ]),
        ),
        (PhpArrayKey::Int(12), PhpValue::Float(1.5)),
    ]);
    let ConfigValue::Map(map) = config_value(value) else {
        panic!("expected map")
    };
    assert_eq!(map[&ConfigValue::String("limit".into())], ConfigValue::I64(42));
    assert_eq!(map[&ConfigValue::String("nullable".into())], ConfigValue::Unit);
    assert_eq!(
        map[&ConfigValue::String("list".into())],
        ConfigValue::Seq(vec![
            ConfigValue::Bool(true),
            ConfigValue::String("value".into())
        ])
    );
    assert_eq!(map[&ConfigValue::I64(12)], ConfigValue::F64(1.5));
}

#[test]
fn sparse_integer_keys_are_not_a_list() {
    let value = PhpValue::Array(vec![(PhpArrayKey::Int(2), PhpValue::Int(7))]);
    assert!(matches!(config_value(value), ConfigValue::Map(_)));
    assert_eq!(
        config_value(PhpValue::Array(Vec::new())),
        ConfigValue::Seq(Vec::new())
    );
}
