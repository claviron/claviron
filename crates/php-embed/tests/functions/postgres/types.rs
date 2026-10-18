use super::*;

#[test]
fn splits_timestamps_into_date_time_and_offset() {
    assert_eq!(
        split_datetime("2026-09-11 20:30:00"),
        ("2026-09-11", Some("20:30:00"), None)
    );
    assert_eq!(
        split_datetime("2026-09-11T20:30:00+03:00"),
        ("2026-09-11", Some("20:30:00"), Some("+03:00"))
    );
    assert_eq!(split_datetime("2026-09-11"), ("2026-09-11", None, None));
}

#[test]
fn parses_laravel_timestamp_format() {
    let value =
        as_timestamp(Parameter::String("2026-09-11 20:30:05".to_owned()))
            .unwrap();
    assert_eq!(format_date(value.date()), "2026-09-11");
    assert_eq!(format_time(value.time()), "20:30:05");
}

#[test]
fn parses_fractional_seconds_and_offsets() {
    let value = as_timestamptz(Parameter::String(
        "2026-09-11T20:30:05.25+03:00".to_owned(),
    ))
    .unwrap();
    assert_eq!(format_time(value.time()), "20:30:05.250000");
    assert_eq!(format_offset(value.offset()), "+03:00");
}

#[test]
fn renders_array_literals_like_the_text_protocol() {
    let items = vec![
        Some("a".to_owned()),
        None,
        Some("with space".to_owned()),
        Some(String::new()),
    ];
    assert_eq!(render_array(&items), "{a,NULL,\"with space\",\"\"}");
}

#[test]
fn coerces_strings_to_booleans_the_way_postgres_does() {
    assert!(as_bool(Parameter::String("t".to_owned())).unwrap());
    assert!(!as_bool(Parameter::String("FALSE".to_owned())).unwrap());
    assert!(as_bool(Parameter::String("maybe".to_owned())).is_err());
}

#[test]
fn detects_array_type_names_in_both_spellings() {
    assert_eq!(element_key("_INT4"), Some("INT4"));
    assert_eq!(element_key("INT4[]"), Some("INT4"));
    assert_eq!(element_key("INT4"), None);
}
