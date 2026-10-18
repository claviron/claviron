use serde::{Deserialize, Deserializer};

use super::{one_or_many, opt_one_or_many};

#[derive(Debug, Deserialize, PartialEq)]
struct Item {
    #[serde(deserialize_with = "validated_name")]
    name: String,
}

fn validated_name<'de, D: Deserializer<'de>>(
    deserializer: D
) -> Result<String, D::Error> {
    let name = String::deserialize(deserializer)?;
    if name == "rejected" {
        return Err(serde::de::Error::custom("item name is not allowed"));
    }
    Ok(name)
}

#[derive(Debug, Deserialize)]
struct OptionalItems {
    #[serde(default, deserialize_with = "opt_one_or_many")]
    items: Option<Vec<Item>>,
}

#[derive(Debug, Deserialize)]
struct RequiredItems {
    #[serde(deserialize_with = "one_or_many")]
    items: Vec<Item>,
}

#[test]
fn optional_single_and_list_preserve_item_errors() {
    for source in ["{}", "items: null"] {
        assert!(
            serde_yaml::from_str::<OptionalItems>(source)
                .unwrap()
                .items
                .is_none()
        );
    }
    assert!(
        serde_yaml::from_str::<OptionalItems>("items: []")
            .unwrap()
            .items
            .unwrap()
            .is_empty()
    );
    for source in ["items: {name: accepted}", "items: [{name: accepted}]"] {
        let items = serde_yaml::from_str::<OptionalItems>(source)
            .unwrap()
            .items
            .unwrap();
        assert_eq!(items, vec![Item { name: "accepted".into() }]);
    }
    for source in [
        "items: {name: rejected}",
        "items: [{name: accepted}, {name: rejected}]",
    ] {
        let error = serde_yaml::from_str::<OptionalItems>(source)
            .unwrap_err()
            .to_string();
        assert!(error.contains("item name is not allowed"), "{error}");
    }
}

#[test]
fn required_single_and_list_preserve_item_errors() {
    for source in ["items: {name: accepted}", "items: [{name: accepted}]"] {
        let items =
            serde_yaml::from_str::<RequiredItems>(source).unwrap().items;
        assert_eq!(items, vec![Item { name: "accepted".into() }]);
    }
    for source in ["items: {name: rejected}", "items: [{name: rejected}]"] {
        let error = serde_yaml::from_str::<RequiredItems>(source)
            .unwrap_err()
            .to_string();
        assert!(error.contains("item name is not allowed"), "{error}");
    }
}
