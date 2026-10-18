use super::*;

use congif::{ConfigFile, Configurable, async_trait};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, PartialEq)]
struct Settings {
    enabled: bool,
    count: u32,
    optional: Option<String>,
    list: Vec<String>,
    secret: String,
}

struct Owner(std::path::PathBuf);

#[async_trait]
impl Configurable for Owner {
    type Config = Settings;
    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.0)
    }
    fn absent_source(&self) -> Option<Settings> {
        Some(Settings {
            enabled: false,
            count: 0,
            optional: None,
            list: vec!["first".into()],
            secret: "not exported".into(),
        })
    }
    fn namespace(&self) -> String {
        "example".into()
    }
    async fn configure(
        &self,
        _: &Settings,
    ) -> congif::Result<()> {
        Ok(())
    }
    fn export_config(
        &self,
        value: &Settings,
    ) -> congif::Result<Option<ConfigValue>> {
        congif::export(&std::collections::BTreeMap::from([
            ("enabled", ConfigValue::Bool(value.enabled)),
            ("count", ConfigValue::U32(value.count)),
            ("optional", ConfigValue::Unit),
            (
                "list",
                ConfigValue::Seq(
                    value
                        .list
                        .iter()
                        .cloned()
                        .map(ConfigValue::String)
                        .collect(),
                ),
            ),
        ]))
    }
}

#[tokio::test]
async fn exported_lookup_preserves_null_false_zero_and_hides_unexported_fields()
{
    let directory = tempfile::tempdir().unwrap();
    let owner = Owner(directory.path().join("absent.yaml"));
    let reader = ConfigReader::new().with_builtin_parsers().unwrap();
    reader.prepare_owners(&[&owner]).await.unwrap();
    assert_eq!(
        lookup(&reader, "example.enabled").unwrap(),
        Some(ConfigValue::Bool(false))
    );
    assert_eq!(
        lookup(&reader, "example.count").unwrap(),
        Some(ConfigValue::U32(0))
    );
    assert_eq!(
        lookup(&reader, "example.optional").unwrap(),
        Some(ConfigValue::Unit)
    );
    assert_eq!(
        lookup(&reader, "example.list.0").unwrap(),
        Some(ConfigValue::String("first".into()))
    );
    assert_eq!(lookup(&reader, "example.secret").unwrap(), None);
    assert_eq!(lookup(&reader, "unknown.value").unwrap(), None);
    assert!(lookup(&reader, "example..count").is_err());
}

#[test]
fn integer_conversion_never_silently_loses_precision() {
    assert_eq!(php_integer(i64::MAX as u64).unwrap(), i64::MAX);
    assert!(php_integer(u64::MAX).is_err());
    assert!(array_key(ConfigValue::Bool(true)).is_err());
    assert!(matches!(
        array_key(ConfigValue::I64(-1)).unwrap(),
        ArrayKey::Long(-1)
    ));
}
