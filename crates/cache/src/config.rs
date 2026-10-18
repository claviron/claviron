use std::collections::HashMap;

use serde::Deserialize;
use serde::de::DeserializeOwned;

#[derive(Clone, Debug, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    #[serde(default)]
    pub default: String,
    #[serde(default)]
    pub stores: HashMap<String, CacheStoreConfig>,
}

impl CacheConfig {
    /// A document that composes no cache: what an absent `cache.yaml` says.
    pub fn is_empty(&self) -> bool {
        self.stores.is_empty() && self.default.trim().is_empty()
    }
}

#[derive(Clone, Debug, Deserialize, Default, PartialEq)]
pub struct CacheStoreConfig {
    #[serde(default, alias = "kind")]
    pub driver: String,

    #[serde(flatten)]
    pub options: HashMap<String, serde_yaml::Value>,
}

impl CacheStoreConfig {
    pub fn driver_key(&self) -> String {
        self.driver.trim().to_ascii_lowercase()
    }

    pub fn options_value(&self) -> serde_yaml::Value {
        let mut mapping = serde_yaml::Mapping::new();
        for (key, value) in &self.options {
            mapping
                .insert(serde_yaml::Value::String(key.clone()), value.clone());
        }
        serde_yaml::Value::Mapping(mapping)
    }

    pub fn deserialize_options<T>(&self) -> super::Result<T>
    where
        T: DeserializeOwned,
    {
        serde_yaml::from_value(self.options_value()).map_err(super::Error::from)
    }
}

#[derive(Clone, Deserialize)]
pub struct FileCacheStoreConfig {
    pub path: String,
    #[serde(default)]
    pub prefix: Option<String>,
}
