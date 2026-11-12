use std::collections::HashMap;

use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::drivers::memory;

#[derive(Clone, Debug, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    #[serde(default)]
    pub default: String,
    #[serde(default)]
    pub stores: HashMap<String, CacheStoreConfig>,
}

impl CacheConfig {
    /// A document with no explicit store declarations or default.
    pub fn is_empty(&self) -> bool {
        self.stores.is_empty() && self.default.trim().is_empty()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct CacheStoreConfig {
    #[serde(default = "default_driver", alias = "kind")]
    pub driver: String,

    #[serde(flatten)]
    pub options: HashMap<String, serde_yaml::Value>,
}

fn default_driver() -> String {
    memory::NAME.to_owned()
}

impl Default for CacheStoreConfig {
    fn default() -> Self {
        Self { driver: default_driver(), options: HashMap::new() }
    }
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
