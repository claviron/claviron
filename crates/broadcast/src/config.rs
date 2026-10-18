use std::collections::{BTreeMap, HashSet};

use claviron_pusher::{PusherAppId, PusherAppKey, PusherAppSecret};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BroadcastConfig {
    applications: BTreeMap<String, BroadcastApplicationConfig>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BroadcastApplicationConfig {
    pub id: PusherAppId,
    pub key: PusherAppKey,
    #[serde(default)]
    pub secret: Option<PusherAppSecret>,
    #[serde(default)]
    pub client_events: bool,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum BroadcastConfigError {
    #[error("Broadcast application name cannot be empty")]
    EmptyApplicationName,
    #[error("duplicate Pusher app id '{0}'")]
    DuplicateAppId(String),
    #[error("duplicate Pusher app key '{0}'")]
    DuplicateAppKey(String),
}

impl BroadcastConfig {
    pub fn validate(self) -> Result<Self, BroadcastConfigError> {
        let mut ids = HashSet::with_capacity(self.applications.len());
        let mut keys = HashSet::with_capacity(self.applications.len());
        for (name, application) in &self.applications {
            if name.trim().is_empty() {
                return Err(BroadcastConfigError::EmptyApplicationName);
            }
            if !ids.insert(application.id.as_str()) {
                return Err(BroadcastConfigError::DuplicateAppId(
                    application.id.to_string(),
                ));
            }
            if !keys.insert(application.key.as_str()) {
                return Err(BroadcastConfigError::DuplicateAppKey(
                    application.key.to_string(),
                ));
            }
        }
        Ok(self)
    }

    pub fn applications(
        &self
    ) -> &BTreeMap<String, BroadcastApplicationConfig> {
        &self.applications
    }

    pub fn application_by_key(
        &self,
        key: &str,
    ) -> Option<(&str, &BroadcastApplicationConfig)> {
        self.applications
            .iter()
            .find(|(_, application)| application.key.as_str() == key)
            .map(|(name, application)| (name.as_str(), application))
    }

    pub fn application_by_id(
        &self,
        id: &str,
    ) -> Option<(&str, &BroadcastApplicationConfig)> {
        self.applications
            .iter()
            .find(|(_, application)| application.id.as_str() == id)
            .map(|(name, application)| (name.as_str(), application))
    }
}
