//! `modules.yaml`: which libraries to load and how what they register runs.
//!
//! Nothing here has a default. Everything Claviron enforces is declared, so an
//! application or service the libraries registered and this file does not name
//! stops boot rather than running on a value nobody chose.

use std::collections::BTreeMap;
use std::num::{NonZeroU16, NonZeroUsize};
use std::path::PathBuf;

use continuo::fleet::{Dispatch, DispatchPolicy, InstancePlacement, Scope};
use serde::Deserialize;

pub const CONFIG_FILE: &str = "modules.yaml";

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ModulesConfig {
    /// Loaded in this order, finalized in reverse. A relative path resolves
    /// against the process's working directory.
    pub modules: Vec<PathBuf>,
    #[serde(default)]
    pub applications: BTreeMap<String, ApplicationConfig>,
    #[serde(default)]
    pub services: BTreeMap<String, ServiceConfig>,
    /// The threads operations and event subscribers run on. Required when a
    /// library registers either.
    #[serde(default)]
    pub handlers: Option<HandlersConfig>,
    /// Required when a library subscribes to anything.
    #[serde(default)]
    pub events: Option<EventsConfig>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HandlersConfig {
    pub instances: NonZeroU16,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EventsConfig {
    /// Events one subscription may fall behind before it loses them.
    pub capacity: NonZeroUsize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationConfig {
    /// OS threads running this application's handlers, in each serving process:
    /// how many run at once.
    pub instances: NonZeroU16,
    /// Requests that may wait for an instance. Beyond it, refused with 503.
    pub max_waiting: usize,
    /// Management metadata.
    #[serde(default)]
    pub frameworks: Vec<String>,
    #[serde(default)]
    pub icon: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    pub scope: Scope,
    pub dispatch: Dispatch,
    #[serde(default)]
    pub dispatch_policy: Option<DispatchPolicy>,
}

impl ServiceConfig {
    pub(crate) fn placement(&self) -> InstancePlacement {
        InstancePlacement {
            scope: self.scope,
            dispatch: self.dispatch,
            policy: self.dispatch_policy,
        }
    }
}
