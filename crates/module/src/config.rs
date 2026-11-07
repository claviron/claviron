//! `modules.yaml`: which libraries to load and how what they register runs.
//!
//! Nothing here has a default. Everything Claviron enforces is declared, so an
//! application or service the libraries registered and this file does not name
//! stops boot rather than running on a value nobody chose.

use std::collections::BTreeMap;
use std::num::NonZeroU16;
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
