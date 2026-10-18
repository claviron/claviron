//! A runner's launch and the registrations every runner makes alike, as
//! pieces a runner calls in its own order. The flow — what is registered when,
//! and what a launch may ask — is the runner's; these are the mechanism.
//!
//! A worker is started with no command line, so the launch its master
//! resolved travels to it: [`Launch::prepare`] writes it and
//! [`Launch::inherited`] reads it back in a worker before anything is
//! registered from it.

use std::path::Path;
use std::sync::Arc;

use claviron_http::trust::Trust;
use claviron_runtime::{AppState, registry};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::options::{RunnerOptions, merge, split};
use crate::routing::Routing;
use crate::{OrbitIngress, SocketIngress};

/// A runner's launch: the options every runner shares, and the runner's own
/// keys beside them in one flat file.
#[derive(Clone, Debug, Default)]
pub struct Launch<O> {
    pub runner: RunnerOptions,
    pub own: O,
}

impl<O: Serialize> Serialize for Launch<O> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        use serde::ser::Error;
        let shared =
            serde_yaml::to_value(&self.runner).map_err(S::Error::custom)?;
        let own = serde_yaml::to_value(&self.own).map_err(S::Error::custom)?;
        merge(shared, own).serialize(serializer)
    }
}

impl<'de, O: DeserializeOwned> Deserialize<'de> for Launch<O> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        use serde::de::Error;
        let file = serde_yaml::Value::deserialize(deserializer)?;
        let (shared, own) = split(file).map_err(D::Error::custom)?;
        let runner =
            serde_yaml::from_value(shared).map_err(D::Error::custom)?;
        // A key neither side knows lands here; the list of what is known is
        // both halves, or the message would point away from the fix.
        let own = serde_yaml::from_value(own).map_err(|error| {
            D::Error::custom(format!(
                "{error}, or one of the runner's: {}",
                RunnerOptions::KEYS.join(", ")
            ))
        })?;
        Ok(Self { runner, own })
    }
}

/// A launch ready to start: the name, the workers, and the envelope a worker
/// reads back.
pub struct Prepared {
    pub name: String,
    pub fleet_name: claviron_runtime::FleetName,
    pub workers: u8,
    pub envelope: Vec<u8>,
}

impl<O: Serialize + DeserializeOwned> Launch<O> {
    /// Name the launch, check it, and write what a worker inherits:
    /// everything it serves, the name included, and not what only the master
    /// does — it starts no workers and watches nothing.
    pub fn prepare(
        &mut self,
        name: String,
    ) -> Result<Prepared, String> {
        self.runner.name = Some(name.clone());
        self.runner.validate()?;
        let fleet_name = claviron_runtime::FleetName::try_new(
            self.runner.fleet_name.as_deref().unwrap_or(&name),
        )
        .map_err(|error| {
            format!("fleet_name: {error}; set fleet_name separately from name")
        })?;
        self.runner.fleet_name = Some(fleet_name.to_string());
        #[cfg(feature = "supervisor")]
        let workers = self.runner.workers.unwrap_or(0);
        #[cfg(not(feature = "supervisor"))]
        let workers = 0;
        #[cfg_attr(
            not(any(feature = "supervisor", feature = "watch")),
            allow(unused_mut)
        )]
        let mut inherited = self.clone_via_serde()?;
        #[cfg(feature = "supervisor")]
        {
            inherited.runner.workers = None;
        }
        #[cfg(feature = "watch")]
        {
            inherited.runner.watch = false;
            inherited.runner.watch_paths.clear();
        }
        let envelope = serde_json::to_vec(&inherited)
            .map_err(|error| error.to_string())?;
        Ok(Prepared { name, fleet_name, workers, envelope })
    }

    fn clone_via_serde(&self) -> Result<Self, String> {
        let text =
            serde_json::to_vec(self).map_err(|error| error.to_string())?;
        serde_json::from_slice(&text).map_err(|error| error.to_string())
    }

    /// The launch a worker was handed, or this one where it was resolved.
    /// Read first, before anything is registered from it.
    pub fn inherited(
        self,
        state: &AppState,
    ) -> registry::Result<Self> {
        if !state.is_worker() {
            return Ok(self);
        }
        match state
            .registry_ref()
            .resolve::<claviron_runtime::LaunchBlob>()
            .filter(|blob| !blob.is_empty())
        {
            Some(blob) => {
                serde_json::from_slice(blob.as_bytes()).map_err(|error| {
                    registry::Error::msg(format!("launch envelope: {error}"))
                })
            }
            None => Ok(self),
        }
    }
}

/// What stands in front of the owner: the launch's request policy and bound
/// on requests admitted at once, and its static mounts anchored at `root`.
/// Registered before the application's own `configure`, which composes in
/// front of it.
pub fn register_routing(
    state: &AppState,
    runner: &RunnerOptions,
    #[cfg_attr(not(feature = "files"), allow(unused_variables))] root: &Path,
) -> registry::Result<()> {
    let policy = runner.policy().map_err(registry::Error::msg)?;
    let routing = Routing::new(policy, runner.max_concurrent_requests);
    #[cfg(feature = "files")]
    let routing = routing.files(
        runner
            .file_site()
            .map_err(registry::Error::msg)?
            .map(|files| Arc::new(files.under(root))),
    );
    state.registry_ref().insert(Arc::new(routing))?;
    // Where `claviron::http::routing::register_routable` collects host-free
    // routables, before the application's registrations add theirs.
    #[cfg(feature = "axum")]
    claviron_http::routing::register(state)?;
    Ok(())
}

/// The peers the launch trusts, unless `configure` registered its own.
pub fn register_trust(
    state: &AppState,
    runner: &RunnerOptions,
) -> registry::Result<()> {
    if state.registry_ref().resolve::<Trust>().is_none() {
        state
            .registry_ref()
            .insert(Arc::new(Trust::new(runner.trusted.clone())))?;
    }
    Ok(())
}

/// Every door the launch asked for: its sockets, and its Orbit lane under
/// `runtime` (`php`, `asgi`), bounded by the launch's `max_in_flight` or else
/// by `default_in_flight`.
pub fn register_doors(
    state: &AppState,
    runner: &RunnerOptions,
    runtime: &'static str,
    default_in_flight: Option<usize>,
) -> registry::Result<()> {
    let binds = runner.binds();
    if !binds.is_empty() {
        let ingress = SocketIngress::new(binds, runner.protocols());
        #[cfg(feature = "tls")]
        let ingress = ingress.tls(runner.tls());
        state.registry_ref().register(Arc::new(ingress))?;
    }
    if runner.orbit {
        crate::management::register_link_application_management(state)?;
        if runner.edge_shutdown_policy
            == crate::options::EdgeShutdownPolicy::Shutdown
        {
            state.events().listen::<crate::orbit::EdgeShutdownInitiated, _>(
                |state, _notice| async move {
                    #[cfg(feature = "supervisor")]
                    claviron_supervisor::ServerManager::new(&state)
                        .shutdown()
                        .map_err(|error| {
                            registry::Error::msg(error.to_string())
                        })?;
                    #[cfg(not(feature = "supervisor"))]
                    state.initiate_shutdown();
                    Ok(())
                },
            )?;
        }
        let name = runner.name.clone().ok_or_else(|| {
            registry::Error::msg("the launch was prepared without a name")
        })?;
        state.registry_ref().register(Arc::new(OrbitIngress::new(
            name,
            runtime,
            runner.max_in_flight.or(default_in_flight),
        )))?;
    }
    Ok(())
}
