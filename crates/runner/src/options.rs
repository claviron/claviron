//! What every runner's launch says alike: its name, the doors it answers on,
//! whose forwarding headers it believes, the request policy, static mounts,
//! source watching and the workers.
//!
//! A runner keeps its own file (`gufi.yaml`, `lumenize.yaml`) and its own
//! keys beside these — a virtual environment, a PHP configuration directory —
//! and the file stays flat: [`split`] hands these keys to [`RunnerOptions`]
//! and the rest to the runner's own section, each refusing a key it does not
//! know, so a mistyped key is an error rather than a silent default.
//!
//! Everything here serializes, because a master hands its resolved launch to
//! the workers it starts. What Claviron's own types only read — the request
//! policy, a static mount — travels as it was written and is parsed where it
//! is used.

#[cfg(any(feature = "files", feature = "watch", feature = "tls"))]
use std::path::PathBuf;

use claviron_http::config::ListenerOverrides;
use claviron_http::policy::{HttpPolicy, HttpPolicyOverride};
use claviron_http::trust::Network;
use serde::{Deserialize, Serialize};

/// What a runner does when the edge master announces its own shutdown.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum EdgeShutdownPolicy {
    #[default]
    Continue,
    Shutdown,
}

/// The shared part of a runner's launch. Every field is optional; what an
/// unset one means is said on it, and nothing is invented for it.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(feature = "cli", derive(clap::Args))]
pub struct RunnerOptions {
    /// What this application is called. The edge routes to it by this name.
    #[cfg_attr(feature = "cli", arg(short, long))]
    pub name: Option<String>,

    /// Framework labels; omission discovers them, an empty list opts out.
    /// Descriptive only, never a routing or execution selector.
    #[serde(
        alias = "framework",
        deserialize_with = "claviron_helpers::de::opt_one_or_many"
    )]
    #[cfg_attr(feature = "cli", arg(long = "framework", action = clap::ArgAction::Append))]
    pub frameworks: Option<Vec<String>>,

    /// Symbolic admin-client icon code; unknown codes use its runtime default.
    #[cfg_attr(feature = "cli", arg(long))]
    pub icon: Option<String>,

    /// The environment selecting `.env.<environment>`. Unstated, `local`.
    /// Typed only: a launch script states it, a file does not.
    #[serde(skip)]
    #[cfg_attr(feature = "cli", arg(long, conflicts_with = "production"))]
    pub environment: Option<String>,

    /// Run in production. Typed only: a service manager passes it, and no
    /// file can.
    #[serde(skip)]
    #[cfg_attr(feature = "cli", arg(long))]
    pub production: bool,

    /// The Orbit namespace, independent of the application name. Unwritten,
    /// use the application name if it is a valid fleet name.
    #[cfg_attr(
        feature = "cli",
        arg(long = "fleet", visible_alias = "fleet-name")
    )]
    pub fleet_name: Option<String>,

    /// Answer through `fleet_name`, advertising `name` inside that namespace.
    #[cfg_attr(feature = "cli", arg(long))]
    pub orbit: bool,

    /// React to the edge master's ShutdownInitiated notice. A disconnected
    /// Link alone never triggers this policy. Written in the runner file.
    #[cfg_attr(feature = "cli", arg(skip))]
    pub edge_shutdown_policy: EdgeShutdownPolicy,

    /// Named listeners, with the same declarations as `http.yaml`.
    #[cfg_attr(feature = "cli", arg(skip))]
    pub listeners: Option<serde_yaml::Value>,

    /// Shared transport defaults under `defaults.listener`. Named listeners
    /// override them; Orbit also uses their execution selection.
    #[cfg_attr(feature = "cli", arg(skip))]
    pub defaults: Option<serde_yaml::Value>,

    /// Peers whose forwarding headers to believe: an address or a CIDR,
    /// repeatable. Nobody, unless stated. Carried as the text it was written
    /// as, so encoding a launch and reading it back yields what was typed.
    #[cfg_attr(feature = "cli", arg(long = "trusted"))]
    #[serde(with = "networks")]
    pub trusted: Vec<Network>,

    /// Serve HTTPS on the TCP socket with this certificate chain.
    #[cfg(feature = "tls")]
    #[cfg_attr(feature = "cli", arg(long, requires = "tls_key"))]
    pub tls_cert: Option<PathBuf>,

    /// The private key for `--tls-cert`.
    #[cfg(feature = "tls")]
    #[cfg_attr(feature = "cli", arg(long, requires = "tls_cert"))]
    pub tls_key: Option<PathBuf>,

    /// Require a client certificate signed by this authority.
    #[cfg(feature = "tls")]
    #[cfg_attr(feature = "cli", arg(long, requires = "tls_cert"))]
    pub tls_client_ca: Option<PathBuf>,

    /// Exchanges taken from the Orbit lane and not yet answered. Unwritten,
    /// no bound: work waits in the lane, where the edge sees it.
    #[cfg_attr(feature = "cli", arg(long))]
    pub max_in_flight: Option<usize>,

    /// The request side of the policy (`max_body_size`, `body_read_timeout`,
    /// `queue_timeout`, …). Unwritten, nothing is bounded.
    /// Written in the file only; carried as written.
    #[cfg_attr(feature = "cli", arg(skip))]
    pub requests: Option<serde_yaml::Value>,

    /// The response side of the policy (`max_body_size`,
    /// `idle_timeout`, …). Unwritten, nothing is bounded.
    #[cfg_attr(feature = "cli", arg(skip))]
    pub responses: Option<serde_yaml::Value>,

    /// What stands in front of the application: redirects, headers, access
    /// rules, files and route scopes, as a host declares them on an edge. It
    /// applies to every door. Written in the file only; carried as written.
    #[cfg_attr(feature = "cli", arg(skip))]
    pub site: Option<serde_yaml::Value>,

    /// Static mounts served before the application, for a process no edge
    /// stands in front of. Written in the file only.
    #[cfg(feature = "files")]
    #[cfg_attr(feature = "cli", arg(skip))]
    pub files: Option<crate::files::FilesSource>,

    /// Probe public files before the application. Disabled unless requested.
    #[cfg(feature = "files")]
    #[cfg_attr(feature = "cli", arg(long))]
    pub try_files: bool,

    /// Directories under public to mount at their matching URL prefixes.
    /// With `files`, directories are relative to its configured root instead.
    #[cfg(feature = "files")]
    #[cfg_attr(feature = "cli", arg(long = "asset"))]
    pub assets: Vec<PathBuf>,

    /// Restart the application when its source changes. Development only.
    #[cfg(feature = "watch")]
    #[cfg_attr(feature = "cli", arg(long))]
    pub watch: bool,

    /// What to watch, relative to the application's root. Unwritten, the
    /// runner's own defaults.
    #[cfg(feature = "watch")]
    #[cfg_attr(feature = "cli", arg(long = "watch-path"))]
    pub watch_paths: Vec<PathBuf>,

    /// How many worker processes serve this application. Zero, or unwritten,
    /// is one standalone process; with workers the master serves nothing.
    #[cfg(feature = "supervisor")]
    #[cfg_attr(feature = "cli", arg(short, long))]
    pub workers: Option<u8>,

    /// How the worker fleet is supervised: rotation, recovery, heartbeat and
    /// the deadlines a restart runs on. Consulted only with workers, and every
    /// field has the supervisor's own value. Written in the file only.
    #[cfg(feature = "supervisor")]
    #[cfg_attr(feature = "cli", arg(skip))]
    pub supervisor: Option<claviron_supervisor::SupervisorConfig>,
}

impl RunnerOptions {
    /// The keys these options take in a runner's file; [`split`] hands them
    /// here and the rest to the runner.
    pub const KEYS: &'static [&'static str] = &[
        "name",
        "framework",
        "frameworks",
        "icon",
        "fleet_name",
        "orbit",
        "edge_shutdown_policy",
        "defaults",
        "listeners",
        "trusted",
        #[cfg(feature = "tls")]
        "tls_cert",
        #[cfg(feature = "tls")]
        "tls_key",
        #[cfg(feature = "tls")]
        "tls_client_ca",
        "max_in_flight",
        "requests",
        "responses",
        "site",
        #[cfg(feature = "files")]
        "files",
        #[cfg(feature = "files")]
        "try_files",
        #[cfg(feature = "files")]
        "assets",
        #[cfg(feature = "watch")]
        "watch",
        #[cfg(feature = "watch")]
        "watch_paths",
        #[cfg(feature = "supervisor")]
        "workers",
        #[cfg(feature = "supervisor")]
        "supervisor",
    ];

    /// These options over those written, field by field: the command line
    /// wins wherever it said anything. A flag can open a door the file left
    /// shut; closing one the file opened is an edit to the file, because an
    /// absent flag and a `false` one are the same argv.
    pub fn over(
        self,
        written: Self,
    ) -> Self {
        Self {
            name: self.name.or(written.name),
            frameworks: self.frameworks.or(written.frameworks),
            icon: self.icon.or(written.icon),
            // Typed only; a file never carries them.
            environment: self.environment,
            production: self.production,
            fleet_name: self.fleet_name.or(written.fleet_name),
            orbit: self.orbit || written.orbit,
            edge_shutdown_policy: if self.edge_shutdown_policy
                == EdgeShutdownPolicy::default()
            {
                written.edge_shutdown_policy
            } else {
                self.edge_shutdown_policy
            },
            listeners: self.listeners.or(written.listeners),
            defaults: self.defaults.or(written.defaults),
            // One decision: a command line that names any peer is the whole
            // list.
            trusted: if self.trusted.is_empty() {
                written.trusted
            } else {
                self.trusted
            },
            #[cfg(feature = "tls")]
            tls_cert: self.tls_cert.or(written.tls_cert),
            #[cfg(feature = "tls")]
            tls_key: self.tls_key.or(written.tls_key),
            #[cfg(feature = "tls")]
            tls_client_ca: self.tls_client_ca.or(written.tls_client_ca),
            max_in_flight: self.max_in_flight.or(written.max_in_flight),
            requests: self.requests.or(written.requests),
            responses: self.responses.or(written.responses),
            site: self.site.or(written.site),
            #[cfg(feature = "files")]
            files: self.files.or(written.files),
            #[cfg(feature = "files")]
            try_files: self.try_files || written.try_files,
            #[cfg(feature = "files")]
            assets: if self.assets.is_empty() {
                written.assets
            } else {
                self.assets
            },
            #[cfg(feature = "watch")]
            watch: self.watch || written.watch,
            #[cfg(feature = "watch")]
            watch_paths: if self.watch_paths.is_empty() {
                written.watch_paths
            } else {
                self.watch_paths
            },
            #[cfg(feature = "supervisor")]
            workers: self.workers.or(written.workers),
            #[cfg(feature = "supervisor")]
            supervisor: self.supervisor.or(written.supervisor),
        }
    }

    /// What cannot be served as written, said before anything starts.
    pub fn validate(&self) -> Result<(), String> {
        self.socket_listeners()?;
        self.site()?;
        if let Some(name) = &self.fleet_name {
            claviron_runtime::FleetName::try_new(name)
                .map_err(|error| format!("fleet_name: {error}"))?;
        }
        #[cfg(feature = "tls")]
        {
            if self.tls_cert.is_some() != self.tls_key.is_some() {
                return Err("tls_cert and tls_key are given together".into());
            }
            if self.tls_client_ca.is_some() && self.tls_cert.is_none() {
                return Err("tls_client_ca needs tls_cert and tls_key".into());
            }
            if self.tls_cert.is_some() && !self.names_a_socket() {
                return Err(
                    "TLS is served on a tcp socket, and none is named".into()
                );
            }
        }
        if self.max_in_flight == Some(0) {
            return Err(
                "max_in_flight is positive, or unwritten for no bound".into()
            );
        }
        self.policy()?;
        #[cfg(feature = "files")]
        self.file_site()?;
        Ok(())
    }

    /// The common listener defaults, including Orbit's execution selection.
    /// Request and reply policy belongs at the application root.
    pub fn listener_defaults(&self) -> Result<ListenerOverrides, String> {
        #[derive(Default, Deserialize)]
        #[serde(default, deny_unknown_fields)]
        struct Defaults {
            listener: ListenerOverrides,
        }

        let Some(written) = &self.defaults else {
            return Ok(ListenerOverrides::default());
        };
        let defaults: Defaults = serde_yaml::from_value(written.clone())
            .map_err(|error| format!("defaults: {error}"))?;
        let listener = defaults.listener;
        if listener.requests != Default::default()
            || listener.responses != Default::default()
        {
            return Err(
                "defaults.listener: requests and responses belong at the runner root"
                    .into(),
            );
        }
        Ok(listener)
    }

    /// The site as written. It declares its own files, so `files`, `try_files`
    /// and `assets` beside it would be two answers to one question.
    pub fn site(&self) -> Result<Option<claviron_pipeline::Site>, String> {
        let Some(written) = &self.site else { return Ok(None) };
        #[cfg(feature = "files")]
        if self.files.is_some() || self.try_files || !self.assets.is_empty() {
            return Err(
                "site declares its own files; leave out files, try_files and assets"
                    .into(),
            );
        }
        serde_yaml::from_value(written.clone())
            .map(Some)
            .map_err(|error| format!("site: {error}"))
    }

    /// One file site for both explicit policies and the asset shorthand.
    #[cfg(feature = "files")]
    pub fn file_site(&self) -> Result<Option<crate::files::Files>, String> {
        if self.files.is_none() && !self.try_files && self.assets.is_empty() {
            return Ok(None);
        }
        let files = self
            .files
            .as_ref()
            .map(|source| source.parse())
            .transpose()?
            .unwrap_or_else(crate::files::Files::public);
        files.with_assets(&self.assets, self.try_files).map(Some)
    }

    /// The request policy as written; unwritten sides bound nothing.
    pub fn policy(&self) -> Result<HttpPolicy, String> {
        fn parse<T: serde::de::DeserializeOwned + Default>(
            value: &Option<serde_yaml::Value>,
            what: &str,
        ) -> Result<T, String> {
            match value {
                Some(value) => serde_yaml::from_value(value.clone())
                    .map_err(|error| format!("{what}: {error}")),
                None => Ok(T::default()),
            }
        }
        let requests = parse(&self.requests, "requests")?;
        let responses = parse(&self.responses, "responses")?;
        Ok(HttpPolicy::unlimited()
            .narrow(&HttpPolicyOverride { requests, responses }))
    }

    /// Whether the launch names a socket, whichever build reads it.
    pub fn names_a_socket(&self) -> bool {
        self.listeners.as_ref().is_some_and(|value| {
            value.as_mapping().is_some_and(|value| !value.is_empty())
        })
    }

    /// Resolve through HTTP's single declaration API; no runner-only socket schema.
    pub fn socket_listeners(
        &self
    ) -> Result<
        Vec<std::sync::Arc<claviron_http::config::ListenerConfig>>,
        String,
    > {
        self.listener_defaults()?;
        let mut written = serde_yaml::Mapping::new();
        if let Some(listeners) = &self.listeners {
            written.insert("listeners".into(), listeners.clone());
        }
        if let Some(defaults) = &self.defaults {
            written.insert("defaults".into(), defaults.clone());
        }
        if let Some(declarations) =
            self.listeners.as_ref().and_then(serde_yaml::Value::as_mapping)
        {
            for declaration in
                declarations.values().filter_map(serde_yaml::Value::as_mapping)
            {
                if declaration.contains_key(serde_yaml::Value::from("requests"))
                    || declaration
                        .contains_key(serde_yaml::Value::from("responses"))
                {
                    return Err("listener requests/responses are application-wide; declare them at the runner root".into());
                }
            }
        }
        let mut declarations: claviron_http::config::HttpYaml =
            serde_yaml::from_value(written.into())
                .map_err(|error| format!("listeners: {error}"))?;
        // Application owns body policy once, for sockets and Orbit alike.
        declarations.defaults.listener.requests.max_body_size =
            Some(claviron_http::policy::Limit::Off);
        declarations.defaults.listener.requests.body_read_timeout =
            Some(claviron_http::policy::Limit::Off);
        let snapshot = declarations
            .resolve()
            .map_err(|error| format!("listeners: {error}"))?;
        claviron_http::config::ListenerCapabilities {
            tls: cfg!(feature = "tls"),
            h3: false,
        }
        .validate(&snapshot)
        .map_err(|error| error.to_string())?;
        if snapshot.listeners.values().any(|listener| {
            listener.proxy.as_ref().is_some_and(|proxy| {
                !proxy.headers.is_empty() || !proxy.trusted.is_empty()
            })
        }) {
            return Err("listener proxy policy is host-owned; runner forwarding trust is declared with trusted".into());
        }
        Ok(snapshot.listeners.into_values().collect())
    }

    /// The certificate for the TCP socket, if one is named.
    #[cfg(feature = "tls")]
    pub fn tls(&self) -> Option<crate::Tls> {
        let (certificate, key) =
            self.tls_cert.clone().zip(self.tls_key.clone())?;
        let tls = crate::Tls::new(certificate, key);
        Some(match &self.tls_client_ca {
            Some(ca) => tls.client_ca(ca.clone()),
            None => tls,
        })
    }
}

/// A runner's flat file, divided: the keys [`RunnerOptions`] takes, and the
/// rest for the runner's own section. Each side then refuses what it does not
/// know.
pub fn split(
    file: serde_yaml::Value
) -> Result<(serde_yaml::Value, serde_yaml::Value), String> {
    let mapping = match file {
        serde_yaml::Value::Mapping(mapping) => mapping,
        serde_yaml::Value::Null => serde_yaml::Mapping::new(),
        _ => return Err("the file is a mapping of keys".into()),
    };
    let (mut shared, mut own) =
        (serde_yaml::Mapping::new(), serde_yaml::Mapping::new());
    for (key, value) in mapping {
        let known =
            key.as_str().is_some_and(|key| RunnerOptions::KEYS.contains(&key));
        if known {
            shared.insert(key, value)
        } else {
            own.insert(key, value)
        };
    }
    Ok((serde_yaml::Value::Mapping(shared), serde_yaml::Value::Mapping(own)))
}

/// Both halves again, for a launch written back out: the runner's section
/// and these keys in one flat mapping.
pub fn merge(
    shared: serde_yaml::Value,
    own: serde_yaml::Value,
) -> serde_yaml::Value {
    let mut mapping = match own {
        serde_yaml::Value::Mapping(mapping) => mapping,
        _ => serde_yaml::Mapping::new(),
    };
    if let serde_yaml::Value::Mapping(shared) = shared {
        mapping.extend(shared);
    }
    serde_yaml::Value::Mapping(mapping)
}

/// `Network` is parsed from text and prints back to it; carrying the text is
/// both smaller and closer to what was meant than carrying the parsed halves.
mod networks {
    use claviron_http::trust::Network;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(
        networks: &[Network],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(networks.iter().map(ToString::to_string))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D
    ) -> Result<Vec<Network>, D::Error> {
        Vec::<String>::deserialize(deserializer)?
            .into_iter()
            .map(|text| text.parse().map_err(serde::de::Error::custom))
            .collect()
    }
}

#[cfg(test)]
#[path = "../tests/options.rs"]
mod tests;
