use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::PathBuf;
use std::str::FromStr;

use claviron_helpers::de::one_or_many;
use claviron_http::ext::PathPrefix;
use claviron_http::policy::{RequestPolicyOverride, ResponsePolicyOverride};
use claviron_runtime::registry::{Error, Result};
use serde::Deserialize;

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize, serde::Serialize,
)]
#[serde(rename_all = "lowercase")]
pub enum PhpRunner {
    #[cfg(not(any(feature = "php_fcgi", feature = "php_embed")))]
    #[default]
    #[serde(skip)]
    Unavailable,
    #[cfg(feature = "php_fcgi")]
    #[serde(alias = "fpm")]
    #[default]
    Fcgi,
    #[cfg(feature = "php_embed")]
    #[cfg_attr(not(feature = "php_fcgi"), default)]
    #[serde(alias = "embed")]
    Script,
    /// Named persistent HTTP application in the embedded runtime.
    #[cfg(feature = "php_embed")]
    Application,
}

impl PhpRunner {
    pub fn as_str(&self) -> &'static str {
        match self {
            #[cfg(not(any(feature = "php_fcgi", feature = "php_embed")))]
            PhpRunner::Unavailable => "unavailable",
            #[cfg(feature = "php_fcgi")]
            PhpRunner::Fcgi => "fcgi",
            #[cfg(feature = "php_embed")]
            PhpRunner::Script => "script",
            #[cfg(feature = "php_embed")]
            PhpRunner::Application => "application",
        }
    }
}

impl FromStr for PhpRunner {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            #[cfg(feature = "php_fcgi")]
            "fcgi" => Ok(PhpRunner::Fcgi),
            #[cfg(feature = "php_embed")]
            "script" => Ok(PhpRunner::Script),
            #[cfg(feature = "php_embed")]
            "application" => Ok(PhpRunner::Application),
            _ => Err(Error::msg(format!(
                "Invalid PHP runner configuration: {}",
                value
            ))),
        }
    }
}

impl TryFrom<&str> for PhpRunner {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

// Optional: TryFrom<String> delegates to &str
impl TryFrom<String> for PhpRunner {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        PhpRunner::try_from(value.as_str())
    }
}

/// PHP configuration - supports both single and multiple configs
/// Single: php: { fpm: web, ... }
/// Multiple: php: [{ path: "/api", fpm: api }, ...]
#[derive(Deserialize, Clone)]
#[serde(remote = "Self", deny_unknown_fields)]
pub struct PhpConfig {
    /// Optional path prefix check (e.g. `/api`). Requests below the
    /// prefix get this PhpConfig, others fall through to the next
    /// host-level entry.
    pub path: Option<PathPrefix>,
    #[serde(default)]
    pub runner: PhpRunner,
    /// Named pool from php.yaml applications. Only runner: application uses it;
    /// boot script, boot environment and capacity belong to that definition.
    /// Application routes do not accept route environment overrides.
    pub application: Option<String>,
    /// FPM pool names to use for this route
    #[serde(default, deserialize_with = "one_or_many")]
    pub fpm: Vec<String>,
    /// Per-location document directory override. Absolute paths (e.g.
    /// `/sites/api/public`) are used verbatim; relative paths (e.g.
    /// `other`) resolve against the host's docroot. The composed value
    /// becomes `PhpRuntime.root` — i.e. the FCGI `DOCUMENT_ROOT`.
    /// Field is named `dir` to avoid collision with the host-level
    /// `root:` directive — the two live at different semantic layers
    /// and shouldn't share a name.
    pub dir: Option<PathBuf>,
    /// Optional entry script override
    pub entry_script: Option<String>,
    pub entry_query: Option<String>,
    /// Optional script/FastCGI environment override; forbidden for application
    /// routes, whose boot environment belongs to the named application.
    /// A value is text, as an
    /// environment variable always is; YAML's numbers and booleans are taken
    /// as the text they are written as (`APP_ID: 2` is `"2"`).
    #[serde(
        default,
        alias = "env",
        alias = "environments",
        deserialize_with = "scalar_map"
    )]
    pub environment: HashMap<String, String>,
    /// Optional script/FastCGI environment source paths, retained without file
    /// I/O. Application routes must configure these on the named application.
    /// The execution owner resolves these separately from config. The YAML
    /// `environment:` block overrides values from this file (yaml
    /// inline = explicit operator intent, wins).
    ///
    /// Absolute paths are used verbatim; relative paths are resolved against
    /// the process working directory, like application and task env files.
    ///
    /// One path, or several: later files override earlier ones, so a
    /// per-host file listed after the shared one wins; inline
    /// `environment:` still wins over every file.
    #[serde(default, deserialize_with = "claviron_helpers::de::one_or_many")]
    pub env_file: Vec<PathBuf>,
    #[serde(default)]
    pub try_files: bool,
    /// Additional non-`.php` file extensions that may be routed through
    /// `entry_script` when `try_files` is enabled and no disk file wins.
    /// Extensionless paths are always eligible for entry-script routing;
    /// dotted asset-like paths must opt in here.
    #[serde(default = "default_entry_extensions")]
    pub entry_extensions: Vec<String>,
    /// fs::metadata-validate URI-derived `.php` candidates before
    /// PHP is invoked. Default `true` (proxy-layer 404 instead of
    /// SAPI fatal-error page exposing DOCUMENT_ROOT). `entry_script`
    /// is never validated — that's an operator pin.
    #[serde(default = "default_true")]
    pub validate_script: bool,
    /// This application's request policy, under its host's and its
    /// listener's: the same `requests:` fields, narrowing theirs
    /// ([`RequestPolicy::narrow`](claviron_http::policy::RequestPolicy::narrow)).
    /// It replaces `exec_mode`: the reply is always a stream, and how the
    /// request body reaches PHP is `requests.buffering`'s to say
    /// (`docs/REVIEW.md` §3e).
    #[serde(default)]
    pub requests: RequestPolicyOverride,
    /// This application's reply policy, narrowing its host's and its
    /// listener's the same way (`docs/REVIEW.md` §3e).
    #[serde(default)]
    pub responses: ResponsePolicyOverride,
    /// Index filenames searched when a request resolves to a directory.
    /// Vhost layer composes this from the host's `indexes:` (filtered to
    /// `.php` candidates) before handing PhpConfig to the proxy. Defaults
    /// to `["index.php"]`.
    #[serde(default = "default_php_indexes")]
    pub indexes: Vec<String>,
}

impl<'de> Deserialize<'de> for PhpConfig {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D
    ) -> std::result::Result<Self, D::Error> {
        // Remote derive supplies the field decoder; validate the complete route
        // before it can enter a host configuration snapshot.
        let config = Self::deserialize(deserializer)?;
        config.validate().map_err(serde::de::Error::custom)?;
        Ok(config)
    }
}

impl PhpConfig {
    pub fn validate(&self) -> Result<()> {
        #[cfg(feature = "php_embed")]
        if self.runner == PhpRunner::Application {
            if self.application.as_deref().is_none_or(|name| {
                name.trim().is_empty() || name.contains('\0')
            }) {
                return Err(Error::msg(
                    "runner: application requires a nonempty application name",
                ));
            }
            if !self.environment.is_empty() || !self.env_file.is_empty() {
                return Err(Error::msg(
                    "runner: application does not support site environment/env or env_file; configure the named application's boot environment in php.yaml applications",
                ));
            }
            if self.entry_script.is_some()
                || self.entry_query.is_some()
                || self.dir.is_some()
                || !self.fpm.is_empty()
                || self.try_files
                || !self.entry_extensions.is_empty()
            {
                return Err(Error::msg(
                    "application routes select a named application; configure its script, boot environment and instances in php.yaml applications — routes may declare path, application, requests and responses",
                ));
            }
            return Ok(());
        }
        if self.application.is_some() {
            return Err(Error::msg("application requires runner: application"));
        }
        Ok(())
    }
}

fn default_php_indexes() -> Vec<String> {
    vec!["index.php".to_string()]
}

fn default_entry_extensions() -> Vec<String> {
    Vec::new()
}

fn default_true() -> bool {
    true
}

/// A map whose values may be written as any YAML scalar and are kept as text.
fn scalar_map<'de, D>(
    deserializer: D
) -> std::result::Result<HashMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Scalar {
        Text(String),
        Integer(i64),
        Float(f64),
        Bool(bool),
    }
    let raw = HashMap::<String, Scalar>::deserialize(deserializer)?;
    Ok(raw
        .into_iter()
        .map(|(key, value)| {
            let text = match value {
                Scalar::Text(text) => text,
                Scalar::Integer(number) => number.to_string(),
                Scalar::Float(number) => number.to_string(),
                Scalar::Bool(flag) => flag.to_string(),
            };
            (key, text)
        })
        .collect())
}
