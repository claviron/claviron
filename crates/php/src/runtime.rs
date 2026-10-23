use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use claviron_http::ext::PathPrefix;
use claviron_http::policy::{
    HttpPolicy, HttpPolicyOverride, RequestPolicy, RequestPolicyOverride,
    ResponsePolicy, ResponsePolicyOverride,
};
use claviron_runtime::{AppState, RunMode};
use http::request::Parts;

use crate::config::{PhpConfig, PhpRunner};

/// Runtime-shaped PHP request configuration.
///
/// `PhpConfig` is the YAML-shaped, mutable, "user-facing" type used at config
/// load. `PhpRuntime` is the lean, immutable, `Arc`-wrapped type consumed
/// directly by the hot path of each request. Composition (root/index
/// resolution from the host, FPM pool list normalization, environment Arc
/// wrapping) happens **once per host at boot** — the resulting
/// `Arc<PhpRuntime>` is pinned to the per-host Axum router's `Extension` layer,
/// so request handling only pays the cost of an `Extension::extract` (O(1) HashMap lookup).
#[derive(Clone)]
pub struct PhpRuntime {
    /// Prepared disk mapping; request URI and script names remain HTTP paths.
    pub mapping: Option<claviron_http::config::mapping::RouteMapping>,
    /// Optional URL prefix gate; requests outside this prefix bypass PHP.
    /// Server-side `chroot`-style scope — `PathPrefix` validates the
    /// leading `/`, normalizes the trailing `/`, and provides
    /// segment-safe matching (`/api` matches `/api/x` but not `/apix`).
    pub path: Option<PathPrefix>,

    /// Explicit entry script override (e.g. `index.php`, `fundist.php`).
    pub entry_script: Option<String>,

    /// Optional rewritten query template for entry_script-driven routes.
    pub entry_query: Option<String>,

    /// Whether to first try files when entry script is used.
    pub try_files: bool,

    /// Non-`.php` extensions that may fall through to `entry_script`
    /// after `try_files` fails to find a disk file. Empty by default,
    /// so dotted asset URLs keep the historical static/404 path unless
    /// an operator explicitly marks the extension as route-owned.
    pub entry_extensions: Arc<[String]>,

    /// fs::metadata-validate URI-derived `.php` candidates before
    /// dispatching to PHP. Default `true`: missing scripts return 404
    /// at the proxy layer instead of letting PHP open the include
    /// path and emit a fatal error page (which leaks DOCUMENT_ROOT
    /// and the include_path layout to clients). `entry_script` is
    /// never validated — it's an operator pin and missing-file there
    /// is a configuration error, not a request error.
    /// Set `false` only if the runner is driven by something that
    /// already guarantees existence (atomic deploy + opcache
    /// `validate_timestamps=0`, immutable container image, …).
    pub validate_script: bool,

    /// `.php` index candidates for directory-style requests.
    /// Always populated; defaults to `["index.php"]` if not declared by the route or host.
    pub indexes: Arc<[String]>,

    /// Resolved document directory used as the FCGI `DOCUMENT_ROOT`.
    /// Composed at boot from `php_cfg.dir` (relative → host docroot
    /// joined; absolute → verbatim) or the host's docroot when no `dir:`
    /// override is set, then canonicalized when possible so runtime
    /// dispatch works on cleanly normalized paths.
    pub dir: PathBuf,

    /// FPM pool names this route may dispatch to.
    /// Future: pre-resolve to `Arc<[Arc<FpmClient>]>` once the composer can access PhpService.
    pub fpm: Arc<[String]>,

    /// FastCGI environment overrides applied on top of standard CGI vars.
    pub environment: Arc<HashMap<String, String>>,

    /// Environment source paths, retained for the execution owner to resolve.
    /// Runtime composition does not read files or merge their contents.
    pub env_file: Arc<[PathBuf]>,

    /// Execution-owner preparation, shared by requests on this route. The
    /// composer performs no environment I/O; dropping the route releases it.
    pub execution_environment:
        Arc<OnceLock<claviron_app::environment::ExecutionEnvironment>>,

    pub php_runner: PhpRunner,

    /// Named embedded application; no filesystem script dispatch in this model.
    pub application: Option<String>,

    /// This application's `requests:` block, pinned at boot; narrows the
    /// policy the edge hands over with each request ([`Self::requests`]).
    pub requests: RequestPolicyOverride,
    /// This application's `responses:` block, pinned at boot.
    pub responses: ResponsePolicyOverride,

    /// Server run mode (master / worker / standalone), pinned at boot and accessible in request handlers.
    pub server_run_mode: RunMode,

    /// Total worker slots visible to this process, pinned at boot.
    pub server_worker_count: u8,

    /// Server hostname, pinned at boot and available for logging or headers.
    pub server_hostname: String,

    /// Server software / application name, available at request-time for logging, headers, or context.
    pub server_software: String,
}

impl PhpRuntime {
    pub fn relative_path<'a>(
        &self,
        path: &'a str,
    ) -> Option<&'a str> {
        match &self.mapping {
            Some(mapping) => mapping.relative_path(path),
            None => Some(path.trim_start_matches('/')),
        }
    }

    /// A pinned entry is already relative to the document directory. Only
    /// URI-derived script names have the route's URL prefix removed.
    pub fn script_relative_path<'a>(
        &self,
        name: &'a str,
        pinned: bool,
    ) -> Option<&'a str> {
        if pinned {
            Some(name.trim_start_matches('/'))
        } else {
            self.relative_path(name)
        }
    }
    /// The policy this request runs under, both sides: the edge's (its
    /// listener's, narrowed by its host), carried in the request's
    /// extensions, narrowed once more by this route's `php:` block —
    /// transport, host, path. The policy follows the routing path only;
    /// php.yaml describes the engine's instances, not how a request is run.
    /// HTTP execution requires the caller to supply this extension, including
    /// direct callers without an edge. Executors reject a missing policy before
    /// using this composition helper.
    pub fn policy(
        &self,
        parts: &Parts,
    ) -> HttpPolicy {
        parts
            .extensions
            .get::<HttpPolicy>()
            .copied()
            .unwrap_or(HttpPolicy::unlimited())
            .narrow(&HttpPolicyOverride {
                requests: self.requests,
                responses: self.responses,
            })
    }

    /// The request side of [`Self::policy`].
    pub fn requests(
        &self,
        parts: &Parts,
    ) -> RequestPolicy {
        self.policy(parts).requests
    }

    /// The reply side of [`Self::policy`].
    pub fn responses(
        &self,
        parts: &Parts,
    ) -> ResponsePolicy {
        self.policy(parts).responses
    }

    /// Whether this runtime allows a non-`.php` request extension to be
    /// routed through the PHP `entry_script` after static `try_files` misses.
    pub fn allows_entry_extension(
        &self,
        ext: &str,
    ) -> bool {
        self.entry_script.is_some()
            && self
                .entry_extensions
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(ext))
    }

    /// Constructs a `PhpRuntime` from a `PhpConfig` template plus host-level defaults.
    ///
    /// # Parameters
    /// - `app_state`: Global application state providing context, registries, and host-level services.
    ///   Used during runtime construction to resolve dependencies (e.g., PhpFcgiService) or
    ///   populate runtime-wide shared resources.
    /// - `template`: The per-route `PhpConfig` template.
    /// - `default_root`: The host document root, used if the route has no `dir:` override.
    /// - `host_indexes`: The host’s index list; used if the route has no `indexes:` defined.
    ///
    /// # Returns
    /// A `PhpRuntime` containing all configuration values needed for request-time PHP execution.
    /// Values are precomputed and pinned to the host’s runtime, minimizing per-request overhead.
    pub fn from_config(
        state: &AppState,
        template: &PhpConfig,
        default_root: PathBuf,
        host_indexes: &[String],
    ) -> Self {
        let indexes: Arc<[String]> = if !template.indexes.is_empty() {
            Arc::from(template.indexes.clone().into_boxed_slice())
        } else {
            let php_indexes: Vec<String> = host_indexes
                .iter()
                .filter(|n| n.ends_with(".php"))
                .cloned()
                .collect();
            if php_indexes.is_empty() {
                Arc::from(vec!["index.php".to_string()].into_boxed_slice())
            } else {
                Arc::from(php_indexes.into_boxed_slice())
            }
        };

        // nginx-parity: relative `dir:` overrides resolve against the
        // host's docroot (so `dir: other` under `claviron.dev` (host
        // root `public`) becomes `public/other`). Absolute paths stay
        // literal. No override → host root verbatim.
        let dir = match template.dir.as_ref() {
            Some(d) if d.is_absolute() => d.clone(),
            Some(d) => default_root.join(d),
            None => default_root,
        };
        let dir = match dir.canonicalize() {
            Ok(real_dir) => real_dir,
            Err(err) => {
                tracing::warn!(
                    path = %dir.display(),
                    error = %err,
                    "failed to canonicalize php runtime dir; using non-canonical path"
                );
                dir
            }
        };

        Self {
            mapping: None,
            path: template.path.clone(),
            try_files: template.try_files,
            entry_extensions: Arc::from(
                template
                    .entry_extensions
                    .iter()
                    .filter_map(|ext| normalize_entry_extension(ext))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            validate_script: template.validate_script,
            entry_script: template.entry_script.clone(),
            entry_query: template.entry_query.clone(),
            indexes,
            dir,
            fpm: Arc::from(template.fpm.clone().into_boxed_slice()),
            environment: Arc::new(template.environment.clone()),
            env_file: Arc::from(template.env_file.clone()),
            execution_environment: Arc::new(OnceLock::new()),
            php_runner: template.runner,
            application: template.application.clone(),
            requests: template.requests,
            responses: template.responses,
            server_run_mode: state.run_mode(),
            server_worker_count: state.worker_count(),
            server_software: state.name().to_string(),
            server_hostname: state.hostname().to_string(),
        }
    }
}

fn normalize_entry_extension(ext: &str) -> Option<String> {
    let ext = ext.trim().trim_start_matches('.');
    if ext.is_empty() { None } else { Some(ext.to_ascii_lowercase()) }
}
