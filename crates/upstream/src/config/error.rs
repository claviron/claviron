//! Owns profile and app configuration validation failures.

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("upstream profile `{profile}` resolve is invalid: {detail}")]
    InvalidResolve { profile: String, detail: String },
    #[error("upstream profile `{profile}` TLS is invalid: {detail}")]
    InvalidTls { profile: String, detail: String },
    #[error("upstream capacity `{field}` is invalid: {detail}")]
    InvalidCapacity { field: &'static str, detail: &'static str },
    #[error(
        "upstream profile `{profile}` balance `{field}` is invalid: {detail}"
    )]
    InvalidBalance {
        profile: String,
        field: &'static str,
        detail: &'static str,
    },
    #[error(
        "upstream profile `{profile}` health `{field}` is invalid: {detail}"
    )]
    InvalidHealth { profile: String, field: &'static str, detail: &'static str },
    #[error("upstream profile `{profile}` must define at least one origin")]
    NoOrigins { profile: String },
    #[error(
        "upstream profile `{profile}` must declare connection.connect_timeout: a duration, or off"
    )]
    UndeclaredConnectTimeout { profile: String },
    #[error(
        "upstream profile `{profile}` uses unsupported scheme `{scheme}`; supported schemes are tcp and unix"
    )]
    UnsupportedScheme { profile: String, scheme: String },
    #[error(
        "upstream profile `{profile}` origin `{origin}` is invalid: {detail}"
    )]
    InvalidOrigin { profile: String, origin: String, detail: String },
    #[error(
        "upstream profile `{profile}` origin `{origin}` requires cold desired resolution"
    )]
    DnsOriginRejected { profile: String, origin: String },
    #[error(
        "upstream profile `{profile}` origin `{origin}` must resolve to a socket address with a port"
    )]
    MissingPort { profile: String, origin: String },
    #[error(
        "upstream profile `{profile}` UDS origin `{origin}` requires explicit authority"
    )]
    MissingAuthority { profile: String, origin: String },
    #[error("capacity.max_live must be greater than zero")]
    ZeroMaxLive,
    #[error(
        "upstream app `{app}` route {route} must select an upstream profile"
    )]
    AppRouteMissingProfile { app: String, route: usize },
    #[error(
        "upstream app `{app}` route {route} references unknown upstream profile `{profile}`"
    )]
    AppRouteUnknownProfile { app: String, route: usize, profile: String },
    #[error("upstream app `{app}` must define at least one route")]
    AppHasNoRoutes { app: String },
    #[error(
        "upstream app `{app}` route {route} must define at most one selector: exact, path, paths, or regex"
    )]
    AppRouteSelectorCount { app: String, route: usize },
    #[error(
        "upstream app `{app}` route {route} selector `{selector}` must start with `/`"
    )]
    AppRouteSelectorMustStartWithSlash {
        app: String,
        route: usize,
        selector: String,
    },
    #[error("upstream app `{app}` route {route} selector cannot be empty")]
    AppRouteEmptySelector { app: String, route: usize },
    #[error(
        "upstream app `{app}` route {route} regex `{pattern}` is invalid: {detail}"
    )]
    AppRouteInvalidRegex {
        app: String,
        route: usize,
        pattern: String,
        detail: String,
    },
    #[error(
        "upstream app `{app}` has more than one unconditional root fallback"
    )]
    AppDuplicateRootFallback { app: String },
}
