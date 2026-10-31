use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use claviron_http::config::ProxyConfig;
use claviron_http::ext::{ConnCtx, HostKey, HostName, RealIpExt};
use claviron_http::resolvers::resolve_real_ip_from_headers;
use claviron_types::Host;
use http::{Extensions, Request};
use tracing::debug;

use crate::config::HostConfig;
use crate::loader::table::VhostTable;

/// Bundle of resolved vhost extensions to be injected into each request
/// This avoids parameter bloat when passing to h1/h2/h3 serve functions
#[derive(Clone)]
pub struct VhostCtx {
    /// Requested hostname: SNI for TLS, validated authority for cleartext.
    pub hostname: HostName,
    /// The canonical vhost key (resolved)
    pub vhost_key: HostKey,
    /// The vhost configuration
    pub vhost_config: Arc<HostConfig>,
    /// Routing prepared with this host publication, pinned for the connection.
    pub application: Option<crate::pipeline::HostApp>,
}

/// Resolve against the same host snapshot used to select TLS authentication.
pub fn resolve_sni_from_table(
    table: &VhostTable,
    server_name: Option<&str>,
) -> Result<VhostCtx, HostRejection> {
    let sni = server_name.ok_or(HostRejection::Missing)?;
    let host = Host::from_str(sni)
        .map_err(|error| HostRejection::Invalid(sni.to_owned(), error))?;
    match table.resolve_with_application(&host) {
        Some((record, application)) => {
            let key = &record.canonical;
            debug!("resolved SNI '{}' => canonical key '{:?}'", sni, key);
            Ok(VhostCtx {
                hostname: HostName(Arc::new(host)),
                vhost_key: HostKey(Arc::new(key.clone())),
                vhost_config: record.config.clone(),
                application: application.cloned(),
            })
        }
        None => Err(HostRejection::NotHosted(sni.to_string())),
    }
}

fn inject_vhost_ctx(
    extensions: &mut Extensions,
    vctx: &VhostCtx,
) {
    extensions.insert(vctx.hostname.clone());
    extensions.insert(vctx.vhost_key.clone());
    extensions.insert(vctx.vhost_config.clone());
}

/// Immutable, connection/host-owned values with their extension-table capacity
/// prepared once. Each request receives independent boxes, never a shared map.
/// Cloning the populated table avoids intermediate growth on empty requests;
/// typed values still use the ordinary `http::Extensions` allocations. Policy
/// and arrival time are published later by dispatch, not captured here.
pub(super) struct ContextExtensions(Extensions);

impl ContextExtensions {
    pub(super) fn new(
        ctx: &Arc<ConnCtx>,
        vctx: &VhostCtx,
    ) -> Self {
        let mut extensions = Extensions::new();
        if let Some(ip) = ctx.real_ip {
            extensions.insert(Arc::clone(ctx));
            extensions.insert(RealIpExt(ip));
        }
        inject_fixed_context(&mut extensions, ctx, vctx);
        Self(extensions)
    }

    /// Forwarded identity remains request-local. Native or caller-provided
    /// extensions use individual inserts so unrelated entries retain ownership.
    pub(super) fn inject<B>(
        &self,
        req: &mut Request<B>,
        ctx: &Arc<ConnCtx>,
        vctx: &VhostCtx,
        proxy: Option<&ProxyConfig>,
    ) -> Arc<ConnCtx> {
        let (conn_ctx, real_ip) = resolve_request_context(req, ctx, proxy);
        if req.extensions().is_empty() {
            *req.extensions_mut() = self.0.clone();
            if ctx.real_ip.is_none() {
                req.extensions_mut().insert(Arc::clone(&conn_ctx));
                req.extensions_mut().insert(RealIpExt(real_ip));
            }
        } else {
            // Preserve existing extensions and replacement behavior, without
            // allocating a temporary cloned table for this native path.
            req.extensions_mut().insert(Arc::clone(&conn_ctx));
            inject_fixed_context(req.extensions_mut(), ctx, vctx);
            req.extensions_mut().insert(RealIpExt(real_ip));
        }
        conn_ctx
    }
}

fn inject_fixed_context(
    extensions: &mut Extensions,
    ctx: &ConnCtx,
    vctx: &VhostCtx,
) {
    extensions.insert(ctx.listener.clone());
    extensions.insert(ctx.scheme());
    if let Some(protocol) = ctx.proto {
        extensions.insert(protocol);
    }
    inject_vhost_ctx(extensions, vctx);
}

/// A direct identity is fixed at accept; a trusted proxy can multiplex clients
/// over one connection, so its identity must be resolved for every request.
fn resolve_request_context<B>(
    req: &Request<B>,
    ctx: &Arc<ConnCtx>,
    proxy_cache: Option<&ProxyConfig>,
) -> (Arc<ConnCtx>, std::net::IpAddr) {
    match ctx.real_ip {
        // Direct connection: the identity was fixed when it was accepted, so
        // every request shares the connection's context as it is.
        Some(ip) => (Arc::clone(ctx), ip),
        // Behind a proxy one connection carries many clients: resolve this
        // request's real IP from its headers (not cached) into its own copy.
        None => {
            let real_ip = match proxy_cache {
                Some(proxy) => resolve_real_ip_from_headers(
                    req.headers(),
                    ctx.remote.ip(),
                    &proxy.trusted,
                    &proxy.headers,
                ),
                None => ctx.remote.ip(),
            };
            let mut conn_ctx = ctx.as_ref().clone();
            conn_ctx.real_ip = Some(real_ip);
            (Arc::new(conn_ctx), real_ip)
        }
    }
}

/// Result of SNI/vhost resolution at connection time
#[derive(Clone, Debug)]
pub enum HostRejection {
    /// No SNI provided (on HTTPS this should drop connection)
    Missing,

    /// SNI provided but invalid format
    Invalid(String, String),

    /// SNI provided but no matching vhost found
    NotHosted(String),
}

impl fmt::Display for HostRejection {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            HostRejection::Missing => {
                write!(f, "No SNI provided")
            }
            HostRejection::Invalid(sni, err) => {
                write!(f, "Invalid SNI format '{}' err: {}", sni, err)
            }
            HostRejection::NotHosted(sni) => {
                write!(f, "Unknown SNI '{}'", sni)
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/service/extensions.rs"]
mod tests;
