/// Accepted transport scheme. Server transports publish `ConnCtx`; standalone
/// adapters may instead supply a typed `Scheme`. Headers and raw request URIs
/// never establish trusted transport facts.
pub fn request_scheme(
    extensions: &http::Extensions
) -> Option<http::uri::Scheme> {
    #[cfg(feature = "server")]
    if let Some(ctx) = extensions.get::<std::sync::Arc<super::ConnCtx>>() {
        return Some(ctx.scheme());
    }

    extensions
        .get::<http::uri::Scheme>()
        .filter(|scheme| {
            **scheme == http::uri::Scheme::HTTP
                || **scheme == http::uri::Scheme::HTTPS
        })
        .cloned()
}

#[cfg(test)]
#[path = "../../tests/ext/scheme.rs"]
mod tests;
