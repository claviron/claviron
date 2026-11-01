//! Response header edits, merged once for an effective host/path scope.

use std::collections::HashMap;
use claviron_http::config::host::HostHeadersConfig;
use http::{HeaderName, HeaderValue};
use super::*;

pub struct HeaderSetLayer {
    headers: Box<[(HeaderName, HeaderValue)]>,
}

impl HeaderSetLayer {
    pub fn new(headers: impl IntoIterator<Item = (HeaderName, HeaderValue)>) -> Option<Self> {
        let headers: Box<[_]> = headers.into_iter().collect();
        (!headers.is_empty()).then_some(Self { headers })
    }
}

impl<S> HostLayer<S> for HeaderSetLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        layer.after(move |head, _| {
            for (name, value) in &self.headers {
                head.headers.insert(name.clone(), value.clone());
            }
        });
        Ok(())
    }
}

pub struct HeaderClearLayer {
    headers: Box<[HeaderName]>,
}

impl HeaderClearLayer {
    pub fn new(headers: impl IntoIterator<Item = HeaderName>) -> Option<Self> {
        let headers: Box<[_]> = headers.into_iter().collect();
        (!headers.is_empty()).then_some(Self { headers })
    }
}

impl<S> HostLayer<S> for HeaderClearLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        layer.after(move |head, _| {
            for name in &self.headers { head.headers.remove(name); }
        });
        Ok(())
    }
}

/// Resolve inheritance before registration. No default Server value is added.
/// Response hooks unwind, so Set is registered first and Clear runs first.
pub fn prepare_headers<S>(
    builder: &mut HostPipelineBuilder<S>,
    defaults: Option<&HostHeadersConfig>,
    scope: &HostHeadersConfig,
) -> crate::Result<()> {
    let mut actions = HashMap::new();
    for headers in defaults.into_iter().chain(std::iter::once(scope)) {
        if let Some(server) = &headers.server {
            let value = HeaderValue::from_str(server)
                .map_err(|error| crate::Error::Invalid(format!("invalid Server header: {error}")))?;
            actions.insert(http::header::SERVER, Some(value));
        }
        for name in &headers.unset { actions.insert(name.clone(), None); }
        for (name, value) in &headers.set { actions.insert(name.clone(), Some(value.clone())); }
    }
    let mut set = Vec::new();
    let mut clear = Vec::new();
    for (name, value) in actions {
        match value {
            Some(value) => set.push((name, value)),
            None => clear.push(name),
        }
    }
    set.sort_unstable_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
    clear.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
    if let Some(layer) = HeaderSetLayer::new(set) { builder.layer(layer)?; }
    if let Some(layer) = HeaderClearLayer::new(clear) { builder.layer(layer)?; }
    Ok(())
}
