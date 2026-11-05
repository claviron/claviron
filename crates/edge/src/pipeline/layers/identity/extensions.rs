//! Export only runtime values; no configuration or body is retained.
//!
//! Three values are exported: the policy, the connection and the host name
//! the request was accepted for. The canonical host key stays inside host
//! selection: it is how the edge finds a host's configuration, not a fact a
//! backend reads. Three entries also fit a request's extension table before
//! it has to grow.

use claviron_http::policy::HttpPolicy;
use http::Extensions;

use crate::pipeline::host::HostContext;

/// Keep an existing extension box when replacing a value.
fn set_context<T: Clone + Send + Sync + 'static>(
    extensions: &mut Extensions,
    value: T,
) {
    let mut incoming = Some(value);
    let slot = extensions.get_or_insert_with(|| {
        incoming.take().expect("extension initializer runs once")
    });
    if let Some(value) = incoming {
        drop(std::mem::replace(slot, value));
    }
}

pub(super) fn inject(
    extensions: &mut Extensions,
    policy: HttpPolicy,
    context: Option<HostContext<'_>>,
) {
    set_context(extensions, policy);
    if let Some(context) = context {
        set_context(extensions, context.connection.clone());
        if let Some((hostname, _)) = context.host {
            set_context(extensions, hostname.clone());
        }
    }
}

#[cfg(test)]
#[path = "../../../../tests/pipeline/layers/identity/extensions.rs"]
mod tests;
