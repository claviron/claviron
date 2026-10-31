use std::sync::Arc;

use http::Request;
use regex::RegexSet;

/// Cloning `DenyService` is cheap (ARC clones) and safe across edge tasks.
/// Optimized global deny service backed by a RegexSet.
/// Patterns come from the resolved HTTP request defaults.
#[derive(Clone)]
pub struct DenyService {
    set: Arc<RegexSet>,
}

/// A request whose unchanged URI passed a specific immutable deny matcher.
/// The request is read-only until the receiving matcher verifies this proof.
/// Borrowing the matcher adds no allocation or reference-count operation.
pub struct DenyCheckedRequest<'a, B> {
    request: Request<B>,
    matcher: &'a RegexSet,
}

impl<B> DenyCheckedRequest<'_, B> {
    pub fn request(&self) -> &Request<B> {
        &self.request
    }

    /// Consume the immutable proof inside native ingress. No mutable head is
    /// exposed between this identity check and the receiving pipeline's deny gate.
    pub(crate) fn into_ingress(
        self,
        deny: &DenyService,
    ) -> (Request<B>, bool) {
        (self.request, std::ptr::eq(self.matcher, deny.set.as_ref()))
    }
}

impl DenyService {
    pub(crate) fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
    /// Retain the already-compiled request deny matcher for an ingress snapshot.
    pub fn from_set(set: &RegexSet) -> Self {
        Self { set: Arc::new(set.clone()) }
    }

    /// Build an empty deny service (matches no paths). Used as a fallback
    /// when the HTTP layer config service has not produced a snapshot yet.
    pub fn empty() -> Self {
        Self { set: Arc::new(RegexSet::empty()) }
    }

    /// Check if a path matches any deny pattern
    pub fn is_blocked(
        &self,
        path: &str,
    ) -> bool {
        !self.set.is_empty() && self.set.is_match(path)
    }

    /// Check before host selection while retaining proof for later admission.
    /// Failure returns the original request; the transport decides how to reject it.
    #[expect(
        clippy::result_large_err,
        reason = "Both outcomes move the request without allocating on the admission path"
    )]
    pub fn check_request<B>(
        &self,
        request: Request<B>,
    ) -> Result<DenyCheckedRequest<'_, B>, Request<B>> {
        if self.is_blocked(request.uri().path()) {
            Err(request)
        } else {
            Ok(DenyCheckedRequest { request, matcher: &self.set })
        }
    }

    /// Reuse an early check only for this exact matcher and unchanged request.
    /// A different policy must evaluate its own rules, never trust a stale proof.
    #[expect(
        clippy::result_large_err,
        reason = "Both outcomes move the request without allocating on the admission path"
    )]
    pub fn accept_checked<B>(
        &self,
        checked: DenyCheckedRequest<'_, B>,
    ) -> Result<Request<B>, Request<B>> {
        if std::ptr::eq(self.set.as_ref(), checked.matcher)
            || !self.is_blocked(checked.request.uri().path())
        {
            Ok(checked.request)
        } else {
            Err(checked.request)
        }
    }
}
