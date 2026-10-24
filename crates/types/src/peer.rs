use std::sync::Arc;

/// Certificate facts supplied by an accepting transport after TLS validation.
/// Parsing these facts does not authenticate a peer. Admission must first check
/// the transport's verification result and configured trust policy.
///
/// URI SANs are opaque identity candidates: the application selects its namespace
/// and handles missing or ambiguous identities. No URI, CN or issuer DN grants
/// access by itself. `issuer_dn` is a certificate name, not a trust-anchor ID.
/// The host owns trust-domain selection and binding to application subjects.
#[derive(Clone, PartialEq, Eq)]
pub struct PeerIdentity {
    /// Exact public leaf DER, available for host-owned credential pinning.
    pub certificate: Arc<[u8]>,
    pub subject_dn: String,
    pub issuer_dn: String,
    pub common_names: Vec<String>,
    pub subject_uris: Vec<String>,
}

impl std::fmt::Debug for PeerIdentity {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("PeerIdentity")
            .field("subject_dn", &self.subject_dn)
            .field("issuer_dn", &self.issuer_dn)
            .field("common_names", &self.common_names)
            .field("subject_uris", &self.subject_uris)
            .finish_non_exhaustive()
    }
}
