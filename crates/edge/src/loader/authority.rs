use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use claviron_runtime::AppState;
use claviron_tls::{CertAuthority, CertificateStore};
use claviron_types::ssl::certstore::AuthorityEntry;

use super::table::VhostTable;

#[derive(Clone, Default)]
pub(crate) struct PreparedAuthorities(
    HashMap<AuthorityEntry, Arc<CertAuthority>>,
);

impl PreparedAuthorities {
    pub(super) async fn load(
        table: &VhostTable,
        state: &AppState,
        previous: Option<&Self>,
    ) -> crate::Result<Self> {
        let mut authorities = HashMap::new();
        if state.is_master() {
            return Ok(Self(authorities));
        }
        for (host, record) in table.canonical_entries() {
            let auth = &record.config.tls.client_auth;
            let Some(source) =
                auth.authority.as_ref().filter(|_| auth.is_required())
            else {
                continue;
            };
            if authorities.contains_key(source) {
                continue;
            }
            let authority = if let Some(authority) =
                previous.and_then(|old| old.get(source))
            {
                authority.clone()
            } else {
                let store = state.registry_ref().resolve::<CertificateStore>()
                    .ok_or_else(|| crate::Error::other(format!(
                        "TLS host {host}: client_auth.authority requires CertificateStore"
                    )))?;
                Arc::new(store.trust(source).await.map_err(|error| {
                    crate::Error::other(format!("TLS host {host}: {error}"))
                })?)
            };
            authorities.insert(source.clone(), authority);
        }
        Ok(Self(authorities))
    }

    pub(crate) fn get(
        &self,
        source: &AuthorityEntry,
    ) -> Option<&Arc<CertAuthority>> {
        self.0.get(source)
    }

    pub(super) fn install(
        mut self,
        table: &mut VhostTable,
    ) {
        if let Some(current) = &table.authorities {
            // An alias or reload publication can complete while sources load.
            // Keep its material for a selector it already owns.
            self.0.extend(
                current
                    .0
                    .iter()
                    .map(|(source, ca)| (source.clone(), ca.clone())),
            );
        }
        let selected: HashSet<_> = table
            .canonical_entries()
            .filter_map(|(_, record)| {
                let auth = &record.config.tls.client_auth;
                auth.authority.as_ref().filter(|_| auth.is_required())
            })
            .collect();
        self.0.retain(|source, _| selected.contains(source));
        table.authorities = (!self.0.is_empty()).then(|| Arc::new(self));
    }
}
