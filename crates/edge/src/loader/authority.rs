use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[cfg(any(all(feature = "tcp", feature = "tls"), feature = "quic"))]
use arc_swap::ArcSwap;
#[cfg(any(all(feature = "tcp", feature = "tls"), feature = "quic"))]
use claviron_http::ListenerId;
#[cfg(any(all(feature = "tcp", feature = "tls"), feature = "quic"))]
use claviron_http::config::ListenerConfig;
use claviron_runtime::AppState;
use claviron_tls::auth::ClientAuth;
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
            let Some(source) = record
                .config
                .tls
                .client_auth
                .as_ref()
                .and_then(ClientAuth::store_authority)
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
                record
                    .config
                    .tls
                    .client_auth
                    .as_ref()
                    .and_then(ClientAuth::store_authority)
            })
            .collect();
        self.0.retain(|source, _| selected.contains(source));
        table.authorities = (!self.0.is_empty()).then(|| Arc::new(self));
    }
}

/// The store CA each listener's own `tls.client_auth` selects. It is read
/// when the listener's declaration is prepared, which is asynchronous, and
/// kept for the synchronous host publications that follow, so a host change
/// never reads the store. A listener's next declaration replaces its entry.
#[cfg(any(all(feature = "tcp", feature = "tls"), feature = "quic"))]
#[derive(Default)]
pub(crate) struct ListenerAuthorities(
    ArcSwap<HashMap<ListenerId, (AuthorityEntry, Arc<CertAuthority>)>>,
);

#[cfg(any(all(feature = "tcp", feature = "tls"), feature = "quic"))]
impl ListenerAuthorities {
    /// Read the authority each of `listeners` selects, if any. A CA bundle,
    /// or no policy, needs nothing from the store.
    pub(crate) async fn prepare<'a>(
        &self,
        state: &AppState,
        listeners: impl IntoIterator<Item = &'a Arc<ListenerConfig>>,
    ) -> crate::Result<()> {
        let mut read = Vec::new();
        for listener in listeners {
            let Some(source) =
                listener.client_auth().and_then(ClientAuth::store_authority)
            else {
                continue;
            };
            let store = state
                .registry_ref()
                .resolve::<CertificateStore>()
                .ok_or_else(|| {
                    crate::Error::other(format!(
                        "listener '{}': tls.client_auth.authority requires CertificateStore",
                        listener.id
                    ))
                })?;
            let authority = store.trust(source).await.map_err(|error| {
                crate::Error::other(format!(
                    "listener '{}': {error}",
                    listener.id
                ))
            })?;
            read.push((
                listener.id.clone(),
                source.clone(),
                Arc::new(authority),
            ));
        }
        if read.is_empty() {
            return Ok(());
        }
        self.0.rcu(|current| {
            let mut next = HashMap::clone(current);
            for (id, source, authority) in &read {
                next.insert(id.clone(), (source.clone(), authority.clone()));
            }
            next
        });
        Ok(())
    }

    /// The authority prepared for `listener`, when it was prepared for this
    /// same selection.
    pub(crate) fn get(
        &self,
        listener: &ListenerId,
        source: &AuthorityEntry,
    ) -> Option<Arc<CertAuthority>> {
        self.0
            .load()
            .get(listener)
            .filter(|(prepared, _)| prepared == source)
            .map(|(_, authority)| authority.clone())
    }
}
