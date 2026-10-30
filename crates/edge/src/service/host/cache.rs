use super::*;
use crate::loader::table::VhostTable;
use crate::service::HostIdentityCache;

/// One connection's last immutable host policy. Application selection and live
/// ACL checks remain request-local; this cache owns no request, body or backend.
pub(crate) struct CleartextHostCache {
    identity: HostIdentityCache,
    current: Option<Arc<HostPolicy>>,
    connection: Arc<ConnCtx>,
    ingress: HostIngress,
    access: Arc<VhostResolver>,
    listener: HttpPolicy,
}

impl CleartextHostCache {
    pub(crate) fn new(
        connection: Arc<ConnCtx>,
        ingress: HostIngress,
        access: Arc<VhostResolver>,
        listener: HttpPolicy,
    ) -> Self {
        Self {
            identity: HostIdentityCache::default(),
            current: None,
            connection,
            ingress,
            access,
            listener,
        }
    }

    pub(crate) fn resolve(
        &mut self,
        authority: &RequestAuthority,
        table: &VhostTable,
    ) -> Option<VhostCtx> {
        // Always resolve against the current publication: an old entry must
        // never revive a removed host or keep an alias's previous target.
        let host = self.identity.resolve(authority, table);
        if host.is_none() {
            self.current = None;
        }
        host
    }

    pub(crate) fn bind(
        &mut self,
        app: HostApp,
        mut host: VhostCtx,
    ) -> HostService {
        // The chosen application moves into this request, not the policy cache.
        host.application = None;
        let unchanged = self.current.as_ref().is_some_and(|policy| {
            Arc::ptr_eq(&policy.host.hostname.0, &host.hostname.0)
                && Arc::ptr_eq(&policy.host.vhost_key.0, &host.vhost_key.0)
                && Arc::ptr_eq(&policy.host.vhost_config, &host.vhost_config)
        });
        if !unchanged {
            self.current = Some(Arc::new(HostPolicy::new(
                self.connection.clone(),
                host,
                self.ingress.clone(),
                self.access.clone(),
            )));
        }
        HostService {
            app,
            policy: self
                .current
                .as_ref()
                .expect("host policy is prepared")
                .clone(),
            listener: self.listener,
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/service/host/cache.rs"]
mod tests;
