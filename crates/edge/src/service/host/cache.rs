use super::*;
use crate::loader::table::VhostTable;
use crate::service::{AcceptedAuthority, HostIdentityCache, RequestAuthority};
use claviron_http::ServiceResult;

/// One connection's last immutable host policy. Application selection remains
/// request-local; this cache owns no request, body or backend.
pub(crate) struct CleartextHostCache {
    identity: HostIdentityCache,
    current: Option<Arc<HostPolicy>>,
    connection: Arc<ConnCtx>,
    listener: HttpPolicy,
    admitted: Option<Admitted>,
}

/// The last request this connection admitted to a serving host: its authority
/// fields, the publication they were resolved against and the owner chosen.
struct Admitted {
    authority: AcceptedAuthority,
    table: VhostTable,
    service: HostService,
}

impl CleartextHostCache {
    pub(crate) fn connection(&self) -> &Arc<ConnCtx> {
        &self.connection
    }
    pub(crate) fn new(
        connection: Arc<ConnCtx>,
        listener: HttpPolicy,
    ) -> Self {
        Self {
            identity: HostIdentityCache::default(),
            current: None,
            connection,
            listener,
            admitted: None,
        }
    }

    /// Bind a request that repeats the last admitted authority fields to the
    /// same owner, without validating or resolving them again.
    ///
    /// The same fields against the same publication select the same host, so
    /// nothing is skipped that could answer differently. Another authority or
    /// another publication leaves the request unhandled for full admission,
    /// which is what keeps a connection from being pinned to its first host
    /// or to a replaced binding.
    pub(crate) fn repeated<B>(
        &self,
        request: Request<B>,
        table: &VhostTable,
    ) -> ServiceResult<PreparedRequest<B>, Request<B>> {
        match &self.admitted {
            Some(admitted)
                if admitted.table.same_publication(table)
                    && admitted.authority.repeats(&request) =>
            {
                ServiceResult::Handled(
                    admitted
                        .service
                        .clone()
                        .into_request(request, RequestArrival::Established),
                )
            }
            _ => ServiceResult::Unhandled(request),
        }
    }

    /// Remember a request full admission has just bound to `service`.
    pub(crate) fn admit<B>(
        &mut self,
        request: &Request<B>,
        table: &VhostTable,
        service: &HostService,
    ) {
        let mut authority = self
            .admitted
            .take()
            .map(|admitted| admitted.authority)
            .unwrap_or_default();
        authority.record(request);
        self.admitted = Some(Admitted {
            authority,
            table: table.clone(),
            service: service.clone(),
        });
    }

    pub(crate) fn resolve(
        &mut self,
        authority: &RequestAuthority,
        table: &VhostTable,
    ) -> Option<VhostCtx> {
        // Always resolve against the supplied publication: an old entry must
        // never revive a removed host or keep an alias's previous target.
        let host = self.identity.resolve(
            authority,
            table,
            Some(&self.connection.listener),
        );
        if host.is_none() {
            self.current = None;
        }
        host
    }

    pub(crate) fn bind(
        &mut self,
        app: HostExecution,
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
                self.listener,
            )));
        }
        HostService {
            app,
            policy: self
                .current
                .as_ref()
                .expect("host policy is prepared")
                .clone(),
        }
    }
}
