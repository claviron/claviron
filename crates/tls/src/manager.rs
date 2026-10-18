use claviron_hosting::{Certificate, HostedCertificate, HostingStore};
use tracing::debug;

use crate::error::Error;
use crate::resolver::CertResolver;

pub struct CertificateManager<'a> {
    resolver: &'a CertResolver,
    store: &'a HostingStore,
}

impl<'a> CertificateManager<'a> {
    pub fn new(
        store: &'a HostingStore,
        resolver: &'a CertResolver,
    ) -> Self {
        Self { store, resolver }
    }

    pub async fn upsert_cn(
        &self,
        cn: &str,
    ) -> crate::Result<(Certificate, Vec<String>)> {
        let hosted = self
            .store
            .certificate_by_cn(cn)
            .await
            .map_err(|error| Error::Other(error.to_string()))?
            .ok_or_else(|| {
                Error::Other(format!("Certificate '{cn}' not found"))
            })?;
        self.upsert(hosted)
    }

    pub fn upsert(
        &self,
        hosted: HostedCertificate,
    ) -> crate::Result<(Certificate, Vec<String>)> {
        let cert = hosted.certificate;
        if !cert.kind.is_server() {
            return Err(Error::Other("client and CA certificates cannot be installed in the server resolver".into()));
        }
        let certified_key = crate::pem::db_cert_to_certified_key(&cert)?;
        let cert_arc = std::sync::Arc::new(certified_key);

        self.resolver.upsert_host(&cert.cn, cert_arc.clone());
        debug!(cn = %cert.cn, "certificate loaded");

        let aliases = hosted
            .aliases
            .into_iter()
            .map(|alias| alias.hostname)
            .collect::<Vec<_>>();
        for alias in &aliases {
            self.resolver.upsert_host(alias, cert_arc.clone());
        }
        debug!(cn = %cert.cn, ?aliases, "certificate aliases loaded");
        Ok((cert, aliases))
    }
}
