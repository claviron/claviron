//! TLS follows bounded TCP dialing; ALPN must match the owning HTTP protocol.
use std::net::SocketAddr;

use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::config::profile::UpstreamProtocol;
use crate::health::probe::OriginReachabilityFailure;
use crate::health::probe::failure::TlsCause;
use crate::origin::Origin;
use crate::proxy::service::UpstreamService;
impl UpstreamService {
    pub(crate) async fn dial_tls(
        &self,
        address: &SocketAddr,
        origin: &Origin,
    ) -> Result<TlsStream<TcpStream>, OriginReachabilityFailure> {
        let profile = &self.inner.profile;
        let stream = super::tcp::dial(
            address,
            profile.connection.connect_timeout,
            profile.connection.tcp_nodelay,
            &origin.source,
        )
        .await?;
        let prepared =
            profile.connector.as_ref().expect("compiled TLS origin connector");
        let name = origin.tls_name.clone().expect("compiled TLS server name");
        let stream = tokio::time::timeout(
            profile.tls.handshake_timeout,
            TlsConnector::from(prepared.client.clone()).connect(name, stream)
        )
        .await
        .map_err(|_| OriginReachabilityFailure::tls(TlsCause::Timeout))?
        .map_err(|error| {
            let certificate = matches!(
                error.get_ref().and_then(|e| e.downcast_ref::<rustls::Error>()),
                Some(rustls::Error::InvalidCertificate(_) | rustls::Error::NoCertificatesPresented)
            );
            if certificate {
                tracing::error!(origin = %origin.label, %error, "upstream TLS certificate failed");
            } else {
                tracing::debug!(origin = %origin.label, %error, "upstream TLS handshake failed");
            }
            let alpn = matches!(
                error.get_ref().and_then(|e| e.downcast_ref::<rustls::Error>()),
                Some(rustls::Error::NoApplicationProtocol)
            );
            OriginReachabilityFailure::tls(if certificate {
                TlsCause::Certificate
            } else if alpn {
                TlsCause::AlpnMismatch
            } else {
                TlsCause::Handshake
            })
        })?;
        let expected = match profile.protocol {
            UpstreamProtocol::H1 => b"http/1.1".as_slice(),
            UpstreamProtocol::H2 => b"h2".as_slice(),
        };
        if stream.get_ref().1.alpn_protocol() != Some(expected) {
            return Err(OriginReachabilityFailure::tls(TlsCause::AlpnMismatch));
        }
        Ok(stream)
    }
}
