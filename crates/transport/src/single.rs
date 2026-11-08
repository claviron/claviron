//! Named socket listeners serving one request owner.

use std::sync::Arc;

use claviron_http::config::{ListenerConfig, ListenerSettings, TcpSecurity};
use claviron_http::listener::{HttpProtocol, ListenerTransport};
use claviron_runtime::{AppState, RunContext, registry};

use crate::{owner::Owner, tcp};

/// Drive a prepared single-owner declaration. A TLS declaration requires the
/// matching certificate input; the caller owns configuration and publication.
pub async fn serve<O: Owner>(
    config: Arc<ListenerConfig>,
    owner: Arc<O>,
    #[cfg(feature = "tls")] tls: Option<&tcp::tls::Tls>,
    state: &AppState,
    context: &RunContext,
) -> registry::Result<()> {
    let ListenerSettings::Tcp(settings) = &config.settings else {
        return Err(registry::Error::msg(
            "single socket owner requires a stream listener",
        ));
    };
    if matches!(settings.security, TcpSecurity::Tls { .. }) {
        #[cfg(feature = "tls")]
        {
            let tls = tls.ok_or_else(|| {
                registry::Error::msg(
                    "TLS listener requires a certificate and key",
                )
            })?;
            let protocols = settings
                .protocols
                .iter()
                .map(|protocol| match protocol {
                    HttpProtocol::H1 => &b"http/1.1"[..],
                    HttpProtocol::H2 => &b"h2"[..],
                    HttpProtocol::H3 => {
                        unreachable!("stream listener validated")
                    }
                })
                .collect::<Vec<_>>();
            let tls = tls
                .config(&protocols)
                .map_err(|error| registry::Error::msg(error.to_string()))?;
            let snapshot =
                Arc::new(tcp::secure::Single::shared(config, tls, owner)?);
            return tcp::listener::serve(
                snapshot,
                context,
                state.run_mode(),
                |snapshot, stream, peer, scope| {
                    tcp::secure::serve(stream, peer, snapshot, scope)
                },
            )
            .await;
        }
        #[cfg(not(feature = "tls"))]
        return Err(registry::Error::msg("TLS transport is not enabled"));
    }
    let h1 = settings.protocols.contains(&HttpProtocol::H1);
    let h2 = settings.protocols.contains(&HttpProtocol::H2);
    let unix = config.transport() == ListenerTransport::Unix;
    let single = tcp::plain::Single::shared(config, owner)?;
    let snapshot =
        Arc::new(if h2 { single.prior_knowledge(h1) } else { single });
    if unix {
        #[cfg(unix)]
        return crate::unix::serve_current(
            || Some(snapshot.clone()),
            snapshot.clone(),
            state,
            context,
        )
        .await;
        #[cfg(not(unix))]
        return Err(registry::Error::msg(
            "Unix sockets are not available on this platform",
        ));
    }
    tcp::listener::serve(
        snapshot,
        context,
        state.run_mode(),
        |snapshot, stream, peer, scope| {
            tcp::plain::serve(stream, peer, snapshot, scope)
        },
    )
    .await
}
