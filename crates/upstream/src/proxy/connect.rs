//! Owns origin dialing and protocol handshake selection before any request bytes.

use hyper_util::rt::TokioIo;

use crate::health::probe::OriginReachabilityFailure;
use crate::origin::{ConnectTarget, Origin};
use crate::proxy::failure::RequestFailure;
use crate::proxy::h1::conn::H1Connection;
use crate::proxy::h2::conn::H2Connection;
use crate::proxy::service::UpstreamService;

impl UpstreamService {
    pub(crate) async fn connect_h1_connection(
        &self,
        origin_index: Option<usize>,
        origin: &Origin,
        epoch: &Arc<crate::proxy::service::lifecycle::ServiceEpoch>,
    ) -> Result<H1Connection, AcquireConnectionError> {
        let driver_end = origin_index.map(|_| H1DriverEndContext);
        epoch
            .transport(origin.id)
            .native(async {
                match &origin.connect {
                    ConnectTarget::Tls(addr) => {
                        let stream = self.dial_tls(addr, origin).await?;
                        handshake_connection(
                            TokioIo::new(stream),
                            origin,
                            driver_end,
                            epoch,
                        )
                        .await
                    }
                    ConnectTarget::Tcp(addr) => {
                        let stream = tcp::dial(
                            addr,
                            self.inner.profile.connection.connect_timeout,
                            self.inner.profile.connection.tcp_nodelay,
                            &origin.source,
                        )
                        .await?;

                        handshake_connection(
                            TokioIo::new(stream),
                            origin,
                            driver_end,
                            epoch,
                        )
                        .await
                    }
                    ConnectTarget::Unix(path) => {
                        let stream = unix::dial(
                            path,
                            self.inner.profile.connection.connect_timeout,
                            &origin.source,
                        )
                        .await?;

                        handshake_connection(
                            TokioIo::new(stream),
                            origin,
                            driver_end,
                            epoch,
                        )
                        .await
                    }
                }
            })
            .await
    }

    pub(crate) async fn connect_h2_connection(
        &self,
        driver_end: Option<H2DriverEndContext>,
        origin: &Origin,
        conn_id: u64,
        epoch: &crate::proxy::service::lifecycle::ServiceEpoch,
    ) -> Result<H2Connection, AcquireConnectionError> {
        epoch
            .transport(origin.id)
            .native(async {
                match &origin.connect {
                    ConnectTarget::Tls(addr) => {
                        let stream = self.dial_tls(addr, origin).await?;
                        handshake_h2_connection(
                            TokioIo::new(stream),
                            origin,
                            conn_id,
                            epoch,
                            driver_end,
                            &self.inner.profile.connection,
                        )
                        .await
                    }
                    ConnectTarget::Tcp(addr) => {
                        let stream = tcp::dial(
                            addr,
                            self.inner.profile.connection.connect_timeout,
                            self.inner.profile.connection.tcp_nodelay,
                            &origin.source,
                        )
                        .await?;

                        handshake_h2_connection(
                            TokioIo::new(stream),
                            origin,
                            conn_id,
                            epoch,
                            driver_end,
                            &self.inner.profile.connection,
                        )
                        .await
                    }
                    ConnectTarget::Unix(path) => {
                        let stream = unix::dial(
                            path,
                            self.inner.profile.connection.connect_timeout,
                            &origin.source,
                        )
                        .await?;

                        handshake_h2_connection(
                            TokioIo::new(stream),
                            origin,
                            conn_id,
                            epoch,
                            driver_end,
                            &self.inner.profile.connection,
                        )
                        .await
                    }
                }
            })
            .await
    }
}

pub(crate) mod io;
mod tcp;
mod unix;
use crate::proxy::h1::conn::{H1DriverEndContext, handshake_connection};
use crate::proxy::h2::conn::{H2DriverEndContext, handshake_h2_connection};

#[derive(Debug)]
pub(crate) enum AcquireConnectionError {
    OriginReachabilityFailure(OriginReachabilityFailure),
    Pool(RequestFailure),
}

use std::sync::Arc;

mod tls;
