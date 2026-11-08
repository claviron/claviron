use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use http_body_util::Empty;
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use tokio::{net::TcpStream, sync::Barrier, task::JoinHandle};

use super::{Args, Result, Target, Window, tls};
use crate::support::{http1::preflight, plain};

pub(super) struct Clients {
    connections: Vec<SendRequest<Empty<Bytes>>>,
    drivers: Vec<JoinHandle<hyper::Result<()>>>,
}

impl Clients {
    pub(super) async fn connect(
        args: &Args,
        target: Target,
        address: SocketAddr,
        trust: Option<&[u8]>,
    ) -> Result<Self> {
        let secure = trust.map(tls::Client::trusting).transpose()?;
        let mut connections = Vec::with_capacity(args.connections.get());
        let mut drivers = Vec::with_capacity(args.connections.get());
        for _ in 0..args.connections.get() {
            let stream = TcpStream::connect(address).await?;
            stream.set_nodelay(true)?;
            // The TLS handshake completes here, before any load window.
            let mut sender = match &secure {
                None => {
                    let (sender, driver) =
                        hyper::client::conn::http1::handshake(TokioIo::new(
                            stream,
                        ))
                        .await?;
                    drivers.push(tokio::spawn(driver));
                    sender
                }
                Some(secure) => {
                    let (sender, driver) =
                        hyper::client::conn::http1::handshake(TokioIo::new(
                            secure.connect(stream).await?,
                        ))
                        .await?;
                    drivers.push(tokio::spawn(driver));
                    sender
                }
            };
            if secure.is_some() {
                named_preflight(&mut sender, target.selects_host()).await?;
            } else {
                preflight(&mut sender, target.selects_host()).await;
            }
            connections.push(sender);
        }
        Ok(Self { connections, drivers })
    }

    pub(super) async fn exercise(
        &mut self,
        duration: Duration,
    ) -> Result<Window> {
        let connections = std::mem::take(&mut self.connections);
        let start = Arc::new(Barrier::new(connections.len() + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::with_capacity(connections.len());
        for mut connection in connections {
            let start = start.clone();
            let stop = stop.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                let mut requests = 0u64;
                loop {
                    // Wait until the connection takes another request: a
                    // reply can finish before its task asks for the next.
                    connection.ready().await?;
                    let response =
                        connection.send_request(plain::request()).await?;
                    plain::consume(response).await;
                    requests += 1;
                    // End only between requests: no partially completed reply is counted.
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                }
                Ok::<_, hyper::Error>((connection, requests))
            }));
        }
        let began = Instant::now();
        start.wait().await;
        tokio::time::sleep(duration).await;
        stop.store(true, Ordering::Relaxed);
        let mut per_connection = Vec::with_capacity(tasks.len());
        for task in tasks {
            let (connection, requests) = task.await??;
            self.connections.push(connection);
            per_connection.push(requests);
        }
        Ok(Window {
            requests: per_connection.iter().sum(),
            wall_ns: u64::try_from(began.elapsed().as_nanos())?,
            per_connection,
        })
    }

    pub(super) async fn shutdown(self) -> Result<()> {
        drop(self.connections);
        for driver in self.drivers {
            driver.await??;
        }
        Ok(())
    }
}

/// Over TLS the handshake's server name selected the host. Send the known
/// host, then another one: an owner that selects hosts must refuse a request
/// whose authority leaves the name the connection was accepted for.
async fn named_preflight(
    client: &mut SendRequest<Empty<Bytes>>,
    selects_host: bool,
) -> Result<()> {
    use http::{StatusCode, header};
    use http_body_util::BodyExt;

    client.ready().await?;
    plain::consume(client.send_request(plain::request()).await?).await;
    let mut other = plain::request();
    other.headers_mut().insert(header::HOST, "missing.test".parse()?);
    client.ready().await?;
    let response = client.send_request(other).await?;
    if selects_host {
        assert_eq!(
            response.status(),
            StatusCode::MISDIRECTED_REQUEST,
            "the request authority must be checked against the accepted name"
        );
        response.into_body().collect().await?;
    } else {
        plain::consume(response).await;
    }
    client.ready().await?;
    plain::consume(client.send_request(plain::request()).await?).await;
    Ok(())
}
