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

use super::{Args, Result, Sample};
use crate::support::{
    http1::{Workload, preflight},
    plain,
};

pub(super) struct Clients {
    connections: Vec<SendRequest<Empty<Bytes>>>,
    drivers: Vec<JoinHandle<hyper::Result<()>>>,
}

impl Clients {
    pub(super) async fn connect(
        args: &Args,
        address: SocketAddr,
    ) -> Result<Self> {
        let mut connections = Vec::with_capacity(args.connections.get());
        let mut drivers = Vec::with_capacity(args.connections.get());
        for _ in 0..args.connections.get() {
            let stream = TcpStream::connect(address).await?;
            stream.set_nodelay(true)?;
            let (mut sender, driver) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream))
                    .await?;
            drivers.push(tokio::spawn(driver));
            preflight(&mut sender, Workload::Plain, args.target.selects_host())
                .await;
            connections.push(sender);
        }
        Ok(Self { connections, drivers })
    }

    pub(super) async fn exercise(
        &mut self,
        duration: Duration,
    ) -> Result<Sample> {
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
        Ok(Sample {
            requests: per_connection.iter().sum(),
            wall_ns: u64::try_from(began.elapsed().as_nanos())?,
            server_cpu_ns: 0,
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
