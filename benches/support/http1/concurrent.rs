//! Persistent H1 load; client and server use separate processes and runtimes.

use std::{io, num::NonZeroUsize, path::PathBuf, time::Duration};

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

#[path = "concurrent/client.rs"]
mod client;
#[path = "concurrent/cpu.rs"]
mod cpu;
#[path = "concurrent/process.rs"]
mod process;
#[path = "concurrent/server.rs"]
mod server;

type Result<T> =
    std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum Target {
    Hyper,
    Axum,
    EdgeOther,
    EdgeAxum,
    EdgeRoutable,
}

impl Target {
    fn selects_host(self) -> bool {
        matches!(self, Self::EdgeOther | Self::EdgeAxum | Self::EdgeRoutable)
    }
}

#[derive(Parser)]
struct Args {
    #[arg(long = "bench", hide = true)]
    _bench: bool,
    #[arg(long, value_enum)]
    target: Target,
    #[arg(long)]
    connections: NonZeroUsize,
    #[arg(long)]
    server_threads: NonZeroUsize,
    #[arg(long)]
    client_threads: NonZeroUsize,
    #[arg(long, value_parser = seconds)]
    warmup_seconds: Duration,
    #[arg(long, value_parser = seconds)]
    measurement_seconds: Duration,
    #[arg(long)]
    samples: NonZeroUsize,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, hide = true)]
    server: bool,
}

fn seconds(text: &str) -> std::result::Result<Duration, String> {
    let value: f64 = text.parse().map_err(|error| format!("{error}"))?;
    let duration = Duration::try_from_secs_f64(value)
        .map_err(|error| error.to_string())?;
    if duration.is_zero() {
        return Err("duration must be positive".into());
    }
    Ok(duration)
}

#[derive(Serialize, Deserialize)]
enum Command {
    Begin,
    End,
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize)]
enum Reply {
    Ready(std::net::SocketAddr),
    Begun,
    Cpu(u64),
    Stopped,
}

#[derive(Serialize)]
struct Sample {
    requests: u64,
    wall_ns: u64,
    server_cpu_ns: u64,
    per_connection: Vec<u64>,
}

#[derive(Serialize)]
struct Report {
    target: Target,
    client_pid: u32,
    server_pid: u32,
    connections: usize,
    server_threads: usize,
    client_threads: usize,
    warmup_requests: u64,
    samples: Vec<Sample>,
    requests: u64,
    wall_ns: u64,
    server_cpu_ns: u64,
    requests_per_second: f64,
    server_cpu_ns_per_request: f64,
}

pub fn run() -> Result<()> {
    let args = Args::parse();
    if args.server {
        return server::run(&args);
    }
    let samples = u32::try_from(args.samples.get())?;
    let window = args.measurement_seconds / samples;
    if window.is_zero() {
        return Err(io::Error::other(
            "measurement window per sample must be positive",
        )
        .into());
    }
    let mut server = process::Server::spawn()?;
    let server_pid = server.pid();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.client_threads.get())
        .enable_all()
        .build()?;
    let mut clients =
        runtime.block_on(client::Clients::connect(&args, server.address))?;
    let warmup_requests =
        runtime.block_on(clients.exercise(args.warmup_seconds))?.requests;
    let mut observations = Vec::with_capacity(args.samples.get());
    for _ in 0..samples {
        server.begin()?;
        let mut sample = runtime.block_on(clients.exercise(window))?;
        sample.server_cpu_ns = server.end()?;
        observations.push(sample);
    }
    runtime.block_on(clients.shutdown())?;
    server.shutdown()?;
    let requests =
        observations.iter().map(|sample| sample.requests).sum::<u64>();
    let wall_ns = observations.iter().map(|sample| sample.wall_ns).sum::<u64>();
    let server_cpu_ns =
        observations.iter().map(|sample| sample.server_cpu_ns).sum::<u64>();
    if requests == 0 || wall_ns == 0 || server_cpu_ns == 0 {
        return Err(
            "completed-request, wall and server CPU totals must be positive"
                .into(),
        );
    }
    let report = Report {
        target: args.target,
        client_pid: std::process::id(),
        server_pid,
        connections: args.connections.get(),
        server_threads: args.server_threads.get(),
        client_threads: args.client_threads.get(),
        warmup_requests,
        requests,
        wall_ns,
        server_cpu_ns,
        requests_per_second: requests as f64 * 1e9 / wall_ns as f64,
        server_cpu_ns_per_request: server_cpu_ns as f64 / requests as f64,
        samples: observations,
    };
    std::fs::write(args.output, serde_json::to_vec_pretty(&report)?)?;
    println!(
        "requests={} requests/s={:.2} server CPU/request={:.3} us",
        report.requests,
        report.requests_per_second,
        report.server_cpu_ns_per_request / 1000.0
    );
    Ok(())
}
