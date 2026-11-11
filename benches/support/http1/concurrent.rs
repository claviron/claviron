//! Persistent H1 load; client and server use separate processes and runtimes.
//!
//! With a control, both servers stay up and their measured windows alternate.
//! Whatever changes in the machine during the run reaches both owners, so the
//! ratio between them keeps its meaning when the absolute values drift.

use std::sync::Arc;
use std::{
    ffi::OsString, io, num::NonZeroUsize, path::PathBuf, time::Duration,
};

use clap::{Parser, ValueEnum};
use claviron::Execution;
use claviron::http::config::{HttpSnapshot, ListenerConfig};
use serde::{Deserialize, Serialize};

#[path = "concurrent/allocation.rs"]
mod allocation;
#[path = "concurrent/client.rs"]
mod client;
#[path = "concurrent/cpu.rs"]
mod cpu;
#[path = "concurrent/execution.rs"]
mod execution;
#[path = "concurrent/process.rs"]
mod process;
#[path = "concurrent/server.rs"]
mod server;
#[path = "concurrent/tls.rs"]
mod tls;

pub use allocation::Counting;

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

/// What carries HTTP/1 between the client and the server.
///
/// A new mode is measured with Edge Other against native Hyper; the other
/// owners stay on `plain`.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    ValueEnum,
)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
enum Mode {
    #[default]
    Plain,
    Tls,
}

impl Mode {
    fn accepts(
        self,
        target: Target,
    ) -> bool {
        match self {
            Self::Plain => true,
            Self::Tls => matches!(target, Target::Hyper | Target::EdgeOther),
        }
    }
}

#[derive(Clone, Parser)]
struct Args {
    #[arg(long = "bench", hide = true)]
    _bench: bool,
    #[arg(long, value_enum)]
    target: Target,
    /// A second owner, measured in alternating windows of the same run.
    #[arg(long, value_enum)]
    control: Option<Target>,
    #[arg(long, value_enum, default_value_t = Mode::Plain)]
    mode: Mode,
    #[arg(long)]
    connections: NonZeroUsize,
    #[arg(long)]
    server_threads: NonZeroUsize,
    /// An `http.yaml` with one listener: its `execution` places the server,
    /// through the same specification the production listener uses.
    #[arg(long)]
    http_config: PathBuf,
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

impl Args {
    /// The same controls for one serving process, naming its owner.
    fn server_arguments(
        &self,
        target: Target,
    ) -> Vec<OsString> {
        fn name(value: impl ValueEnum) -> OsString {
            value
                .to_possible_value()
                .expect("benchmark values are never skipped")
                .get_name()
                .into()
        }
        let mut arguments: Vec<OsString> = Vec::new();
        let mut push = |flag: &str, value: OsString| {
            arguments.push(flag.into());
            arguments.push(value);
        };
        push("--target", name(target));
        push("--mode", name(self.mode));
        push("--connections", self.connections.to_string().into());
        push("--server-threads", self.server_threads.to_string().into());
        push("--http-config", self.http_config.clone().into());
        push("--client-threads", self.client_threads.to_string().into());
        push(
            "--warmup-seconds",
            self.warmup_seconds.as_secs_f64().to_string().into(),
        );
        push(
            "--measurement-seconds",
            self.measurement_seconds.as_secs_f64().to_string().into(),
        );
        push("--samples", self.samples.to_string().into());
        push("--output", self.output.clone().into());
        arguments
    }

    fn http_threads(&self) -> Result<usize> {
        Ok(match self.listener()?.execution {
            Execution::Inherit => self.server_threads.get(),
            Execution::Dedicated => 1,
        })
    }

    /// The one listener the configuration declares.
    fn listener(&self) -> Result<Arc<ListenerConfig>> {
        let source = std::fs::read_to_string(&self.http_config)?;
        let config: HttpSnapshot = serde_yaml::from_str(&source)?;
        let mut listeners = config.listeners.into_values();
        match (listeners.next(), listeners.next()) {
            (Some(listener), None) => Ok(listener),
            _ => Err(format!(
                "{} must declare exactly one listener",
                self.http_config.display()
            )
            .into()),
        }
    }
}

/// How a report names the listener's execution.
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
    BeginCount,
    EndCount,
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize)]
enum Reply {
    /// The bound address and, in `tls` mode, the authority (DER) that
    /// signed this server's certificate.
    Ready {
        address: std::net::SocketAddr,
        trust: Option<Vec<u8>>,
    },
    Begun,
    Used(cpu::Usage),
    Counting,
    Allocated(allocation::Allocations),
    Stopped,
}

/// What the client observed while one load window ran.
struct Window {
    requests: u64,
    wall_ns: u64,
    per_connection: Vec<u64>,
}

#[derive(Serialize)]
struct Sample {
    requests: u64,
    wall_ns: u64,
    server_cpu_ns: u64,
    user_cpu_ns: u64,
    system_cpu_ns: u64,
    per_connection: Vec<u64>,
}

impl Sample {
    fn cpu_ns_per_request(&self) -> f64 {
        self.server_cpu_ns as f64 / self.requests as f64
    }
}

/// One serving process, its clients and what has been measured so far.
struct Side {
    target: Target,
    server: process::Server,
    clients: client::Clients,
    warmup_requests: u64,
    warmup_allocated: allocation::Allocations,
    samples: Vec<Sample>,
}

impl Side {
    fn start(
        args: &Args,
        target: Target,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<Self> {
        let mut server = process::Server::spawn(args.server_arguments(target))?;
        let mut clients = runtime.block_on(client::Clients::connect(
            args,
            target,
            server.address,
            server.trust.as_deref(),
        ))?;
        // Allocations are counted here, never inside a CPU window.
        server.begin_count()?;
        let warmup = runtime.block_on(clients.exercise(args.warmup_seconds))?;
        let warmup_allocated = server.end_count()?;
        if warmup.requests == 0 {
            return Err("warm-up completed no request".into());
        }
        Ok(Self {
            target,
            server,
            clients,
            warmup_requests: warmup.requests,
            warmup_allocated,
            samples: Vec::with_capacity(args.samples.get()),
        })
    }

    fn measure(
        &mut self,
        runtime: &tokio::runtime::Runtime,
        window: Duration,
    ) -> Result<()> {
        self.server.begin()?;
        let observed = runtime.block_on(self.clients.exercise(window))?;
        let used = self.server.end()?;
        self.samples.push(Sample {
            requests: observed.requests,
            wall_ns: observed.wall_ns,
            server_cpu_ns: used.user_ns + used.system_ns,
            user_cpu_ns: used.user_ns,
            system_cpu_ns: used.system_ns,
            per_connection: observed.per_connection,
        });
        Ok(())
    }

    fn finish(
        self,
        runtime: &tokio::runtime::Runtime,
        http_threads: usize,
    ) -> Result<Measured> {
        let Self {
            target,
            server,
            clients,
            warmup_requests,
            warmup_allocated,
            samples,
        } = self;
        runtime.block_on(clients.shutdown())?;
        let server_pid = server.pid();
        server.shutdown()?;
        let total =
            |field: fn(&Sample) -> u64| samples.iter().map(field).sum::<u64>();
        let requests = total(|sample| sample.requests);
        let wall_ns = total(|sample| sample.wall_ns);
        let server_cpu_ns = total(|sample| sample.server_cpu_ns);
        let user_cpu_ns = total(|sample| sample.user_cpu_ns);
        let system_cpu_ns = total(|sample| sample.system_cpu_ns);
        if requests == 0 || wall_ns == 0 || server_cpu_ns == 0 {
            return Err(
                "completed-request, wall and server CPU totals must be positive"
                    .into(),
            );
        }
        let per_request = |value: u64| value as f64 / requests as f64;
        Ok(Measured {
            target,
            server_pid,
            warmup_requests,
            warmup_allocations: warmup_allocated.calls,
            warmup_allocated_bytes: warmup_allocated.bytes,
            requests,
            wall_ns,
            server_cpu_ns,
            user_cpu_ns,
            system_cpu_ns,
            requests_per_second: requests as f64 * 1e9 / wall_ns as f64,
            server_cpu_ns_per_request: per_request(server_cpu_ns),
            user_cpu_ns_per_request: per_request(user_cpu_ns),
            system_cpu_ns_per_request: per_request(system_cpu_ns),
            http_thread_utilization: server_cpu_ns as f64
                / (wall_ns as f64 * http_threads as f64),
            allocations_per_request: warmup_allocated.calls as f64
                / warmup_requests as f64,
            allocated_bytes_per_request: warmup_allocated.bytes as f64
                / warmup_requests as f64,
            samples,
        })
    }
}

/// One owner's complete result. CPU figures come from the measured windows;
/// allocation figures come from its warm-up load.
#[derive(Serialize)]
struct Measured {
    target: Target,
    server_pid: u32,
    warmup_requests: u64,
    warmup_allocations: u64,
    warmup_allocated_bytes: u64,
    samples: Vec<Sample>,
    requests: u64,
    wall_ns: u64,
    server_cpu_ns: u64,
    user_cpu_ns: u64,
    system_cpu_ns: u64,
    requests_per_second: f64,
    server_cpu_ns_per_request: f64,
    user_cpu_ns_per_request: f64,
    system_cpu_ns_per_request: f64,
    /// Server CPU over wall time times HTTP threads. Well below one, the
    /// client set the load and the server spent the window waiting.
    http_thread_utilization: f64,
    allocations_per_request: f64,
    allocated_bytes_per_request: f64,
}

impl Measured {
    fn summary(&self) -> String {
        format!(
            "{:?}: requests={} requests/s={:.2} server CPU/request={:.3} us \
             (user {:.3} + system {:.3}) HTTP-thread utilization={:.1}% \
             allocations/request={:.2}",
            self.target,
            self.requests,
            self.requests_per_second,
            self.server_cpu_ns_per_request / 1000.0,
            self.user_cpu_ns_per_request / 1000.0,
            self.system_cpu_ns_per_request / 1000.0,
            self.http_thread_utilization * 100.0,
            self.allocations_per_request,
        )
    }
}

/// The target against its control, window by window.
#[derive(Serialize)]
struct Paired {
    /// Target CPU/request over control CPU/request, from the run's totals.
    server_cpu_ratio: f64,
    /// The same ratio for each adjacent pair of windows.
    server_cpu_ratio_per_sample: Vec<f64>,
    user_cpu_ns_per_request_difference: f64,
    system_cpu_ns_per_request_difference: f64,
    allocations_per_request_difference: f64,
}

impl Paired {
    fn new(
        target: &Measured,
        control: &Measured,
    ) -> Self {
        Self {
            server_cpu_ratio: target.server_cpu_ns_per_request
                / control.server_cpu_ns_per_request,
            server_cpu_ratio_per_sample: target
                .samples
                .iter()
                .zip(&control.samples)
                .map(|(target, control)| {
                    target.cpu_ns_per_request() / control.cpu_ns_per_request()
                })
                .collect(),
            user_cpu_ns_per_request_difference: target.user_cpu_ns_per_request
                - control.user_cpu_ns_per_request,
            system_cpu_ns_per_request_difference: target
                .system_cpu_ns_per_request
                - control.system_cpu_ns_per_request,
            allocations_per_request_difference: target.allocations_per_request
                - control.allocations_per_request,
        }
    }

    fn summary(
        &self,
        target: Target,
        control: Target,
    ) -> String {
        let (lowest, highest) = self.server_cpu_ratio_per_sample.iter().fold(
            (f64::INFINITY, f64::NEG_INFINITY),
            |(lowest, highest), ratio| {
                (lowest.min(*ratio), highest.max(*ratio))
            },
        );
        format!(
            "{target:?} / {control:?}: server CPU/request x{:.3} \
             (per window pair {lowest:.3}..{highest:.3}) \
             user {:+.3} us system {:+.3} us allocations/request {:+.2}",
            self.server_cpu_ratio,
            self.user_cpu_ns_per_request_difference / 1000.0,
            self.system_cpu_ns_per_request_difference / 1000.0,
            self.allocations_per_request_difference,
        )
    }
}

#[derive(Serialize)]
struct Report {
    #[serde(flatten)]
    measured: Measured,
    client_pid: u32,
    mode: Mode,
    connections: usize,
    server_threads: usize,
    listener_execution: &'static str,
    http_threads: usize,
    client_threads: usize,
    control: Option<Measured>,
    paired: Option<Paired>,
}

pub fn run() -> Result<()> {
    let args = Args::parse();
    for target in std::iter::once(args.target).chain(args.control) {
        if !args.mode.accepts(target) {
            return Err(format!(
                "{:?} mode measures Edge Other against Hyper; {target:?} is not served in it",
                args.mode
            )
            .into());
        }
    }
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
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.client_threads.get())
        .enable_all()
        .build()?;
    let mut sides = vec![Side::start(&args, args.target, &runtime)?];
    if let Some(control) = args.control {
        sides.push(Side::start(&args, control, &runtime)?);
    }
    for sample in 0..samples {
        // Target first, then control first: neither always follows the other.
        if sample % 2 == 0 {
            for side in &mut sides {
                side.measure(&runtime, window)?;
            }
        } else {
            for side in sides.iter_mut().rev() {
                side.measure(&runtime, window)?;
            }
        }
    }
    let http_threads = args.http_threads()?;
    let mut finished = sides
        .into_iter()
        .map(|side| side.finish(&runtime, http_threads))
        .collect::<Result<Vec<_>>>()?
        .into_iter();
    let measured = finished.next().expect("the target is always measured");
    let control = finished.next();
    let paired =
        control.as_ref().map(|control| Paired::new(&measured, control));
    println!("{}", measured.summary());
    if let (Some(control), Some(paired)) = (&control, &paired) {
        println!("{}", control.summary());
        println!("{}", paired.summary(measured.target, control.target));
    }
    let report = Report {
        measured,
        client_pid: std::process::id(),
        mode: args.mode,
        connections: args.connections.get(),
        server_threads: args.server_threads.get(),
        listener_execution: args.listener()?.execution.as_str(),
        http_threads,
        client_threads: args.client_threads.get(),
        control,
        paired,
    };
    std::fs::write(args.output, serde_json::to_vec_pretty(&report)?)?;
    Ok(())
}
