//! An edge process and an application process, and one exchange between them.
//!
//! The edge is this process. It starts the application as a second process of
//! the same executable and keeps a fixed number of exchanges in flight to it,
//! through whichever transport the bench names. The application answers with
//! the production owner contract: `/bench/plain` and `/bench/echo`, both whole,
//! so a reply says its size.
//!
//! Both processes report their user and system CPU and their context switches
//! for each measured window; divided by the exchanges completed in it, they
//! are what one exchange costs each side. The report is written as JSON, so a
//! later run can be set beside it.

use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use claviron::http::ext::{ConnCtx, RequestArrival};
use claviron::http::{RouteBody, RouteResponse, bytes_body, empty_body};
use claviron::transport::owner::Owner;
use http::{Request, Response, StatusCode};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

pub type Result<T> =
    std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub const PLAIN: &[u8] = b"ok\n";

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum Workload {
    /// `GET /bench/plain`: no request body, `ok\n` back.
    Plain,
    /// `POST /bench/echo`: `--body-bytes` there and the same bytes back.
    Echo,
}

impl Workload {
    pub fn path(self) -> &'static str {
        match self {
            Self::Plain => "/bench/plain",
            Self::Echo => "/bench/echo",
        }
    }

    /// The request head; the transport supplies the body it carries.
    pub fn request<B>(
        self,
        body: B,
        length: usize,
    ) -> Request<B> {
        let builder = Request::builder()
            .uri(self.path())
            .header(http::header::HOST, "application.example.test");
        match self {
            Self::Plain => builder.method(http::Method::GET),
            Self::Echo => builder
                .method(http::Method::POST)
                .header(http::header::CONTENT_LENGTH, length),
        }
        .body(body)
        .expect("a valid benchmark request")
    }
}

#[derive(Clone, Parser)]
pub struct Args {
    #[arg(long = "bench", hide = true)]
    _bench: bool,
    #[arg(long, value_enum)]
    pub workload: Workload,
    /// Request body size for `echo`.
    #[arg(long, default_value_t = 0)]
    pub body_bytes: usize,
    /// Link: the request policy's `buffer_size` under `stream_default`. A
    /// body declared within it is held whole by the edge; absent is `off`.
    #[arg(long)]
    pub buffer_size: Option<usize>,
    /// HTTP/2: the upstream pool's `capacity.max_live`, connections it may
    /// open.
    #[arg(long)]
    pub max_live: Option<NonZeroUsize>,
    /// HTTP/2: the upstream pool's `capacity.max_streams` per connection.
    #[arg(long)]
    pub max_streams: Option<NonZeroUsize>,
    /// Exchanges the edge keeps in flight.
    #[arg(long)]
    pub in_flight: NonZeroUsize,
    #[arg(long)]
    pub edge_threads: NonZeroUsize,
    #[arg(long)]
    pub application_threads: NonZeroUsize,
    #[arg(long, value_parser = seconds)]
    pub warmup_seconds: Duration,
    #[arg(long, value_parser = seconds)]
    pub measurement_seconds: Duration,
    #[arg(long)]
    pub samples: NonZeroUsize,
    #[arg(long)]
    pub output: PathBuf,
    /// The application role, started by the edge with the fleet it chose.
    #[arg(long, hide = true)]
    application: Option<String>,
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

/// Stops what an application process serves, on its runtime.
pub type Stop = Box<dyn FnOnce(&Runtime) -> Result<()>>;

/// What an application process serves, and how it stops.
pub struct Serving {
    /// Where the edge reaches it, for a transport with an address.
    pub address: Option<SocketAddr>,
    pub stop: Stop,
}

/// One way for the edge to reach the application.
pub trait Transport: Send + Sync + Sized + 'static {
    const NAME: &'static str;

    /// In the application process: serve [`Bench`] and say where.
    fn serve(
        runtime: &Runtime,
        args: &Args,
        fleet: &str,
    ) -> Result<Serving>;

    /// In the edge process: reach what [`Transport::serve`] started.
    fn connect(
        runtime: &Runtime,
        args: &Args,
        fleet: &str,
        address: Option<SocketAddr>,
    ) -> Result<Self>;

    /// One exchange, its reply read to the end. `caller` is stable for each
    /// of the in-flight callers.
    fn exchange(
        &self,
        caller: usize,
        workload: Workload,
        body: &Bytes,
    ) -> impl Future<Output = Result<Bytes>> + Send;
}

/// Remove every shared-memory segment of `fleet` and its lock files, once
/// both processes have stopped. The fleet name is this run's own, chosen by
/// the edge from its process id; nothing else uses it.
fn clear_fleet(fleet: &str) -> Result<()> {
    use claviron::orbitive::core::shm::{
        companion_lock_files, fleet_lock_path, ring_segment_name_for_uid,
    };
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    for kind in u8::MIN..=u8::MAX {
        let name = ring_segment_name_for_uid(fleet, kind, uid);
        let path = std::ffi::CString::new(name.as_str())?;
        // SAFETY: `path` is a valid, NUL-terminated POSIX SHM name.
        if unsafe { libc::shm_unlink(path.as_ptr()) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ENOENT) {
                return Err(format!("{name}: {error}").into());
            }
        }
        for lock in companion_lock_files(&name, uid)? {
            remove_if_present(&lock)?;
        }
    }
    remove_if_present(&fleet_lock_path(fleet))
}

fn remove_if_present(path: &std::path::Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("{}: {error}", path.display()).into()),
    }
}

/// The application's routes, answered whole.
pub struct Bench;

impl Owner for Bench {
    fn call(
        &self,
        request: Request<RouteBody>,
        _connection: Option<&Arc<ConnCtx>>,
        _arrival: RequestArrival,
    ) -> impl Future<Output = RouteResponse> + Send + use<> {
        async move {
            match request.uri().path() {
                "/bench/plain" => {
                    Response::new(bytes_body(Bytes::from_static(PLAIN)))
                }
                "/bench/echo" => match request.into_body().collect().await {
                    Ok(body) => Response::new(bytes_body(body.to_bytes())),
                    Err(_) => status(StatusCode::BAD_REQUEST),
                },
                _ => status(StatusCode::NOT_FOUND),
            }
        }
    }
}

fn status(code: StatusCode) -> RouteResponse {
    let mut reply = Response::new(empty_body());
    *reply.status_mut() = code;
    reply
}

/// What all threads of one process consumed, and how often they were
/// switched out. macOS reports no context switches here; Linux does.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Usage {
    user_ns: u64,
    system_ns: u64,
    voluntary_switches: u64,
    involuntary_switches: u64,
}

impl Usage {
    fn process() -> Result<Self> {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes the supplied rusage on success.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) }
            != 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: the successful call above initialized every field.
        let usage = unsafe { usage.assume_init() };
        let ns = |value: libc::timeval| {
            value.tv_sec as u64 * 1_000_000_000 + value.tv_usec as u64 * 1_000
        };
        Ok(Self {
            user_ns: ns(usage.ru_utime),
            system_ns: ns(usage.ru_stime),
            voluntary_switches: usage.ru_nvcsw as u64,
            involuntary_switches: usage.ru_nivcsw as u64,
        })
    }

    fn since(
        self,
        earlier: Self,
    ) -> Self {
        Self {
            user_ns: self.user_ns - earlier.user_ns,
            system_ns: self.system_ns - earlier.system_ns,
            voluntary_switches: self.voluntary_switches
                - earlier.voluntary_switches,
            involuntary_switches: self.involuntary_switches
                - earlier.involuntary_switches,
        }
    }

    fn add(
        self,
        other: &Self,
    ) -> Self {
        Self {
            user_ns: self.user_ns + other.user_ns,
            system_ns: self.system_ns + other.system_ns,
            voluntary_switches: self.voluntary_switches
                + other.voluntary_switches,
            involuntary_switches: self.involuntary_switches
                + other.involuntary_switches,
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Control {
    Begin,
    End,
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize)]
enum Reply {
    Ready { address: Option<SocketAddr> },
    Begun,
    Used(Usage),
    Stopped,
}

fn send<T: Serialize>(
    output: &mut impl Write,
    value: &T,
) -> Result<()> {
    serde_json::to_writer(&mut *output, value)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

fn receive<T: for<'de> Deserialize<'de>>(
    input: &mut impl BufRead
) -> Result<T> {
    let mut line = String::new();
    if input.read_line(&mut line)? == 0 {
        return Err("the other process closed its control pipe".into());
    }
    Ok(serde_json::from_str(&line)?)
}

fn application<T: Transport>(
    args: &Args,
    fleet: &str,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.application_threads.get())
        .enable_all()
        .build()?;
    let serving = T::serve(&runtime, args, fleet)?;
    let mut output = std::io::stdout().lock();
    send(&mut output, &Reply::Ready { address: serving.address })?;
    let mut input = std::io::stdin().lock();
    let mut began = None;
    loop {
        match receive::<Control>(&mut input)? {
            Control::Begin => {
                began = Some(Usage::process()?);
                send(&mut output, &Reply::Begun)?;
            }
            Control::End => {
                let earlier = began.take().ok_or("CPU sample not started")?;
                let used = Usage::process()?.since(earlier);
                send(&mut output, &Reply::Used(used))?;
            }
            Control::Shutdown => break,
        }
    }
    (serving.stop)(&runtime)?;
    send(&mut output, &Reply::Stopped)?;
    Ok(())
}

struct Application {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    address: Option<SocketAddr>,
}

impl Application {
    fn spawn(fleet: &str) -> Result<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .args(std::env::args_os().skip(1))
            .arg("--application")
            .arg(fleet)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = child.stdin.take().expect("piped child stdin");
        let mut output =
            BufReader::new(child.stdout.take().expect("piped child stdout"));
        match receive::<Reply>(&mut output) {
            Ok(Reply::Ready { address }) => {
                Ok(Self { child, input, output, address })
            }
            other => {
                drop(input);
                let status = child.wait()?;
                Err(format!(
                    "application did not become ready ({status}): {other:?}"
                )
                .into())
            }
        }
    }

    fn ask(
        &mut self,
        control: Control,
    ) -> Result<Reply> {
        send(&mut self.input, &control)?;
        receive(&mut self.output)
    }

    fn begin(&mut self) -> Result<()> {
        match self.ask(Control::Begin)? {
            Reply::Begun => Ok(()),
            other => Err(format!("unexpected reply {other:?}").into()),
        }
    }

    fn end(&mut self) -> Result<Usage> {
        match self.ask(Control::End)? {
            Reply::Used(usage) => Ok(usage),
            other => Err(format!("unexpected reply {other:?}").into()),
        }
    }

    fn shutdown(mut self) -> Result<u32> {
        let pid = self.child.id();
        match self.ask(Control::Shutdown)? {
            Reply::Stopped => {}
            other => return Err(format!("unexpected reply {other:?}").into()),
        }
        let status = self.child.wait()?;
        if !status.success() {
            return Err(format!("application exited with {status}").into());
        }
        Ok(pid)
    }
}

/// One exchange counter per caller, each on its own cache line.
#[repr(align(128))]
#[derive(Default)]
struct Count(AtomicU64);

#[derive(Serialize)]
struct Sample {
    exchanges: u64,
    wall_ns: u64,
    edge: Usage,
    application: Usage,
}

#[derive(Serialize)]
struct PerExchange {
    cpu_ns: f64,
    user_ns: f64,
    system_ns: f64,
    voluntary_switches: f64,
    involuntary_switches: f64,
}

impl PerExchange {
    fn of(
        usage: &Usage,
        exchanges: u64,
    ) -> Self {
        let per = |value: u64| value as f64 / exchanges as f64;
        Self {
            cpu_ns: per(usage.user_ns + usage.system_ns),
            user_ns: per(usage.user_ns),
            system_ns: per(usage.system_ns),
            voluntary_switches: per(usage.voluntary_switches),
            involuntary_switches: per(usage.involuntary_switches),
        }
    }
}

#[derive(Serialize)]
struct Report {
    transport: &'static str,
    workload: Workload,
    body_bytes: usize,
    buffer_size: Option<usize>,
    max_live: Option<usize>,
    max_streams: Option<usize>,
    in_flight: usize,
    edge_threads: usize,
    application_threads: usize,
    edge_pid: u32,
    application_pid: u32,
    exchanges: u64,
    wall_ns: u64,
    exchanges_per_second: f64,
    edge: PerExchange,
    application: PerExchange,
    samples: Vec<Sample>,
}

fn edge<T: Transport>(args: &Args) -> Result<()> {
    if matches!(args.workload, Workload::Plain) && args.body_bytes != 0 {
        return Err("plain carries no request body".into());
    }
    let fleet = format!("ab{}", std::process::id() % 1_000_000);
    let outcome = measure::<T>(args, &fleet);
    let cleaned = clear_fleet(&fleet);
    let report = outcome?;
    cleaned?;
    println!(
        "{} {:?}: exchanges/s={:.0} edge {:.3} us (user {:.3} + system {:.3}, \
         {:.2} voluntary switches) application {:.3} us (user {:.3} + \
         system {:.3}, {:.2} voluntary switches) per exchange",
        report.transport,
        report.workload,
        report.exchanges_per_second,
        report.edge.cpu_ns / 1000.0,
        report.edge.user_ns / 1000.0,
        report.edge.system_ns / 1000.0,
        report.edge.voluntary_switches,
        report.application.cpu_ns / 1000.0,
        report.application.user_ns / 1000.0,
        report.application.system_ns / 1000.0,
        report.application.voluntary_switches,
    );
    if let Some(directory) = args.output.parent() {
        std::fs::create_dir_all(directory)?;
    }
    std::fs::write(&args.output, serde_json::to_vec_pretty(&report)?)?;
    Ok(())
}

fn measure<T: Transport>(
    args: &Args,
    fleet: &str,
) -> Result<Report> {
    let samples = u32::try_from(args.samples.get())?;
    let window = args.measurement_seconds / samples;
    let mut application = Application::spawn(fleet)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.edge_threads.get())
        .enable_all()
        .build()?;
    let transport =
        Arc::new(T::connect(&runtime, args, fleet, application.address)?);

    let body = Bytes::from(vec![b'x'; args.body_bytes]);
    let expected = match args.workload {
        Workload::Plain => Bytes::from_static(PLAIN),
        Workload::Echo => body.clone(),
    };
    // Every caller's first reply is checked whole before anything counts.
    for caller in 0..args.in_flight.get() {
        let reply = runtime.block_on(transport.exchange(
            caller,
            args.workload,
            &body,
        ))?;
        if reply != expected {
            return Err("a first reply is not the expected body".into());
        }
    }

    let running = Arc::new(AtomicBool::new(true));
    let counts: Arc<[Count]> =
        (0..args.in_flight.get()).map(|_| Count::default()).collect();
    let callers: Vec<_> = (0..args.in_flight.get())
        .map(|caller| {
            let transport = Arc::clone(&transport);
            let running = Arc::clone(&running);
            let counts = Arc::clone(&counts);
            let workload = args.workload;
            let body = body.clone();
            let length = expected.len();
            runtime.spawn(async move {
                while running.load(Ordering::Relaxed) {
                    let reply =
                        transport.exchange(caller, workload, &body).await?;
                    if reply.len() != length {
                        return Err::<
                            (),
                            Box<dyn std::error::Error + Send + Sync>,
                        >(
                            "a reply has the wrong length".into()
                        );
                    }
                    counts[caller].0.fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            })
        })
        .collect();
    let total = || {
        counts.iter().map(|count| count.0.load(Ordering::Relaxed)).sum::<u64>()
    };

    std::thread::sleep(args.warmup_seconds);
    let mut measured = Vec::with_capacity(args.samples.get());
    for _ in 0..samples {
        application.begin()?;
        let edge_before = Usage::process()?;
        let before = total();
        let started = Instant::now();
        std::thread::sleep(window);
        let exchanges = total() - before;
        let wall_ns = started.elapsed().as_nanos() as u64;
        let edge = Usage::process()?.since(edge_before);
        let application = application.end()?;
        measured.push(Sample { exchanges, wall_ns, edge, application });
    }

    running.store(false, Ordering::Relaxed);
    for caller in callers {
        runtime.block_on(caller)??;
    }
    drop(transport);
    runtime.shutdown_background();
    let application_pid = application.shutdown()?;

    let exchanges = measured.iter().map(|sample| sample.exchanges).sum::<u64>();
    let wall_ns = measured.iter().map(|sample| sample.wall_ns).sum::<u64>();
    if exchanges == 0 {
        return Err("no exchange completed in the measured windows".into());
    }
    let sum = |side: fn(&Sample) -> &Usage| {
        measured.iter().map(side).fold(Usage::default(), Usage::add)
    };
    Ok(Report {
        transport: T::NAME,
        workload: args.workload,
        body_bytes: args.body_bytes,
        buffer_size: args.buffer_size,
        max_live: args.max_live.map(NonZeroUsize::get),
        max_streams: args.max_streams.map(NonZeroUsize::get),
        in_flight: args.in_flight.get(),
        edge_threads: args.edge_threads.get(),
        application_threads: args.application_threads.get(),
        edge_pid: std::process::id(),
        application_pid,
        exchanges,
        wall_ns,
        exchanges_per_second: exchanges as f64 * 1e9 / wall_ns as f64,
        edge: PerExchange::of(&sum(|sample| &sample.edge), exchanges),
        application: PerExchange::of(
            &sum(|sample| &sample.application),
            exchanges,
        ),
        samples: measured,
    })
}

/// Parse the arguments and run this process's role.
pub fn run<T: Transport>() -> Result<()> {
    let args = Args::parse();
    match args.application.clone() {
        Some(fleet) => application::<T>(&args, &fleet),
        None => edge::<T>(&args),
    }
}
