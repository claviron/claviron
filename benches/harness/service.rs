//! A normal Claviron Runnable owns the control connection and generated work.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use claviron::orbit::link::application::{ApplicationService, LinkJoined};
use claviron::orbit::{Scope, Scoped};
use claviron::runtime::execution::execution_spec;
use claviron::{
    AppState, Component, ExecutionSpec, ProcessStarted, RunContext, Runnable,
    registry,
};
use tokio::net::UnixStream;
use tokio::task::JoinSet;

use super::{
    Result,
    backend::Backend,
    config::{Launch, Side, Workload},
    diagnostics::Observations,
    wire::{Channel, Command, Counts, Identity, Outcome, Reply, Usage, Window},
};

type Started = tokio::sync::broadcast::Receiver<ProcessStarted>;

struct Control {
    launch: Launch,
    started: Mutex<Option<Started>>,
    link_joined: Mutex<Option<tokio::sync::broadcast::Receiver<LinkJoined>>>,
}

pub fn register(
    state: &AppState,
    launch: Launch,
) -> registry::Result<()> {
    if launch.side == Side::Client
        && state.registry_ref().resolve::<ApplicationService>().is_none()
    {
        state
            .registry_ref()
            .register(Arc::new(ApplicationService::default()))?;
    }
    let started = state.events().subscribe::<ProcessStarted>();
    let link_joined = if launch.side == Side::Application
        && !state.is_master()
        && matches!(launch.transport, super::Transport::Orbit)
    {
        Some(state.events().subscribe::<LinkJoined>())
    } else {
        None
    };
    state.registry_ref().register(Arc::new(Control {
        launch,
        started: Mutex::new(Some(started)),
        link_joined: Mutex::new(link_joined),
    }))?;
    Ok(())
}
impl Component<AppState> for Control {
    type Deps = ();
    fn name(&self) -> &'static str {
        "LinkBenchControl"
    }
    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<AppState>>> {
        Some(self)
    }
    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
}
impl Scoped for Control {
    fn scope(&self) -> Scope {
        Scope::Any
    }
}

#[async_trait]
impl Runnable<AppState> for Control {
    fn execution_spec(
        &self,
        state: &AppState,
    ) -> ExecutionSpec {
        if !state.is_master() && self.launch.side == Side::Client {
            execution_spec(self.launch.client.execution)
        } else {
            ExecutionSpec::default()
        }
    }
    async fn run(
        self: Arc<Self>,
        _: (),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let mut started =
            self.started.lock().expect("startup receiver").take().ok_or_else(
                || registry::Error::msg("benchmark control cannot restart"),
            )?;
        tokio::select! {
            _ = context.cancelled() => return Ok(()),
            event = started.recv() => { event.map_err(|e| registry::Error::msg(e.to_string()))?; }
        }
        let joined =
            self.link_joined.lock().expect("Link readiness receiver").take();
        if let Some(mut joined) = joined {
            tokio::select! {
                _ = context.cancelled() => return Ok(()),
                event = joined.recv() => { event.map_err(|e| registry::Error::msg(e.to_string()))?; }
            }
        }
        match self.control(&state, &context).await {
            Ok(()) => Ok(()),
            Err(error) => {
                state.initiate_shutdown();
                Err(registry::Error::msg(error.to_string()))
            }
        }
    }
}

impl Control {
    async fn control(
        &self,
        state: &AppState,
        context: &RunContext,
    ) -> Result<()> {
        let mut channel =
            Channel::new(UnixStream::connect(&self.launch.control).await?);
        let identity = Identity {
            side: self.launch.side,
            pid: std::process::id(),
            master_pid: state.control_pid(),
            worker: state.worker_id(),
            fleet: state.fleet_name().to_string(),
            execution: if state.is_master() {
                "supervisor".into()
            } else if self.launch.side == Side::Client {
                self.launch.client.execution.as_str().into()
            } else {
                "runner".into()
            },
        };
        channel.send(&Reply::Ready(identity)).await?;
        let mut load: Option<Load> = None;
        let mut window: Option<Window> = None;
        let mut backend = None;
        let body = Bytes::from(vec![b'x'; self.launch.args.body_bytes]);
        let expected = match self.launch.args.workload {
            Workload::Plain => Bytes::from_static(super::PLAIN),
            Workload::Echo => body.clone(),
        };
        let observations = self.launch.args.diagnostics.then(Observations::new);
        let result: Result<()> = async {
        loop {
            let command = tokio::select! {
                _ = context.cancelled() => break,
                command = channel.receive::<Command>() => match command {
                    Ok(command) => command,
                    Err(error) => { state.initiate_shutdown(); return Err(error); }
                },
            };
            if matches!(command, Command::Shutdown) {
                if let Some(load) = load.take() { load.drain().await?; }
                channel.send(&Reply::Ack).await?;
                state.initiate_shutdown();
                return Ok(());
            }
            // Every command's failure is answered with Reply::Failed before
            // this loop ends, so the orchestrator reports the cause.
            let result: Result<Reply> = async { match command {
                Command::Prepare => {
                    if !state.is_master() && self.launch.side == Side::Client {
                        if backend.is_some() { return Err("backend already prepared".into()); }
                        backend = Some(Backend::prepare(state, &self.launch)?);
                    }
                    Ok(Reply::Ack)
                }
                Command::Preflight => {
                    if !state.is_master() && self.launch.side == Side::Client {
                        let prepared = backend.as_ref().ok_or("prepare must run before preflight")?;
                        let reply = tokio::time::timeout(self.launch.client.request_timeout,
                            prepared.exchange(self.launch.args.workload, &body)).await
                            .map_err(|_| "preflight exchange exceeded the client deadline")??;
                        if !reply.0.is_success() || reply.1 != expected {
                            return Err(format!("preflight response mismatch: status {}", reply.0).into());
                        }
                    }
                    Ok(Reply::Ack)
                }
                Command::Load { depth } => {
                    if load.is_some() { return Err("load already running".into()); }
                    if !state.is_master() && self.launch.side == Side::Client {
                        let backend = backend.clone().ok_or("prepare must run before load")?;
                        load = Some(Load::start(depth, backend, body.clone(), expected.clone(), &self.launch, observations.clone()).await?);
                    }
                    Ok(Reply::Ack)
                }
                Command::Begin => {
                    if window.is_some() { return Err("CPU window already open".into()); }
                    window = Some(Window { cpu: Usage::process()?, counts: load.as_ref().map_or_else(Default::default, |load| load.counts.snapshot()), start: Instant::now(), diagnostic: observations.as_ref().map(Observations::snapshot) });
                    Ok(Reply::Ack)
                }
                Command::End => {
                    let before = window.take().ok_or("CPU window not open")?;
                    let cpu = Usage::process()?.since(before.cpu);
                    let outcomes = load.as_ref().map_or_else(Default::default, |load| load.counts.snapshot()).since(before.counts);
                    Ok(Reply::Sample { cpu, outcomes, wall_ns: before.start.elapsed().as_nanos() as u64, diagnostic: observations.as_ref().map(Observations::snapshot).zip(before.diagnostic).map(|(after, before)| after.since(before)) })
                }
                Command::Drain => {
                    if let Some(load) = load.take() { load.drain().await?; }
                    Ok(Reply::Ack)
                }
                Command::Shutdown => unreachable!("handled before dispatch"),
            } }.await;
            match result {
                Ok(reply) => channel.send(&reply).await?,
                Err(error) => { channel.send(&Reply::Failed(error.to_string())).await?; return Err(error); }
            }
        }
        Ok(())
        }.await;
        if let Some(load) = load {
            load.drain().await?;
        }
        result
    }
}

struct Load {
    running: Arc<AtomicBool>,
    counts: Arc<Counts>,
    tasks: JoinSet<Result<()>>,
}
impl Load {
    async fn start(
        depth: usize,
        backend: Arc<Backend>,
        body: Bytes,
        expected: Bytes,
        launch: &Launch,
        observations: Option<Observations>,
    ) -> Result<Self> {
        let mut load = Self {
            running: Arc::new(AtomicBool::new(true)),
            counts: Arc::default(),
            tasks: JoinSet::new(),
        };
        let mut ready = Vec::with_capacity(depth);
        for _ in 0..depth {
            let running = load.running.clone();
            let counts = load.counts.clone();
            let backend = backend.clone();
            let body = body.clone();
            let expected = expected.clone();
            let workload = launch.args.workload;
            let timeout = launch.client.request_timeout;
            let observations = observations.clone();
            let (sent, receive) = tokio::sync::oneshot::channel();
            ready.push(receive);
            load.tasks.spawn(async move {
                let _ = sent.send(());
                while running.load(Ordering::Acquire) {
                    let deadline = tokio::time::Instant::now() + timeout;
                    let exchange = backend.exchange(workload, &body);
                    let response = tokio::time::timeout_at(deadline, async {
                        match &observations {
                            Some(observations) => {
                                observations.track(exchange).await
                            }
                            None => exchange.await,
                        }
                    })
                    .await;
                    // A future can return Ready after a delayed poll past its deadline.
                    let outcome = if tokio::time::Instant::now() > deadline {
                        Outcome::Timeout
                    } else {
                        match response {
                            Err(_) => Outcome::Timeout,
                            Ok(Err(error)) => {
                                if let Some(o) = &observations {
                                    o.rejected(&error.to_string());
                                }
                                Outcome::TransportError
                            }
                            Ok(Ok((status, bytes))) if status.is_success() => {
                                if bytes == expected {
                                    Outcome::Success
                                } else {
                                    Outcome::InvalidBody
                                }
                            }
                            Ok(Ok((status, _))) => match status.as_u16() {
                                503 => Outcome::Unavailable,
                                502 => Outcome::Gateway,
                                _ => Outcome::OtherStatus,
                            },
                        }
                    };
                    counts.increment(outcome);
                }
                Ok(())
            });
        }
        for receive in ready {
            receive.await?;
        }
        Ok(load)
    }
    async fn drain(mut self) -> Result<()> {
        self.running.store(false, Ordering::Release);
        let mut failure = None;
        while let Some(result) = self.tasks.join_next().await {
            let result = result
                .map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                })
                .and_then(|r| r);
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
