//! Phase orchestration and per-process windows; supervisors never enter serving CPU.
use super::{
    Result,
    config::{Args, Side, Transport},
    diagnostics::Diagnostics,
    fixtures::Fixtures,
    process::{self, Family},
    wire::{Command, Identity, Outcomes, Reply, Usage},
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::net::UnixListener;

#[derive(Serialize)]
struct ProcessSample {
    identity: Identity,
    cpu: Usage,
    outcomes: Outcomes,
    wall_ns: u64,
    diagnostic: Option<Diagnostics>,
}
#[derive(Serialize)]
struct RoleSample {
    cpu: Usage,
    cpu_ns_per_success: Option<f64>,
    cpu_ns_per_attempt: Option<f64>,
}
impl RoleSample {
    fn new(
        cpu: Usage,
        outcomes: Outcomes,
    ) -> Self {
        let per = |count: u64| {
            (count != 0)
                .then(|| (cpu.user_ns + cpu.system_ns) as f64 / count as f64)
        };
        Self {
            cpu,
            cpu_ns_per_success: per(outcomes.success),
            cpu_ns_per_attempt: per(outcomes.attempts()),
        }
    }
}
#[derive(Serialize)]
struct Sample {
    outcomes: Outcomes,
    client: RoleSample,
    application: RoleSample,
    supervisors: Usage,
    successful_per_second: f64,
    worker_windows: Vec<ProcessSample>,
}
#[derive(Serialize)]
struct Phase {
    per_worker_concurrency: usize,
    requested_concurrency: usize,
    outcomes: Outcomes,
    success_percent: f64,
    accepted: bool,
    samples: Vec<Sample>,
}
#[derive(Serialize)]
struct Report {
    schema_version: u32,
    transport: Transport,
    measurement_mode: &'static str,
    args: Args,
    client_policy: super::config::ClientConfig,
    compiled_edge: bool,
    compiled_metrics: bool,
    client_master: u32,
    application_master: u32,
    client_workers: Vec<u32>,
    application_workers: Vec<u32>,
    fixtures: BTreeMap<String, String>,
    context_switches_available: bool,
    phases: Vec<Phase>,
    last_accepted_concurrency: Option<usize>,
    first_rejected_concurrency: Option<usize>,
    reached_ceiling: bool,
}

pub async fn run(
    args: Args,
    transport: Transport,
) -> Result<()> {
    let mut fixtures = Fixtures::prepare(&args, transport).await?;
    let listener = UnixListener::bind(&fixtures.application.control)?;
    let mut families = Vec::new();
    let measured = tokio::select! {
        result = measure(&mut fixtures, &listener, &mut families) => result,
        signal = tokio::signal::ctrl_c() => { signal?; Err("benchmark interrupted".into()) },
    };
    let mut cleanup = None;
    // Stop generators first. Only the production supervisors manage workers.
    for family in families.iter_mut().rev() {
        if let Err(error) = family.shutdown().await {
            cleanup.get_or_insert(error);
        }
    }
    drop(listener);
    remove_if_present(&fixtures.application.control)?;
    if cleanup.is_none() {
        for fleet in [&fixtures.client.fleet, &fixtures.application.fleet] {
            clear_fleet(fleet)?;
        }
        remove_if_present(&fixtures.root.join("application.sock"))?;
    }
    let report = match measured {
        Ok(report) => report,
        Err(error) => {
            if let Some(cleanup) = cleanup {
                eprintln!("Cleanup incomplete: {cleanup}");
            }
            return Err(error);
        }
    };
    if let Some(error) = cleanup {
        return Err(error);
    }
    if let Some(parent) =
        args.output.parent().filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.output, serde_json::to_vec_pretty(&report)?)?;
    if report.first_rejected_concurrency.is_some() && !args.keep_errors {
        return Err(
            "acceptance failed; rejected phase is retained in the report"
                .into(),
        );
    }
    Ok(())
}

async fn measure(
    fixtures: &mut Fixtures,
    listener: &UnixListener,
    families: &mut Vec<Family>,
) -> Result<Report> {
    let args = &fixtures.application.args;
    families.push(Family::spawn(fixtures.application.clone())?);
    families[0].ready(listener, args.application_workers).await?;
    // Retain the ephemeral TCP port until the receiving worker family starts.
    // There is no unowned bind/drop/rebind race and no reservation in the load.
    drop(fixtures.tcp_reservation.take());
    families.push(Family::spawn(fixtures.client.clone())?);
    families[1].ready(listener, args.client_workers).await?;
    // Every caller attaches before any timed exchange: a caller's attachment
    // traffic must not overlap another caller's first deadline.
    process::require_ack(
        process::broadcast(families, &Command::Prepare).await?,
    )?;
    process::require_ack(
        process::broadcast(families, &Command::Preflight).await?,
    )?;
    let mut phases = Vec::new();
    let mut depth = 1;
    loop {
        process::require_ack(
            process::broadcast(families, &Command::Load { depth }).await?,
        )?;
        wait(families, args.warmup_seconds).await?;
        let mut samples = Vec::new();
        for _ in 0..args.samples.get() {
            // Workers added under pressure join from this window on.
            process::adopt_late(listener, families).await?;
            process::require_ack(
                process::broadcast(families, &Command::Begin).await?,
            )?;
            wait(families, args.measurement_seconds / args.samples.get())
                .await?;
            let replies = process::broadcast(families, &Command::End).await?;
            let mut windows = Vec::new();
            for (identity, reply) in replies {
                let Reply::Sample { cpu, outcomes, wall_ns, diagnostic } =
                    reply
                else {
                    return Err("missing process CPU sample".into());
                };
                windows.push(ProcessSample {
                    identity,
                    cpu,
                    outcomes,
                    wall_ns,
                    diagnostic,
                });
            }
            let outcomes = windows
                .iter()
                .filter(|s| {
                    s.identity.worker.is_some()
                        && s.identity.side == Side::Client
                })
                .fold(Outcomes::default(), |sum, s| sum.add(s.outcomes));
            let cpu = |side| {
                windows
                    .iter()
                    .filter(|s| {
                        s.identity.worker.is_some() && s.identity.side == side
                    })
                    .fold(Usage::default(), |sum, s| sum.add(s.cpu))
            };
            let supervisors = windows
                .iter()
                .filter(|s| s.identity.worker.is_none())
                .fold(Usage::default(), |sum, s| sum.add(s.cpu));
            let rate = windows
                .iter()
                .filter(|s| {
                    s.identity.side == Side::Client
                        && s.identity.worker.is_some()
                })
                .map(|s| s.outcomes.success as f64 * 1e9 / s.wall_ns as f64)
                .sum();
            if outcomes.attempts() == 0 {
                return Err("no attempts completed in a measured window".into());
            }
            samples.push(Sample {
                outcomes,
                client: RoleSample::new(cpu(Side::Client), outcomes),
                application: RoleSample::new(cpu(Side::Application), outcomes),
                supervisors,
                successful_per_second: rate,
                worker_windows: windows,
            });
        }
        process::require_ack(
            process::broadcast(families, &Command::Drain).await?,
        )?;
        let outcomes = samples
            .iter()
            .fold(Outcomes::default(), |sum, sample| sum.add(sample.outcomes));
        let success_percent =
            outcomes.success as f64 * 100.0 / outcomes.attempts() as f64;
        let accepted = success_percent >= fixtures.client.client.accept;
        let requested = depth
            .checked_mul(usize::from(args.client_workers))
            .ok_or("concurrency overflow")?;
        println!(
            "{:?}: {} per worker, {} total, {:.5}% success — {}",
            fixtures.client.transport,
            depth,
            requested,
            success_percent,
            if accepted { "accepted" } else { "rejected" }
        );
        phases.push(Phase {
            per_worker_concurrency: depth,
            requested_concurrency: requested,
            outcomes,
            success_percent,
            accepted,
            samples,
        });
        if !accepted || depth == args.in_flight.get() {
            break;
        }
        depth = depth
            .checked_add(args.pace.get())
            .ok_or("concurrency overflow")?
            .min(args.in_flight.get());
    }
    let mut resolved = BTreeMap::new();
    for (side, directory) in [
        ("application", &fixtures.application.directory),
        ("client", &fixtures.client.directory),
    ] {
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            if path.extension().is_some_and(|extension| extension == "yaml") {
                resolved.insert(
                    format!(
                        "{side}/{}",
                        path.file_name().unwrap().to_string_lossy()
                    ),
                    std::fs::read_to_string(path)?,
                );
            }
        }
    }
    let pids = |side| {
        families
            .iter()
            .flat_map(|family| &family.peers)
            .filter(|peer| {
                peer.identity.side == side && peer.identity.worker.is_some()
            })
            .map(|peer| peer.identity.pid)
            .collect()
    };
    Ok(Report {
        schema_version: 3,
        transport: fixtures.client.transport,
        measurement_mode: if args.diagnostics {
            "diagnostic"
        } else {
            "clean-cpu"
        },
        args: args.clone(),
        client_policy: fixtures.client.client.clone(),
        compiled_edge: cfg!(feature = "edge"),
        compiled_metrics: cfg!(feature = "metrics"),
        client_master: families[1].child.id(),
        application_master: families[0].child.id(),
        client_workers: pids(Side::Client),
        application_workers: pids(Side::Application),
        fixtures: resolved,
        context_switches_available: !cfg!(target_os = "macos"),
        last_accepted_concurrency: phases
            .iter()
            .rfind(|p| p.accepted)
            .map(|p| p.requested_concurrency),
        first_rejected_concurrency: phases
            .iter()
            .find(|p| !p.accepted)
            .map(|p| p.requested_concurrency),
        reached_ceiling: phases.last().is_some_and(|p| {
            p.accepted && p.per_worker_concurrency == args.in_flight.get()
        }),
        phases,
    })
}

async fn wait(
    families: &[Family],
    duration: Duration,
) -> Result<()> {
    let exits = families.iter().map(|family| Box::pin(family.process.wait()));
    tokio::select! {
        _ = tokio::time::sleep(duration) => Ok(()),
        result = futures_util::future::select_all(exits) => { result.0?; Err("a supervisor exited during measurement".into()) }
    }
}

fn remove_if_present(path: &std::path::Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn clear_fleet(fleet: &str) -> Result<()> {
    use claviron::orbitive::core::shm::{
        companion_lock_files, fleet_lock_path, ring_segment_name_for_uid,
        try_lock_fleet_exclusive,
    };
    // SAFETY: getuid has no preconditions; these are only this run's fleets.
    let uid = unsafe { libc::geteuid() };
    let _exclusive = try_lock_fleet_exclusive(fleet, uid)?
        .ok_or("fleet is still joined; refusing cleanup")?;
    for kind in u8::MIN..=u8::MAX {
        let name = ring_segment_name_for_uid(fleet, kind, uid);
        let path = std::ffi::CString::new(name.as_str())?;
        // SAFETY: path is a valid POSIX SHM name; both owned families exited.
        if unsafe { libc::shm_unlink(path.as_ptr()) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ENOENT) {
                return Err(error.into());
            }
        }
        for lock in companion_lock_files(&name, uid)? {
            remove_if_present(&lock)?;
        }
    }
    remove_if_present(&fleet_lock_path(fleet))
}
