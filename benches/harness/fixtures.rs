//! Resolve committed fixtures into one run-owned directory before either boot.
use super::{
    Result,
    config::{self, Args, ClientConfig, Launch, Side, Transport},
};
use serde_yaml::Value;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::net::TcpListener;

pub struct Fixtures {
    pub root: PathBuf,
    pub application: Launch,
    pub client: Launch,
    pub tcp_reservation: Option<TcpListener>,
}

impl Fixtures {
    pub async fn prepare(
        args: &Args,
        transport: Transport,
    ) -> Result<Self> {
        let config_root = args.config.canonicalize()?;
        let client: ClientConfig =
            config::read(&config_root.join("client.yaml"))?;
        client.validate()?;
        let capacity = claviron::runtime::FleetCapacity::BUILT.raw();
        if u16::from(args.client_workers) >= capacity
            || u16::from(args.application_workers) >= capacity
        {
            return Err(format!("worker counts must be below compiled CLAVIRON_FLEET_CAPACITY={capacity}").into());
        }
        if matches!(args.workload, config::Workload::Plain)
            && args.body_bytes != 0
        {
            return Err("plain has no request body".into());
        }
        if args.measurement_seconds / args.samples.get()
            == std::time::Duration::ZERO
        {
            return Err(
                "measurement window is shorter than the sample count".into()
            );
        }
        let output_root = PathBuf::from(".benchmarks/link");
        std::fs::create_dir_all(&output_root)?;
        let root = output_root.canonicalize()?.join(format!(
            "run-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        std::fs::create_dir(&root)?;
        let path = &root;
        // Origins use a cwd-relative socket name: upstream coordination keys
        // must not inherit the checkout's absolute path length.
        let application_socket = output_root
            .join(root.file_name().expect("run directory name"))
            .join("application.sock");
        let fleet = format!(
            "b{:06x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_micros() as u64
                & 0xffffff
        );
        let orbit = if matches!(transport, Transport::Orbit) {
            let mut orbit: claviron::orbit::config::OrbitConfig =
                config::read(&config_root.join("orbit.yaml"))?;
            let link = orbit
                .link
                .as_mut()
                .ok_or("orbit fixture must declare link geometry")?;
            link.max_in_flight = args.max_in_flight.map(|value| value.get());
            link.max_queued = args.max_queued;
            link.validate().map_err(|error| error.into_std())?;
            if usize::from(args.client_workers)
                + usize::from(args.application_workers)
                + 2
                > link.geometry.fleet_capacity
            {
                return Err("Link geometry needs room for both process families; raise compiled ORBIT_LINK_FLEET_CAPACITY and the selected geometry".into());
            }
            Some(orbit)
        } else {
            None
        };
        let mut listener: Value = match transport {
            Transport::Orbit => Value::Null,
            Transport::H2c => config::read(&config_root.join("h2c.yaml"))?,
            Transport::UdsH2 => config::read(&config_root.join("uds-h2.yaml"))?,
        };
        let (tcp_reservation, address) = if matches!(transport, Transport::H2c)
        {
            let listen = listener["listen"]
                .as_str()
                .ok_or("h2c fixture needs listen")?
                .parse()?;
            let backlog = u32::try_from(
                listener["tcp"]["backlog"]
                    .as_u64()
                    .ok_or("h2c fixture needs tcp.backlog")?,
            )?;
            let socket = claviron::sys::net::create_tcp_listener(
                listen,
                claviron::sys::net::PortSharing::Shared,
                std::num::NonZeroU32::new(backlog)
                    .ok_or("backlog must be positive")?,
            )
            .await?;
            let address = socket.local_addr()?;
            (Some(socket), address)
        } else {
            (None, "127.0.0.1:0".parse::<SocketAddr>()?)
        };
        let application_fleet = format!("{fleet}a");
        let application_name = format!("{fleet}-app");
        require_unused_fleet(&application_fleet)?;
        require_unused_fleet(&format!("{fleet}c"))?;
        let control = path.join("control.sock");
        let application = Launch {
            side: Side::Application,
            args: args.clone(),
            transport,
            control: control.clone(),
            directory: path.join("application"),
            name: application_name.clone(),
            fleet: application_fleet.clone(),
            application_fleet: application_fleet.clone(),
            application_name: application_name.clone(),
            client: client.clone(),
        };
        let client_launch = Launch {
            side: Side::Client,
            directory: path.join("client"),
            name: format!("{fleet}-client"),
            fleet: format!("{fleet}c"),
            ..application.clone()
        };
        let mut runner: claviron::runner::RunnerOptions =
            config::read(&config_root.join("runner.yaml"))?;
        runner.name = Some(application_name);
        runner.fleet_name = Some(application_fleet);
        runner.workers = Some(args.application_workers);
        runner.supervisor =
            Some(config::read(&config_root.join("supervisor.yaml"))?);
        match transport {
            Transport::Orbit => {
                runner.orbit = Some(serde_yaml::to_value(
                    orbit.as_ref().expect("selected Orbit config"),
                )?)
            }
            Transport::H2c | Transport::UdsH2 => {
                let unix = matches!(transport, Transport::UdsH2);
                let listen = if unix {
                    format!("unix:{}", application_socket.display())
                } else {
                    address.to_string()
                };
                listener["listen"] = Value::String(listen);
                runner.listeners = Some(serde_yaml::to_value(
                    std::collections::BTreeMap::from([(
                        "application",
                        listener,
                    )]),
                )?);
            }
        }
        // Keep sender and receiver on the same selected physical Link shape.
        for launch in [&application, &client_launch] {
            std::fs::create_dir_all(&launch.directory)?;
            std::fs::write(
                launch.directory.join("harness.json"),
                serde_json::to_vec(launch)?,
            )?;
            if let Some(orbit) = &orbit {
                std::fs::write(
                    launch.directory.join("orbit.yaml"),
                    serde_yaml::to_string(orbit)?,
                )?;
            }
            std::fs::copy(
                config_root.join("supervisor.yaml"),
                launch.directory.join("supervisor.yaml"),
            )?;
        }
        std::fs::write(
            application.directory.join("runner.yaml"),
            serde_yaml::to_string(&runner)?,
        )?;
        if !matches!(transport, Transport::Orbit) {
            let mut upstream: Value =
                config::read(&config_root.join("upstream.yaml"))?;
            let profile = &mut upstream["upstreams"]["application"];
            let origin = if matches!(transport, Transport::UdsH2) {
                format!("unix:{}", application_socket.display())
            } else {
                format!("tcp://{address}")
            };
            profile["origins"] = serde_yaml::to_value([origin])?;
            if let Some(live) = args.max_live {
                profile["capacity"]["max_live"] =
                    serde_yaml::to_value(live.get())?;
            }
            if let Some(streams) = args.max_streams {
                profile["capacity"]["max_streams"] =
                    serde_yaml::to_value(streams.get())?;
            }
            if let Some(queue) = args.queue_timeout {
                profile["requests"]["queue_timeout"] = Value::String(
                    humantime::format_duration(queue).to_string(),
                );
            }
            std::fs::write(
                client_launch.directory.join("upstream.yaml"),
                serde_yaml::to_string(&upstream)?,
            )?;
        }
        Ok(Self { root, application, client: client_launch, tcp_reservation })
    }
}

// Never adopt retained SHM from an earlier run that happened to use the same name.
fn require_unused_fleet(fleet: &str) -> Result<()> {
    use claviron::orbitive::core::shm::{
        companion_lock_files, fleet_lock_path, ring_segment_name_for_uid,
    };
    if fleet_lock_path(fleet).try_exists()? {
        return Err("generated fleet name already has retained state; retry with a new run".into());
    }
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    for kind in u8::MIN..=u8::MAX {
        let name = ring_segment_name_for_uid(fleet, kind, uid);
        if !companion_lock_files(&name, uid)?.is_empty() {
            return Err("generated fleet has retained locks".into());
        }
        let name = std::ffi::CString::new(name)?;
        // SAFETY: name is a valid NUL-terminated SHM name; no mapping is made.
        let fd = unsafe { libc::shm_open(name.as_ptr(), libc::O_RDONLY, 0) };
        if fd >= 0 {
            // SAFETY: this call owns the descriptor returned by shm_open.
            unsafe {
                libc::close(fd);
            }
            return Err("generated fleet has retained SHM".into());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ENOENT) {
            return Err(error.into());
        }
    }
    Ok(())
}
