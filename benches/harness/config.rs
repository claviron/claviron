use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

use super::Result;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum Transport {
    Orbit,
    H2c,
    UdsH2,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Workload {
    Plain,
    Echo,
}

impl Workload {
    pub fn path(self) -> &'static str {
        match self {
            Self::Plain => "/bench/plain",
            Self::Echo => "/bench/echo",
        }
    }
}

#[derive(Clone, Debug, Parser, Deserialize, Serialize)]
pub struct Args {
    #[arg(long = "bench", hide = true)]
    bench: bool,
    #[arg(long, value_enum)]
    pub transport: Option<Transport>,
    #[arg(long, value_enum)]
    pub workload: Workload,
    #[arg(long, default_value_t = 0)]
    pub body_bytes: usize,
    #[arg(long)]
    pub buffer_size: Option<usize>,
    #[arg(long)]
    pub max_live: Option<NonZeroUsize>,
    #[arg(long)]
    pub max_streams: Option<NonZeroUsize>,
    #[arg(long)]
    pub max_in_flight: Option<NonZeroUsize>,
    /// Exchanges each receiver admits in all, running ones included;
    /// required with `--max-in-flight` (equal to it refuses at once).
    #[arg(long)]
    pub max_waiting: Option<usize>,
    #[arg(long, value_parser = duration)]
    pub queue_timeout: Option<Duration>,
    /// Retain a rejected phase instead of returning an unsuccessful benchmark exit.
    #[arg(long)]
    pub keep_errors: bool,
    #[arg(long)]
    pub diagnostics: bool,
    /// Per-client-worker concurrency ceiling. Every run ramps from one.
    #[arg(long)]
    pub in_flight: NonZeroUsize,
    /// Per-worker concurrency increment after an accepted level.
    #[arg(long, default_value = "1")]
    pub pace: NonZeroUsize,
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u8).range(1..))]
    pub client_workers: u8,
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u8).range(1..))]
    pub application_workers: u8,
    /// Main Tokio threads per client process; dedicated work has its own executor.
    #[arg(long)]
    pub edge_threads: NonZeroUsize,
    #[arg(long)]
    pub application_threads: NonZeroUsize,
    #[arg(long, value_parser = seconds)]
    pub warmup_seconds: Duration,
    #[arg(long, value_parser = seconds)]
    pub measurement_seconds: Duration,
    #[arg(long)]
    pub samples: NonZeroU32,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long, default_value = "benches/config/link")]
    pub config: PathBuf,
}

fn duration(text: &str) -> std::result::Result<Duration, String> {
    let value = humantime::parse_duration(text).map_err(|e| e.to_string())?;
    if value.is_zero() {
        return Err("duration must be positive".into());
    }
    Ok(value)
}
fn seconds(text: &str) -> std::result::Result<Duration, String> {
    let value = text.parse::<f64>().map_err(|e| e.to_string())?;
    let value =
        Duration::try_from_secs_f64(value).map_err(|e| e.to_string())?;
    if value.is_zero() {
        return Err("duration must be positive".into());
    }
    Ok(value)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    pub execution: String,
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
    pub accept: f64,
}

impl ClientConfig {
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.execution.as_str(), "shared" | "dedicated") {
            return Err("client execution must be shared or dedicated".into());
        }
        if self.request_timeout.is_zero()
            || !self.accept.is_finite()
            || !(0.0..=100.0).contains(&self.accept)
        {
            return Err(
                "request_timeout must be positive and accept must be 0..=100"
                    .into(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Client,
    Application,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Launch {
    pub side: Side,
    pub args: Args,
    pub transport: Transport,
    pub control: PathBuf,
    pub directory: PathBuf,
    pub name: String,
    pub fleet: String,
    pub application_fleet: String,
    pub application_name: String,
    pub client: ClientConfig,
}

pub fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    Ok(serde_yaml::from_str(&std::fs::read_to_string(path)?)?)
}

impl Launch {
    pub fn load(directory: &Path) -> Result<Self> {
        Ok(serde_json::from_slice(&std::fs::read(
            directory.join("harness.json"),
        )?)?)
    }
}
