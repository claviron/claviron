use super::{Result, config::Side};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct Usage {
    pub user_ns: u64,
    pub system_ns: u64,
    pub voluntary_switches: u64,
    pub involuntary_switches: u64,
}
impl Usage {
    pub fn process() -> Result<Self> {
        let mut value = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes this output when it succeeds.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, value.as_mut_ptr()) }
            != 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: success above initialized every field.
        let value = unsafe { value.assume_init() };
        let ns = |t: libc::timeval| {
            t.tv_sec as u64 * 1_000_000_000 + t.tv_usec as u64 * 1_000
        };
        Ok(Self {
            user_ns: ns(value.ru_utime),
            system_ns: ns(value.ru_stime),
            voluntary_switches: value.ru_nvcsw as u64,
            involuntary_switches: value.ru_nivcsw as u64,
        })
    }
    pub fn since(
        self,
        before: Self,
    ) -> Self {
        Self {
            user_ns: self.user_ns - before.user_ns,
            system_ns: self.system_ns - before.system_ns,
            voluntary_switches: self.voluntary_switches
                - before.voluntary_switches,
            involuntary_switches: self.involuntary_switches
                - before.involuntary_switches,
        }
    }
    pub fn add(
        self,
        other: Self,
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

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct Outcomes {
    pub success: u64,
    pub unavailable_503: u64,
    pub gateway_502: u64,
    pub other_status: u64,
    pub transport_errors: u64,
    pub invalid_body: u64,
    pub timeouts: u64,
}
impl Outcomes {
    pub fn attempts(self) -> u64 {
        self.success
            + self.unavailable_503
            + self.gateway_502
            + self.other_status
            + self.transport_errors
            + self.invalid_body
            + self.timeouts
    }
    pub fn add(
        self,
        rhs: Self,
    ) -> Self {
        Self {
            success: self.success + rhs.success,
            unavailable_503: self.unavailable_503 + rhs.unavailable_503,
            gateway_502: self.gateway_502 + rhs.gateway_502,
            other_status: self.other_status + rhs.other_status,
            transport_errors: self.transport_errors + rhs.transport_errors,
            invalid_body: self.invalid_body + rhs.invalid_body,
            timeouts: self.timeouts + rhs.timeouts,
        }
    }
    pub fn since(
        self,
        before: Self,
    ) -> Self {
        Self {
            success: self.success - before.success,
            unavailable_503: self.unavailable_503 - before.unavailable_503,
            gateway_502: self.gateway_502 - before.gateway_502,
            other_status: self.other_status - before.other_status,
            transport_errors: self.transport_errors - before.transport_errors,
            invalid_body: self.invalid_body - before.invalid_body,
            timeouts: self.timeouts - before.timeouts,
        }
    }
}

#[repr(usize)]
pub enum Outcome {
    Success,
    Unavailable,
    Gateway,
    OtherStatus,
    TransportError,
    InvalidBody,
    Timeout,
}

#[derive(Default)]
pub struct Counts(pub [AtomicU64; 7]);
impl Counts {
    pub fn snapshot(&self) -> Outcomes {
        let get = |i: usize| self.0[i].load(Ordering::Relaxed);
        Outcomes {
            success: get(Outcome::Success as usize),
            unavailable_503: get(Outcome::Unavailable as usize),
            gateway_502: get(Outcome::Gateway as usize),
            other_status: get(Outcome::OtherStatus as usize),
            transport_errors: get(Outcome::TransportError as usize),
            invalid_body: get(Outcome::InvalidBody as usize),
            timeouts: get(Outcome::Timeout as usize),
        }
    }
    pub fn increment(
        &self,
        outcome: Outcome,
    ) {
        self.0[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Identity {
    pub side: Side,
    pub pid: u32,
    pub master_pid: u32,
    pub worker: Option<u8>,
    pub fleet: String,
    pub execution: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Command {
    /// Attach every caller to its destination; acknowledged only once this
    /// caller's backend is prepared, so the broadcast is a barrier.
    Prepare,
    /// One timed exchange per caller, after every caller has been prepared.
    Preflight,
    Load {
        depth: usize,
    },
    Begin,
    End,
    Drain,
    Shutdown,
}
#[derive(Deserialize, Serialize)]
pub enum Reply {
    Ready(Identity),
    Ack,
    Sample {
        cpu: Usage,
        outcomes: Outcomes,
        wall_ns: u64,
        diagnostic: Option<super::diagnostics::Diagnostics>,
    },
    Failed(String),
}

pub struct Channel(BufReader<UnixStream>);
impl Channel {
    pub fn new(stream: UnixStream) -> Self {
        Self(BufReader::new(stream))
    }
    pub async fn send<T: Serialize>(
        &mut self,
        value: &T,
    ) -> Result<()> {
        let mut bytes = serde_json::to_vec(value)?;
        bytes.push(b'\n');
        self.0.get_mut().write_all(&bytes).await?;
        Ok(())
    }
    pub async fn receive<T: serde::de::DeserializeOwned>(
        &mut self
    ) -> Result<T> {
        let mut line = String::new();
        if self.0.read_line(&mut line).await? == 0 {
            return Err("harness control peer exited".into());
        }
        Ok(serde_json::from_str(&line)?)
    }
    pub async fn ask(
        &mut self,
        value: &Command,
    ) -> Result<Reply> {
        self.send(value).await?;
        match self.receive().await? {
            Reply::Failed(error) => Err(error.into()),
            reply => Ok(reply),
        }
    }
    pub async fn ack(
        &mut self,
        value: &Command,
    ) -> Result<()> {
        match self.ask(value).await? {
            Reply::Ack => Ok(()),
            _ => Err("unexpected harness reply".into()),
        }
    }
}

pub struct Window {
    pub cpu: Usage,
    pub counts: Outcomes,
    pub start: Instant,
    pub diagnostic: Option<super::diagnostics::Diagnostics>,
}
