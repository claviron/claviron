use std::{
    ffi::OsString,
    io::{BufRead, BufReader, Write},
    net::SocketAddr,
    process::{Child, ChildStdin, ChildStdout, Command as Process, Stdio},
};

use super::{Command, Reply, Result, allocation::Allocations, cpu::Usage};

pub(super) struct Server {
    child: Option<Child>,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
    pub(super) address: SocketAddr,
}

impl Server {
    /// Start this executable again as one serving process. The arguments
    /// name its owner, so a control can run beside the measured target.
    pub(super) fn spawn(arguments: Vec<OsString>) -> Result<Self> {
        let mut child = Process::new(std::env::current_exe()?)
            .args(arguments)
            .arg("--server")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = child.stdin.take().expect("piped child stdin");
        let mut output =
            BufReader::new(child.stdout.take().expect("piped child stdout"));
        let ready = read(&mut output);
        let address = match ready {
            Ok(Reply::Ready(address)) => address,
            other => {
                drop(input);
                let status = child.wait()?;
                let reason = match other {
                    Ok(reply) => format!("unexpected reply {reply:?}"),
                    Err(error) => error.to_string(),
                };
                return Err(format!(
                    "server did not become ready ({status}): {reason}"
                )
                .into());
            }
        };
        Ok(Self { child: Some(child), input: Some(input), output, address })
    }

    pub(super) fn pid(&self) -> u32 {
        self.child.as_ref().expect("server child").id()
    }

    fn request(
        &mut self,
        command: Command,
    ) -> Result<Reply> {
        write(self.input.as_mut().expect("running server stdin"), &command)?;
        read(&mut self.output)
    }

    pub(super) fn begin(&mut self) -> Result<()> {
        match self.request(Command::Begin)? {
            Reply::Begun => Ok(()),
            _ => Err("unexpected CPU begin reply".into()),
        }
    }

    pub(super) fn end(&mut self) -> Result<Usage> {
        match self.request(Command::End)? {
            Reply::Used(usage) => Ok(usage),
            _ => Err("unexpected CPU end reply".into()),
        }
    }

    pub(super) fn begin_count(&mut self) -> Result<()> {
        match self.request(Command::BeginCount)? {
            Reply::Counting => Ok(()),
            _ => Err("unexpected allocation count begin reply".into()),
        }
    }

    pub(super) fn end_count(&mut self) -> Result<Allocations> {
        match self.request(Command::EndCount)? {
            Reply::Allocated(allocations) => Ok(allocations),
            _ => Err("unexpected allocation count end reply".into()),
        }
    }

    pub(super) fn shutdown(mut self) -> Result<()> {
        if !matches!(self.request(Command::Shutdown)?, Reply::Stopped) {
            return Err("unexpected server shutdown reply".into());
        }
        drop(self.input.take());
        let status = self.child.take().expect("server child").wait()?;
        if !status.success() {
            return Err(format!("server exited with {status}").into());
        }
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if let Some(mut input) = self.input.take() {
                let _ = write(&mut input, &Command::Shutdown);
            }
            let _ = child.wait();
        }
    }
}

pub(super) fn write(
    writer: &mut impl Write,
    value: &impl serde::Serialize,
) -> Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn read(reader: &mut impl BufRead) -> Result<Reply> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Err("server closed its control channel".into());
    }
    Ok(serde_json::from_str(&line)?)
}
