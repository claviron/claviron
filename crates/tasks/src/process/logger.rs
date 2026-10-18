use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tracing::{error, warn};

use super::CommandSpec;
use crate::output::{
    LineSink, LogConfig, LogTarget, Stream, open_append, resolve_path,
};

pub(super) fn configure(
    cmd: &mut Command,
    spec: &CommandSpec,
) {
    let cwd = spec.cwd.as_deref().map(std::path::Path::new);

    match &spec.log {
        LogConfig::Single(LogTarget::Null) => {
            cmd.stdout(Stdio::null());
            cmd.stderr(Stdio::null());
        }
        LogConfig::Single(LogTarget::Console) => {
            cmd.stdout(Stdio::inherit());
            cmd.stderr(Stdio::inherit());
        }
        LogConfig::Single(LogTarget::Inherit | LogTarget::Channel(_)) => {
            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());
        }
        LogConfig::Single(LogTarget::Path(p)) => {
            let path = resolve_path(p, cwd);
            let (out, err) = open_for_both(&path);
            cmd.stdout(out);
            cmd.stderr(err);
        }
        LogConfig::Split { stdout, stderr } => {
            if let (LogTarget::Path(a), LogTarget::Path(b)) = (stdout, stderr)
                && a == b
            {
                let path = resolve_path(a, cwd);
                let (out, err) = open_for_both(&path);
                cmd.stdout(out);
                cmd.stderr(err);
                return;
            }

            match stdout {
                LogTarget::Null => {
                    cmd.stdout(Stdio::null());
                }
                LogTarget::Console => {
                    cmd.stdout(Stdio::inherit());
                }
                LogTarget::Inherit | LogTarget::Channel(_) => {
                    cmd.stdout(Stdio::piped());
                }
                LogTarget::Path(p) => {
                    let path = resolve_path(p, cwd);
                    match open_append(&path) {
                        Ok(f) => {
                            cmd.stdout(Stdio::from(f));
                        }
                        Err(err) => {
                            error!(
                                path = %path.display(),
                                error = %err,
                                "failed to open stdout log file; writing to the console"
                            );
                            cmd.stdout(Stdio::inherit());
                        }
                    }
                }
            }

            match stderr {
                LogTarget::Null => {
                    cmd.stderr(Stdio::null());
                }
                LogTarget::Console => {
                    cmd.stderr(Stdio::inherit());
                }
                LogTarget::Inherit | LogTarget::Channel(_) => {
                    cmd.stderr(Stdio::piped());
                }
                LogTarget::Path(p) => {
                    let path = resolve_path(p, cwd);
                    match open_append(&path) {
                        Ok(f) => {
                            cmd.stderr(Stdio::from(f));
                        }
                        Err(err) => {
                            warn!(
                                path = %path.display(),
                                error = %err,
                                "failed to open stderr log file; writing to the console"
                            );
                            cmd.stderr(Stdio::inherit());
                        }
                    }
                }
            }
        }
    }
}

fn open_for_both(path: &std::path::Path) -> (Stdio, Stdio) {
    match open_append(path) {
        Ok(f1) => {
            if let Ok(f2) = f1.try_clone() {
                return (Stdio::from(f1), Stdio::from(f2));
            } else {
                match open_append(path) {
                    Ok(f2) => return (Stdio::from(f1), Stdio::from(f2)),
                    Err(err) => {
                        warn!(
                            path = %path.display(),
                            error = %err,
                            "failed to open duplicated task log file; writing to the console"
                        );
                    }
                }
            }
        }
        Err(err) => {
            warn!(
                path = %path.display(),
                error = %err,
                "failed to open task log file; writing to the console"
            );
        }
    }
    (Stdio::inherit(), Stdio::inherit())
}

/// Read the child's piped streams line by line into the server's log or a
/// channel. Called right after spawn; each reader ends when its stream does.
pub(super) fn attach(
    child: &mut Child,
    spec: &CommandSpec,
    task: &str,
) {
    let (stdout, stderr) = match &spec.log {
        LogConfig::Single(target) => (target, target),
        LogConfig::Split { stdout, stderr } => (stdout, stderr),
    };
    if let (true, Some(pipe)) = (lines(stdout), child.stdout.take()) {
        tokio::spawn(read_lines(
            pipe,
            LineSink::for_target(stdout, task, Stream::Stdout),
        ));
    }
    if let (true, Some(pipe)) = (lines(stderr), child.stderr.take()) {
        tokio::spawn(read_lines(
            pipe,
            LineSink::for_target(stderr, task, Stream::Stderr),
        ));
    }
}

fn lines(target: &LogTarget) -> bool {
    matches!(target, LogTarget::Inherit | LogTarget::Channel(_))
}

async fn read_lines(
    pipe: impl AsyncRead + Unpin,
    sink: LineSink,
) {
    let mut reader = BufReader::new(pipe);
    let mut line = Vec::new();
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line).await {
            Ok(0) => break,
            Ok(_) => sink.emit(
                String::from_utf8_lossy(&line).trim_end_matches(['\r', '\n']),
            ),
            Err(error) => {
                warn!(%error, "task output stream failed");
                break;
            }
        }
    }
}
