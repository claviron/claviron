use std::{env, io};

#[cfg(feature = "pipeline")]
pub mod pipeline;
mod system;

pub use system::{SystemLog, SystemLogConfig, SystemLogFormat};

/// Listen for [`ReopenLogs`](claviron_runtime::ReopenLogs) and reopen this
/// process's system log file. Registered once, before runnables start.
pub fn register(
    state: &claviron_runtime::AppState
) -> claviron_runtime::registry::Result<()> {
    state.events().listen::<claviron_runtime::ReopenLogs, _>(|_, _| async {
        system::reopen();
        Ok(())
    })
}

use claviron_runtime::RunMode;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::LocalTime;
use tracing_subscriber::fmt::{self, FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

type Timer = LocalTime<time::format_description::well_known::Rfc3339>;

/// Custom Formatter to prepend role
/// Handles: Time, Level, Target, Role -> delegates Message to inner
struct RoleEventFormat<F> {
    run_mode: RunMode,
    inner: F,
    timer: Option<Timer>,
    ansi: bool,
}

impl<S, N, F> FormatEvent<S, N> for RoleEventFormat<F>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    F: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &fmt::FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        // 1-3. Time/Level/Target, for a line read on its own (console, system
        // log file). Journald records time, unit and pid itself, and gets
        // neither. `ansi` only colours what is written.
        if let Some(timer) = &self.timer {
            use tracing_subscriber::fmt::time::FormatTime;
            let level = *event.metadata().level();
            let target = event.metadata().target();
            if self.ansi {
                write!(writer, "\x1b[2m")?; // Dim
                timer.format_time(&mut writer)?;
                write!(writer, "\x1b[0m ")?;
                let color_code = match level {
                    tracing::Level::TRACE => "\x1b[35m", // Purple
                    tracing::Level::DEBUG => "\x1b[34m", // Blue
                    tracing::Level::INFO => "\x1b[32m",  // Green
                    tracing::Level::WARN => "\x1b[33m",  // Yellow
                    tracing::Level::ERROR => "\x1b[31m", // Red
                };
                write!(writer, "{}{:<5}\x1b[0m ", color_code, level.as_str())?;
                write!(writer, "\x1b[2m{}:\x1b[0m ", target)?;
            } else {
                timer.format_time(&mut writer)?;
                write!(writer, " {:<5} {}: ", level.as_str(), target)?;
            }
        }

        // 4. Role
        match self.run_mode {
            RunMode::Worker(worker_id) => {
                if self.ansi {
                    write!(
                        writer,
                        "\x1b[2m[Worker #{}]\x1b[0m ",
                        worker_id.raw()
                    )?;
                } else {
                    write!(writer, "[Worker #{}] ", worker_id.raw())?;
                }
            }
            RunMode::Master => {
                if self.ansi {
                    write!(writer, "\x1b[2m[Master]\x1b[0m ")?;
                } else {
                    write!(writer, "[Master] ")?;
                }
            }
            RunMode::Standalone => {}
        }

        // 5. Message (inner format handles the rest)
        self.inner.format_event(ctx, writer, event)
    }
}

/// The system log file's layer: the console's line, without colour.
fn system_log_layer<S>(
    run_mode: RunMode,
    system_log: Option<SystemLog>,
) -> Option<impl tracing_subscriber::Layer<S>>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    let system_log = system_log?;
    let format = fmt::format()
        .without_time()
        .with_target(false)
        .with_level(false)
        .with_ansi(false);
    Some(
        fmt::layer()
            .with_writer(system_log.writer())
            .with_ansi(false)
            .event_format(RoleEventFormat {
                run_mode,
                inner: format,
                timer: Some(LocalTime::rfc_3339()),
                ansi: false,
            }),
    )
}

/// Structured journald logger for systemd environments, and the system log
/// file beside it when `logging.yaml` declares one.
pub fn journald_logger(
    run_mode: RunMode,
    system_log: Option<SystemLog>,
) -> Result<(), Error> {
    journald_logger_with_filter(
        run_mode,
        system_log,
        env::var("RUST_LOG").ok().as_deref(),
    )
}

/// Install journald logging with directives resolved by the process host.
/// This entry does not read process env; absent or invalid directives use `info`.
pub fn journald_logger_with_filter(
    run_mode: RunMode,
    system_log: Option<SystemLog>,
    directives: Option<&str>,
) -> Result<(), Error> {
    let file = system_log_layer(run_mode, system_log);
    let env_filter = resolved_filter(directives);

    let jlayer = tracing_journald::layer()?.with_priority_mappings(
        tracing_journald::PriorityMappings {
            info: tracing_journald::Priority::Informational,
            warn: tracing_journald::Priority::Warning,
            error: tracing_journald::Priority::Error,
            debug: tracing_journald::Priority::Debug,
            trace: tracing_journald::Priority::Debug,
        },
    );

    match run_mode {
        RunMode::Standalone => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(jlayer)
                .with(file)
                .init();
        }
        RunMode::Master | RunMode::Worker(_) => {
            let role = run_mode.as_env_value();
            let worker_id = run_mode
                .worker_id()
                .map_or_else(String::new, |id| id.to_string());

            // `with_custom_fields` keeps values for the lifetime of the layer.
            // Role is already static; only the numeric worker ID needs one
            // bounded process-lifetime allocation.
            let worker_id: &'static str = Box::leak(worker_id.into_boxed_str());
            let jlayer = jlayer.with_custom_fields([
                ("ROLE", role.as_bytes()),
                ("WORKER_ID", worker_id.as_bytes()),
            ]);
            tracing_subscriber::registry()
                .with(env_filter)
                .with(jlayer)
                .with(file)
                .init();
        }
    }

    Ok(())
}
/// Console logging for interactive or non-systemd runs, and the system log
/// file beside it when `logging.yaml` declares one.
pub fn console_logger(
    run_mode: RunMode,
    system_log: Option<SystemLog>,
) -> Result<(), Error> {
    console_logger_with_filter(
        run_mode,
        system_log,
        env::var("RUST_LOG").ok().as_deref(),
    )
}

/// Install console logging with directives resolved by the process host.
/// Allows snapshot-owned `RUST_LOG` without populating libc's environment.
pub fn console_logger_with_filter(
    run_mode: RunMode,
    system_log: Option<SystemLog>,
    directives: Option<&str>,
) -> Result<(), Error> {
    let file = system_log_layer(run_mode, system_log);
    let format = fmt::format()
        .without_time()
        .with_target(false)
        .with_level(false)
        .with_ansi(true);

    let layer =
        fmt::layer().with_writer(io::stdout).event_format(RoleEventFormat {
            run_mode,
            inner: format,
            timer: Some(LocalTime::rfc_3339()),
            ansi: true,
        });

    let env_filter = resolved_filter(directives);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(layer)
        .with(file)
        .init();

    Ok(())
}

fn resolved_filter(directives: Option<&str>) -> EnvFilter {
    directives
        .and_then(|value| EnvFilter::try_new(value).ok())
        .unwrap_or_else(|| EnvFilter::new("info"))
}

pub fn client_logger() -> Result<(), Error> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let layer = fmt::layer().without_time().with_ansi(true).with_level(true);

    tracing_subscriber::registry().with(env_filter).with(layer).init();

    Ok(())
}

#[inline]
pub fn is_running_under_systemd() -> bool {
    env::var_os("JOURNAL_STREAM").is_some()
        || env::var_os("INVOCATION_ID").is_some()
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("logging configuration: {0}")]
    Config(String),
}
