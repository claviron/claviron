#[cfg(any(
    all(feature = "postgres", feature = "hosting"),
    feature = "tasks"
))]
use std::sync::Arc;

use claviron_runtime::{AppState, ProcessStarted, Runtime};
#[cfg(feature = "supervisor")]
use claviron_supervisor::{WorkersDraining, WorkersExited, signals};
use tracing::error;

use super::Result;
#[cfg(not(feature = "supervisor"))]
use super::signals;

pub async fn start_services(
    state: AppState,
    operator_signals: claviron_sys::signal::ServerSignals,
) -> Result<()> {
    // What the completion lines below report. It starts here rather than at
    // provider boot on purpose: acquiring the instance lock and resetting
    // shared rings are part of how long a process takes to be up, and a number
    // that left them out would be the wrong number to compare across restarts.
    let booted_at = std::time::Instant::now();

    // Acquire before any provider boot or shared-ring reset, including callers
    // that enter through bootstrap directly rather than Server::serve().
    let _instance_lock = super::instance::acquire(&state)?;
    let is_master = state.is_master();
    let is_worker = state.is_worker();

    // Reload hub (single instance)
    let registry = state.registry();
    let mut runtime = Runtime::default();
    if let Some(dispatcher) =
        registry.resolve::<claviron_orbit::placement::OrbitDispatcher>()
    {
        runtime = runtime.with_dispatcher(dispatcher)?;
    }

    // The runtime owns service generations; transports and future management
    // adapters resolve only this command capability, never duplicate runtime
    // state. Register it before provider boot so it is available throughout
    // the process lifecycle.
    registry.insert(runtime.manager())?;
    claviron_orbit::service::register(&state)?;

    // `supervisor.yaml`'s `signals`, when written, over the binary's choice —
    // read here because the listeners go in before any provider boots.
    #[cfg(feature = "supervisor")]
    let operator_signals = if is_worker {
        operator_signals
    } else {
        claviron_supervisor::configured_signals(&state)
            .map_err(|error| {
                claviron_runtime::registry::Error::msg(error.to_string())
            })?
            .unwrap_or(operator_signals)
    };

    // Install signal handlers before provider boot so startup hangs remain interruptible.
    #[cfg(feature = "supervisor")]
    let signal_handles = if is_worker {
        signals::spawn_worker_signal_handlers(state.clone())?
    } else {
        signals::spawn_main_signal_handlers(state.clone(), operator_signals)?
    };
    #[cfg(not(feature = "supervisor"))]
    let signal_handles =
        signals::spawn_main_signal_handlers(state.clone(), operator_signals)?;

    // The DbService → HostingService chain used to be hand-called here, ahead
    // of `boot_all`. It is a dependency now: `HostingService` names
    // `DbService` in its `Deps`, and `boot_all` boots what a component
    // depends on first. Every component states its requirements the same way.
    //
    // The sequence below stays hand-written rather than `Runtime::run`: the
    // process environment is scrubbed between validation and spawning, and a
    // master reaps its workers between shutdown and drain.

    runtime = with_http_services(runtime);
    #[cfg(feature = "config-watch")]
    runtime.watch_configurations(&state)?;
    if let Err(e) = runtime.configure(&state).await {
        error!("failed to configure services: {}", e);
        return Err(e.into());
    }

    if let Err(e) = registry.boot_all(&state).await {
        error!("failed to boot services: {}", e);
        return Err(e.into());
    }

    let validation = registry.validate_all(&state)?;
    for failure in validation.failures() {
        error!(
            component = failure.component(),
            error = %failure.error(),
            "provider '{}' validation failed: {}",
            failure.component(),
            failure.error()
        );
    }
    if !validation.is_valid() {
        return Err(claviron_runtime::registry::Error::msg(format!(
            "{} provider validation(s) failed",
            validation.failed_count()
        ))
        .into());
    }

    // Worker-level env hygiene. `state.env_snapshot()` was captured
    // at `State::initialize`, so every consumer (boot AND reload
    // path) already resolves `${ENV_KEY}` from the snapshot. From
    // this point libc `environ` carries only OS / runtime essentials.
    //
    // Master is excluded: worker commands inherit its full process
    // environment. Scrubbing on master would leave workers without their
    // boot-time secrets. Standalone and worker processes both serve traffic
    // and own the PHP runtime — they scrub here.
    if !is_master {
        claviron_runtime::env::scrub_process_env(
            claviron_runtime::env::RUNTIME_ESSENTIALS,
        );
    }

    // Start providers that expose Runnable capability.
    let spawned_runnable =
        runtime.spawn_all(registry.as_ref(), state.clone())?;
    tracing::debug!("🚀 spawned runnable services count={}", spawned_runnable);

    {
        // The duration goes inside the message, not beside it as a field: a
        // journal reader sees the line, not the structured pairs.
        let boot_ms = booted_at.elapsed().as_millis();
        if is_master {
            tracing::debug!(
                "✅ Master startup completed in {boot_ms} ms: control-plane services are up; waiting for shutdown while workers serve traffic"
            );
        } else if is_worker {
            tracing::debug!(
                "✅ Worker startup completed in {boot_ms} ms: serving traffic (worker {})",
                state
                    .worker_id()
                    .map_or_else(|| "?".to_owned(), |id| id.to_string())
            );
        } else {
            tracing::debug!(
                "✅ Master startup completed in {boot_ms} ms: standalone mode (master serves traffic)"
            );
        }
    }

    // Local, identity-free milestone in every mode. The supervisor's worker
    // adapter carries a separate identity-bearing acknowledgement over Orbit.
    // Runnables have been submitted; their individual readiness is separate.
    if !state.is_shutting_down() {
        state.events().emit(ProcessStarted);
    }

    let runtime_result = runtime.wait_until_shutdown(&state).await;
    if let Err(e) = &runtime_result {
        error!("runtime loop exited with error: {e}");
    }

    // Ensure all lifecycle listeners observe shutdown and can exit cleanly.
    if !state.is_shutting_down() {
        state.initiate_shutdown();
    }

    // Stop reload listener, then wait for shutdown listener to exit.
    signal_handles.abort_reload();
    signal_handles.wait_shutdown().await;

    // Worker topology marker (supervisor providers are booted via Bootable::boot).
    #[cfg(feature = "supervisor")]
    {
        if is_master {
            // Master services own shared control-plane resources that
            // worker processes may still be using. Drain and reap the
            // worker fleet first; only then drain runnables and run
            // the provider finalizers.
            use claviron_supervisor::WorkerSupervisor;
            if let Some(supervisor) = registry.resolve::<WorkerSupervisor>() {
                supervisor.shutdown_workers(&state).await; // Send SIGTERM and then SIGKILL to all workers (sets the shutdown flag)
                supervisor.wait_workers().await; // Wait for a graceful exit
            } else {
                error!(
                    "❌ WorkerSupervisor not found during shutdown; skipping worker termination"
                );
            }
        }
    }

    let drain_result = runtime.drain().await;
    if let Err(e) = &drain_result {
        error!("runtime drain failed: {e}");
        runtime.abort_and_drain().await;
    }

    // Runnables have drained (each run() future owns its own graceful
    // teardown); release the remaining non-running resources.
    let finalization = registry.finalize_all(&state).await?;
    #[cfg(feature = "supervisor")]
    let mut failed_finalizers = 0usize;
    for failure in finalization.failures() {
        #[cfg(feature = "supervisor")]
        {
            failed_finalizers += 1;
        }
        error!(
            component = failure.component(),
            error = %failure.error(),
            "provider '{}' finalization failed: {}",
            failure.component(),
            failure.error()
        );
    }

    // Final worker notification: write synchronously to Orbit, since a local
    // event-forwarding task may not get another turn before process exit.
    #[cfg(feature = "supervisor")]
    if is_worker {
        if let Err(error) =
            claviron_supervisor::publish_worker_teardown_completed(
                &state,
                failed_finalizers,
            )
        {
            tracing::warn!(%error, "failed to publish worker teardown completion");
        }
    }

    runtime_result?;
    drain_result?;
    if finalization.failures().is_empty() {
        if is_worker {
            tracing::info!(
                "✅ worker #{} shutdown gracefully",
                state.worker_id().unwrap_or(0)
            );
        } else {
            tracing::info!("🏁 {} shutdown gracefully 🎉", state.name());
        }
    }
    Ok(())
}

pub fn register(state: &AppState) -> Result<()> {
    #[cfg(any(all(feature = "tls", feature = "postgres"), feature = "tasks"))]
    let registry = state.registry_ref();
    #[cfg(feature = "tls")]
    claviron_tls::resolver::register(state)?;

    #[cfg(feature = "supervisor")]
    {
        use claviron_supervisor::Error;

        // These local observers share the runtime's single event dispatcher task.
        // Register before provider boot and runnable submission can emit milestones.
        state
            .events()
            .listen::<claviron_supervisor::WorkersBootConfirmed, _>(
                |_, event| async move {
                    tracing::debug!(
                        workers = event.count,
                        "✅ Worker fleet boot completed"
                    );
                    Ok(())
                },
            )
            .map_err(Error::Bootstrap)?;
        state
            .events()
            .listen::<WorkersDraining, _>(|_, _| async {
                tracing::debug!("Worker fleet draining");
                Ok(())
            })
            .map_err(Error::Bootstrap)?;
        state
            .events()
            .listen::<WorkersExited, _>(|_, _| async {
                tracing::debug!("Worker fleet exited");
                Ok(())
            })
            .map_err(Error::Bootstrap)?;
    }
    state
        .events()
        .listen::<claviron_runtime::ShutdownInitiated, _>(|_, _| async {
            tracing::debug!("Process shutdown initiated");
            Ok(())
        })
        .map_err(|error| {
            claviron_runtime::registry::Error::msg(error.to_string())
        })?;

    // Cert / cert-alias live updates over postgres NOTIFY/LISTEN. Runs in
    // EVERY process (master + workers + standalone) — `CertResolver` is
    // the system's single source of TLS truth and stays in lock-step
    // across the cluster regardless of role. Filters per-event by own
    // `server_id` via the `hosting.certificate_server` pivot, so cert
    // changes destined for other servers are no-ops here.
    // With hosting data in SQLite there is no NOTIFY channel; the listener
    // sees the profile and stays idle.
    #[cfg(all(feature = "postgres", feature = "hosting"))]
    registry
        .register(Arc::new(claviron_tls::CertificateListener::default()))?;

    claviron_orbit::register(state)?;

    // The system log file reopens on `ReopenLogs`, which the operator's
    // reopen signal emits in the master and Orbit carries to each worker.
    claviron_logging::register(state)?;

    // Declared log channels, when the config directory has `logging.yaml`.
    #[cfg(feature = "log-pipeline")]
    claviron_logging::pipeline::register(state)?;

    claviron_app::register(state)?;
    #[cfg(feature = "supervisor")]
    claviron_app::fleet::register(state)?;

    #[cfg(feature = "supervisor")]
    claviron_supervisor::register(state)?;

    #[cfg(feature = "db")]
    claviron_db::register(state)?;

    // HostingService is the sole owner of this instance's `server_id`, resolved from the immutable
    // process-family name supplied by ServerOptions (or the binary-name
    // default) via `DbService`. Required by hosting-aware providers
    // (CertResolverProvider's DB cert merge, vhost alias listener, …).
    #[cfg(feature = "hosting")]
    claviron_hosting::register(state)?;

    #[cfg(feature = "tasks")]
    claviron_tasks::register(state)?;

    // Lua scripts reach the fleet's cells, whether or not an engine that
    // registers them too is built in.
    #[cfg(feature = "lua")]
    claviron_orbit::register_cells(state)?;
    // And streams between two scripts, or a script and anything else holding
    // the far side.
    #[cfg(feature = "lua")]
    claviron_orbit::register_streams(state)?;
    // Applications `lua.yaml` declares, loaded in each worker.
    #[cfg(feature = "lua")]
    claviron_lua::serve::register(state)?;
    // Application events reach Lua scripts from the whole fleet.
    #[cfg(feature = "lua")]
    claviron_lua::events::register(state)?;
    // …and the fleet's invocations, sent and served.
    #[cfg(feature = "lua")]
    claviron_orbit::register_invoke(state)?;
    // `lua.yaml`'s search path for `require`.
    #[cfg(feature = "lua")]
    claviron_lua::config::register(state)?;

    // `lua:` tasks: installed on the task service just registered.
    #[cfg(all(feature = "lua", feature = "tasks"))]
    claviron_lua::task::register(state)
        .map_err(claviron_runtime::registry::Error::msg)?;

    // The Rust side of invocations: one `invoke-router` task in the fleet
    // serves every operation registered on `InvocationRouter`, answering on
    // the application event bus. Registered here because the router (app)
    // and the task service (tasks) do not know each other.
    //
    // The ring itself is registered by whoever runs code on it, the PHP embed
    // and the JS application services. A build with neither has no transport
    // and nothing to route, so the task is not registered rather than left
    // failing on every retry with "transport is unavailable".
    #[cfg(feature = "tasks")]
    if let (Some(tasks), Some(router), Some(_transport)) = (
        registry.resolve::<claviron_tasks::service::TaskService>(),
        registry.resolve::<claviron_app::invoke::InvocationRouter>(),
        registry.resolve::<claviron_orbit::InvocationProvider>(),
    ) {
        let defaults = claviron_tasks::TaskConfig::from_yaml(
            "timing: { kind: daemon }\nscope: workers\ndispatch: one\nattempts: unlimited\nbackoff: 1s\ngraceful_timeout: 15s\n"
        )
        .map_err(|error| {
            claviron_runtime::registry::Error::msg(format!("{}: {error}", claviron_app::invoke::TASK))
        })?;
        tasks.register_builtin(
            claviron_app::invoke::TASK,
            defaults,
            move |state: AppState, cancellation: tokio_util::sync::CancellationToken| {
                let router = Arc::clone(&router);
                async move {
                    router.serve(state, cancellation).await.map_err(claviron_tasks::Error::other)
                }
            }
        );
    }

    #[cfg(feature = "edge")]
    {
        // Reject unsupported transports before publishing a config candidate,
        // on master as well as workers, at both boot and reload.
        claviron_edge::register_http_config_with_capabilities(
            state,
            claviron_http::config::ListenerCapabilities {
                tls: cfg!(feature = "tls"),
                h3: cfg!(feature = "http3"),
            },
        )?;
    }

    #[cfg(feature = "upstream")]
    claviron_upstream::register(state)?;

    #[cfg(feature = "php_fcgi")]
    claviron_php_fcgi::register(state)?;

    #[cfg(feature = "php_embed")]
    claviron_php_embed::register(state)?;

    #[cfg(feature = "cache")]
    claviron_cache::register(state)?;

    #[cfg(feature = "broadcast")]
    claviron_broadcast::register(state)?;

    #[cfg(feature = "edge")]
    claviron_edge::register(state)?;
    // The static file cache learns about changed files from a reload, not
    // from the hot path. Application servers can own it without an edge.
    #[cfg(feature = "files")]
    claviron_files::register(state)?;

    #[cfg(feature = "geoip")]
    claviron_geoip::register(state)?;

    Ok(())
}

/// Compose HTTP transports through their standard service registration contract.
/// Both normal startup and configuration checks use this same declaration.
/// Recording only: `Runtime::configure` applies the registrations.
pub(super) fn with_http_services(runtime: Runtime) -> Runtime {
    #[cfg(all(feature = "edge", feature = "tls"))]
    let runtime = runtime.with_services::<(
        claviron_hyper::HttpServer,
        claviron_hyper::HttpsServer,
    )>();
    #[cfg(all(feature = "edge", not(feature = "tls")))]
    let runtime = runtime.with_services::<claviron_hyper::HttpServer>();
    #[cfg(all(feature = "edge", feature = "http3"))]
    let runtime = runtime.with_services::<claviron_h3::H3Server>();
    runtime
}
