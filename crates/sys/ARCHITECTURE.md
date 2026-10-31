# claviron-sys Architecture

`sys` contains small OS-facing primitives shared by runtime
crates.

## Role

The crate keeps low-level platform work out of higher-level services:

- TCP/UDP socket creation helpers
- socket options and bind behavior
- physical short-hostname discovery
- process RSS/system memory helpers
- stable process identity, liveness waiting, and exact-process signaling
- non-blocking named process exclusion with an owned OS lock
- platform-specific implementations through `nix`, `/proc`, sysctl, or
  libproc where needed

It is a helper crate, not a runtime provider.

## Boundary

Use this crate when a service needs OS behavior that should be consistent
across Claviron transports. Do not put HTTP, TLS, PHP, or app policy here.

Keep APIs narrow and boring. This crate should make platform work less
surprising, not become a second runtime abstraction layer.

`short_hostname()` reads the kernel hostname and removes its DNS suffix. It is
the shared primitive for process identity and host-specific boot configuration;
the selection or precedence of configuration files remains server policy.

Socket builders return `std::io::Result` for OS I/O failures and invalid socket
arguments. `sys` therefore has no dependency on the runtime's
registry error model, keeping the dependency direction available to runtime
state.

TCP/UDP builders require an explicit `PortSharing` choice. `Shared` enables
`SO_REUSEPORT`; `Exclusive` disables it. TCP keeps `SO_REUSEADDR` for restart
after TIME_WAIT. Exclusive UDP disables `SO_REUSEADDR` too, because it can
permit competing live datagram binds independently of `SO_REUSEPORT`.
The owning service maps runtime roles to this policy; sys imports no runtime
state and does not infer worker count or fleet geometry.

TCP additionally requires a caller-supplied `NonZeroU32` backlog. Conversion
to the signed socket API is checked before creating/binding any socket;
overflow returns `io::ErrorKind::InvalidInput`. Sys chooses no backlog default,
does not derive one from descriptor limits or worker counts, and does not read
environment variables. HTTP configuration owns the declared default and the
caller passes its resolved value. The kernel may cap that requested queue.

`net::classify_accept_error` returns a small `AcceptErrorClass`: transient
peer/network failure, OS resource pressure, or unrecoverable local failure.
Symbolic platform errno handling stays here (including Linux pending-network
errors), rather than leaking libc constants into Hyper. The caller owns retry
timers, logging and service shutdown; sys neither sleeps nor spawns recovery
tasks. Normal tests inject error values without exhausting real descriptors.

`rss::read_rss` accepts the current process or an explicit PID. Linux
uses `/proc`, FreeBSD uses `sysctl`, and macOS uses
`proc_pid_rusage(RUSAGE_INFO_V2)`. Higher-level process managers may use
that primitive for their own child processes without importing metrics
or transport state.

`process::identity` pairs a PID with the process start stamp reported by the
kernel, avoiding accidental attachment or signaling after PID reuse.
`process::ProcessHandle` waits for that incarnation and can signal it. Linux
uses a pidfd for event-driven exit readiness and race-free
`pidfd_send_signal`. FreeBSD registers `EVFILT_PROC/NOTE_EXIT` in a private
kqueue before rechecking the start identity, then awaits the queue through
Tokio readiness. The exit event stays unread so concurrent, cancelled and later
waiters observe the same terminal state without reaping the child. Zombie
process records are no longer live identities. macOS retains its polling
fallback. Both platforms' numeric `kill(2)` fallback still has the OS API's
narrow check-to-signal PID-reuse race. The higher-level
meaning of attach, restart, or detach remains the owning service's policy, not
`sys` policy.

`process::ProcessLock` implements cooperative host-local named exclusion with
`flock(LOCK_EX | LOCK_NB)`. The RAII guard owns a close-on-exec descriptor;
drop or process death releases the lock, but the empty rendezvous file is never
unlinked. No PID-file contents, liveness polling, stale-file deletion or time
lease participates in acquisition. Errors retain `io::ErrorKind`, including
`WouldBlock` for contention. Role selection and lifecycle ownership stay in
the caller.

Names are BLAKE3-encoded into the fixed `/tmp/claviron-process-<hash>.lock`
namespace on macOS, Linux and FreeBSD. This path is part of the local named-lock
protocol, not a per-application temp directory: UID, `TMPDIR`, cwd, config path
and binary location must not partition uniqueness. New empty files are readable
across UIDs; existing files are opened read-only without truncation or permission
changes. Symlinks, hardlinks and non-regular files are rejected, as are observed
inode replacement races. Permission errors fail closed. All participants must
share this local filesystem namespace and must exclude these files from live
cleanup; containers/systemd private-tmp mounts are separate domains. The API is
not a cross-container coordinator or a boundary against a user who can delete
or replace the lock inode. It neither detects nor terminates orphan children.
