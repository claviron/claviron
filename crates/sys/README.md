# claviron-sys

Small native OS primitives shared by Claviron runtime components.

This crate keeps socket options, process identity, resource observations, and
host-local exclusion below the services that use them. It has no runtime
provider or configuration-file owner and does not decide application routing,
TLS, worker placement, restart policy, or graceful shutdown.

Applications normally use `claviron::sys` through the root crate's `sys`
feature. The current native implementations target Linux, FreeBSD, and macOS;
do not assume equivalent process/socket support on every Rust target.

## Socket construction with explicit sharing

`net::create_tcp_listener` creates a nonblocking Tokio TCP listener;
`create_udp_socket` returns a nonblocking standard UDP socket for its caller's
IO driver. Both require `PortSharing::Exclusive` or `Shared`.

TCP also requires an explicit `NonZeroU32` backlog from its caller. Values above
2147483647 fail with `InvalidInput` before a socket is opened; valid values go
to `listen` without a library-selected default. The OS may cap the requested
pending-connection queue. The owning service/configuration chooses its value.

`Shared` enables `SO_REUSEPORT`; distribution among listeners is kernel-specific.
TCP keeps `SO_REUSEADDR` for restart after TIME_WAIT. Exclusive UDP disables
both reuse options so the default is not accidentally a competing live bind.
The caller maps runtime roles to this choice; sys does not infer fleet geometry.

`classify_accept_error` distinguishes transient peer/network failures,
resource pressure, and fatal local failures using platform errno knowledge.
It does not log, sleep, retry, or start a recovery task. The accepting service
owns those policies.

## Exact process incarnations

`process::identity(pid)` obtains `ProcessIdentity`: numeric PID plus a
kernel-reported start stamp. `ProcessHandle::open` checks that incarnation
before attaching. `wait` observes its exit even when the process is not a
child of the current runtime; it does not reap the child for its parent.

The platform boundary is deliberately visible:

- Linux uses pidfd readiness and `pidfd_send_signal` where supported.
- FreeBSD uses kqueue process-exit readiness, retaining the terminal event for
  concurrent, cancelled, or later waiters.
- macOS retains identity polling for exit observation.

Without a process descriptor, signaling checks the start stamp before numeric
`kill`, but retains that OS API's narrow check-to-signal PID-reuse race. A PID
and a successful earlier lookup are not a permanent ownership capability.

The opt-in `legacy-linux` feature supports kernels lacking `pidfd_open`:
child exits use SIGCHLD and adopted non-child processes use polling. Without
it, the missing kernel capability is reported explicitly rather than silently
degrading. The facade forwards this through its `legacy-linux` feature.

Waiting, signaling, attaching, and owning a process are distinct operations.
Higher-level services decide whether a process may be adopted, restarted,
terminated, or left running; obtaining a handle makes none of those decisions.

## Host-local named exclusion

`process::ProcessLock::try_acquire` attempts a nonblocking OS file lock. Keep
the returned guard for exactly the work it protects:

```rust
use std::io;
use claviron::sys::process::ProcessLock;

fn with_named_exclusion(
    name: &str,
    work: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    let _ownership = ProcessLock::try_acquire(name)?;
    work()
}
```

Contention is `io::ErrorKind::WouldBlock`. Drop or process exit releases the
lock; it is not a TTL lease, PID-file-content check, or stale-process detector.
The descriptor is close-on-exec.

Names map through BLAKE3 into the fixed
`/tmp/claviron-process-<hash>.lock` namespace, independent of UID, cwd, or
`TMPDIR`. Empty rendezvous files remain after release and must not be removed
while participants are live: unlinking permits a second lock on another inode.
Symlinks, hardlinks, non-regular files, and observed path-replacement races are
rejected; permission errors fail closed.

This is cooperative exclusion within one local filesystem namespace, not a
security boundary or cross-container coordinator. Private temporary mounts
create separate lock domains. It does not detect or kill orphan processes.

## Observations and signal vocabulary

- `short_hostname()` reads the OS hostname and removes its DNS suffix, with
  `unknown` as fallback. Configuration precedence remains with the server.
- `rss::read_rss(None)` reports current-process RSS in bytes; `Some(pid)`
  selects another process. Missing observations return `None`, not zero.
- `signal::Signal` names a bounded set of Unix signals. `ServerSignals`
  validates distinct operator mappings for reload, reopen, and restart; it
  does not install handlers or send signals by itself.

OS failures remain `std::io` results or the primitive's documented optional
observation, not HTTP status codes or runtime registry errors.

## Related crates

- [Runtime](../runtime/README.md): lifecycle and shared process context.
- [Supervisor](../supervisor/README.md): process ownership and fleet supervision.
- [Tasks](../tasks/README.md): external command execution and exit policy.
- [Hyper](../edge/src/tcp/mod.rs) and [H3](../edge/src/quic/mod.rs): transport listeners above
  these socket primitives.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
