# claviron-php-fcgi — Architecture

The PHP runner that talks to **PHP-FPM over FastCGI**. Sister crate to
`php-embed` (in-process libphp.so SAPI); the two share the
native request/body and adapter contracts while owning distinct connection,
execution and metrics machinery. Neither engine owns an Axum binding. Host
composition selects runner: fcgi or runner: script at preparation.

What it adds beyond a vanilla FCGI client:

- A **modern keep-alive pool** with three-tier sizing (`min_live` /
  `max_idle` / `max_live`), proactive replenisher, and a
  **live-client guarantee**: every connection in the idle queue has
  proven a successful FPM round-trip at least once in its lifetime —
  a `ping.path` round-trip when the replenisher dialled it, a real
  request when an acquire-time dial did. TCP-connect-only is not
  enough either way.
- A **streaming detector** that picks Buffered vs Streaming mode
  per-request (SSE Accept, multipart, content-length threshold,
  unknown-length body methods) — same body, different transport
  shape, no per-route operator config.
- A **one-shot fallback** when no `pool:` block is configured —
  fresh dial per request. Useful for low-traffic operator scripts
  that don't deserve a warm pool but must remain serviceable.
- A **bypass adapter** (`PhpFpmAdapter`) for hosts whose entire
  business is "send this to PHP" — bypasses the axum router,
  calls FPM directly, optionally captures `(request_body,
  response_status, response_body)` triples for B2B / FW-secured
  termination middleware via `tokio::sync::mpsc::Sender<HttpExchange>`.
- A **periodic FPM `/status` scraper** (single source per FPM, runs
  on master + standalone) that publishes
  `claviron_fpm_*{pool=...}` gauges to Prometheus alongside the
  pool-side `claviron_fcgi_*` counters that workers emit.

The crate is **lazy by design** — pools defined in `fpm.yaml` but
never touched by request traffic stay un-instantiated forever; no
idle FCGI sockets, no housekeep noise, no per-worker overhead for
unused entries. First request to a pool name materialises it.

---

## 1. Position in the request pipeline

Host admission -> Rust Application -> Static -> TryFiles -> prepared PHP owner
-> PhpFpmAdapter -> native script resolution -> FPM connection/lease -> reply.
The existing host compiler temporarily bridges this native owner until the
remaining host adapters migrate; the FCGI crate itself has no Axum dependency,
feature or router binding.

PhpFpmAdapter::try_handle(self, request) receives a caller-selected runtime.
Script-resolution Unhandled returns the same unread Request<RouteBody> before
connection acquisition. Handled responses retain the existing execution,
streaming, retry, capture, cancellation and pool-lease behavior. Prefix ordering
and the choice among runner types belong to Edge's cold Embed preparation.
The terminal handle(&self, request) retains 404 on prefix/resolution misses.

## 2. Configuration sources

Two YAMLs cooperate:

### 2.1 `config/fpm.yaml` — process-wide FPM catalog

```yaml
metrics:
  enabled: true
  dispatch_policy:          # singleton collector ownership, not a client pool
    capacity: 1
    lease_ttl: "20s"
    renewal_interval: "5s"
  interval: "60s"          # FPM /status scrape cadence
  publish_interval: "1s"   # worker FCGI Orbit publication cadence
  max_age: "5s"            # fleet sample freshness bound

housekeep:
  enabled: true
  interval: "15s"          # stale sweep; ≤ the tightest idle_timeout / 2

fpm:
  default:
    unix: /var/run/php/php-fpm.sock          # OR `tcp: 127.0.0.1:9000`
    connect_timeout: "2s"                    # entry-level: one-shot needs it too
    exec_timeout: "30s"                      # entry-level: one-shot needs it too
    head_timeout: "30s"                      # wait for response headers; defaults to exec_timeout
    status_path: "/fpm-status"               # for metrics scraping (optional)
    pool:                                    # presence ⇒ keep-alive pool
      min_live: 4                            # FPM analog: pm.start_servers
      max_idle: 8                            # FPM analog: pm.max_spare_servers
      max_live: 24                           # FPM analog: pm.max_children
      ping_path: "/ping"                     # validate every fresh conn
      acquire_timeout: "5s"
      max_lifetime: "10m"
      max_requests: 5000
      idle_timeout: "60s"

  api:
    tcp: 10.0.0.5:9000
    # no `pool:` block — one-shot dial per request
    connect_timeout: "1s"
    exec_timeout: "10s"
```

`connect_timeout` and `exec_timeout` are **entry-level**, not
`pool:`-level: they apply to one-shot entries too, which have no `pool:`
block to hold them. Both `FpmClient` and `PoolConfig` parse with
`deny_unknown_fields`, so putting them in the wrong block fails the load
instead of leaving the operator reading budgets the server never applied.
The outer `fpm.yaml` container is one strict document — `fpm:`,
`housekeep:` and `metrics:` — owned by `PhpFcgiService` as a Congif
`Configurable`: prepared once before boot and in the reload batch, with
`${NAME}` expanded from the application's frozen configuration environment,
and never opened by this crate. The metrics providers and the housekeeping
task take their sections from that prepared document. An absent file is the
empty document: no pool, metrics off.

Each top-level key is an **FPM entry name** that vhost routes
reference. Two entry shapes:

- **Pooled** (`pool:` present) — keep-alive `FcgiPool` with the
  three-tier sizing described in §3.
- **OneShot** (`pool:` absent) — fresh dial, run, drop per request.
  Same `connect_timeout`/`exec_timeout` budget as pooled; just
  no slot reuse. Suitable for ops dashboards, low-traffic admin
  panels, and remote FPMs where keeping a pool warm doesn't pay off.

### 2.2 `config/vhost.yaml` (per-route) — owner of `PhpRuntime`

The vhost layer (claviron-edge → claviron-edge-config) parses
`vhost.yaml` and produces a `PhpRuntime` per route. The fcgi crate
never reads `vhost.yaml` itself — it receives an
`Arc<PhpRuntime>` from the layer above.

`PhpRuntime` carries:

- `runner: fcgi | sapi` — selects this crate vs `php-embed`.
- `path:` — URL prefix this PHP route claims.
- `dir:`, `entry_script:`, `entry_extensions:`, `indexes:`, `try_files:`,
  `validate_script:` — script resolution.
- `fpm: ["default"]` (or `["primary", "fallback"]`) — names of
  `fpm.yaml` entries this route may dispatch to. The balancer
  (§3.7) picks the most-spacious pool when the list has > 1 entry.
- `exec_mode: Buffered | Stream | Resolve` — request-time mode hint
  (Resolve = inspect headers per request, see §5).
- `requests:` and `responses:` — route overrides of the supplied `HttpPolicy`.

Same `PhpRuntime` type used by `php-embed`; the `runner`
field is the only fork point.

FastCGI HTTP execution requires the existing `HttpPolicy` request extension.
`PhpRuntime::policy` applies the PHP route's overrides; it does not read listener
or host configuration. Its effective `requests.max_body_size` is validated
inside the existing buffered collection and live STDIN projection. The
transport's HTTP body guard is not wrapped or boxed a second time. Missing policy is an
internal execution error; an explicit `off` remains valid. Oversized declared
or buffered input returns 413 before FPM receives the request. There is no
separate `post_max_size` field/default; PHP-FPM's INI limits remain independent.

The FCGI gateway also owns CGI parameter projection from Claviron's
request context. TLS is terminated before PHP-FPM sees the request, so
`ConnCtx::tls` is exported as standard CGI/mod_ssl-style params:
`HTTPS=on`, `SSL_PROTOCOL`, `SSL_CIPHER`, `SSL_CLIENT_VERIFY`,
`SSL_CLIENT_S_DN`, and `SSL_CLIENT_I_DN`.

`REQUEST_SCHEME`, `HTTPS=on|off` and `HTTP_X_FORWARDED_PROTO` come from the
accepted connection, including cleartext H1. SSL-specific params exist only
when TLS metadata exists. Shared PHP request-header filtering prevents
client `Forwarded`/`X-Forwarded-Proto` from overwriting this projection.

---

## 3. The pool model

The non-obvious part of the crate. Built around three distinct
metaphors borrowed from FPM's process manager — but applied
**client-side** (claviron → FPM), not on the FPM side itself.

### 3.1 Three-tier sizing

```
                           live (atomic counter)
        ┌──────────────────────────────────────────────────┐
        │                                                  │
        │    ╔═══════════════╗                             │
        │    ║   idle queue  ║      leased connections     │
        │    ║  ┌─────────┐  ║      ┌─────┬─────┬─────┐    │
        │    ║  │   ⚙     │  ║      │  ⚙  │  ⚙  │  ⚙  │    │
        │    ║  │   ⚙     │  ║      └─────┴─────┴─────┘    │
        │    ║  │   ⚙     │  ║                             │
        │    ║  └─────────┘  ║                             │
        │    ╚═══════════════╝                             │
        └──────────────────────────────────────────────────┘
        0      min_live      max_idle                    max_live
```

| | Meaning | FPM analog |
|---|---|---|
| `min_live` | warm minimum — replenisher proactively keeps `live` ≥ this | `pm.start_servers` / `pm.min_spare_servers` |
| `max_idle` | idle queue cap — on lease drop, conns past this close instead of returning | `pm.max_spare_servers` |
| `max_live` | the entry's ceiling — `acquire()` queues callers up to `acquire_timeout` once reached | `pm.max_children` |

The floor is on `live`, not on the idle queue: a leased connection
satisfies it, so under sustained load the queue can be empty while the
floor is met. That is what keeps an acquire from dialling once per
request, and it is why the field is `min_live` — it was called `min_idle`
until the behaviour was written down, and the name said the opposite of
what it does. The old spelling still parses (`serde(alias)`), because the
operator config is a shared mount and does not move in the same step as
the binary. `upstream` spells the same idea `min_idle` and means
the same thing by it; it should follow, so one vocabulary does not become
two.

`acquire_timeout` is the whole wait an acquire can impose. When the wait
ends in a fresh dial, that dial gets what is left of the budget rather
than starting a second one — `connect_timeout` still caps it on its own,
so a dead FPM fails fast wherever it is dialled, but the caller's total
is one number instead of a sum. A dial cut short by the acquire deadline
reports as a timeout, not as FPM refusing a connection.

### 3.1a Process-local ownership

Each worker owns its own FCGI connections. `max_live`, `max_idle` and
`min_live` are all process-local and are enforced without an Orbit pool
ledger. Orbit remains in this crate for metrics publication only; it does not
participate in connection admission, borrowing or lifetime.

Crucially, the replenisher checks **`live`**, not `idle_count`. A
leased connection is still "ours" — it returns on `Drop`. If the
floor compared against `idle_count`, every acquire would temporarily
trip the replenisher, which combined with Drop semantics
(`idle_count >= max_idle` → close on return) would churn one fresh
dial per request. Comparing against `live` means the pool only
refills when there's a **genuine shortage**: stale eviction,
`purge_idle()` after FPM restart detection, or unrecoverable lease
drops.

Bursty connections (SSE, streaming uploads, request spikes) land in
the `max_idle..max_live` zone naturally — they don't bloat the
warm pool because they close on drop instead of returning to idle,
and `live` stays ≥ `min_live` so the post-burst drain doesn't
spam-replenish.

### 3.2 The live-client guarantee

> **Every connection in the idle queue has proven a successful FPM
> round-trip at least once in its lifetime.**

This is the single most important invariant the pool offers. TCP
connect succeeding does **not** mean PHP-FPM is responsive on the
other side. We've seen all of:

- Half-open Unix sockets after FPM master restart (the listening
  socket is still bound but no worker is wired through).
- Orphan worker sockets where the worker process is gone but the
  fd lingers under FPM's `pm.process_idle_timeout` window.
- Slow-loris FPM workers that accept the TCP connection then never
  read the FCGI request frames.

The pool defends against all three by **pinging** every connection
the replenisher dials, before publishing it to the idle queue. That
is the only path that puts a connection into the idle queue without a
real request having run on it; a connection from an acquire-time dial
reaches the queue only by surviving its request. The ping is a single FCGI request to FPM's
`ping.path` (typically `/ping` returning `pong`); FPM intercepts at
the master/worker level, no PHP runtime invocation, no opcache
load, ~microsecond round-trip. A successful pong proves:

1. The FCGI socket is wired through to a live FPM worker.
2. The worker can complete a full BEGIN_REQUEST → STDIN → STDOUT →
   END_REQUEST cycle on this keep-alive conn.
3. We have not been silently disconnected by FPM's idle timeout.

When `ping_path` is unset (lazy-mode operator fallback), the first
real request becomes the canary — accepted as a tradeoff, but the
default config should always set it.

There is no separate warm-up pass. The first acquire dials, then
`maybe_spawn_replenisher` fills to `min_live` behind it; a second warm
path would only duplicate the replenisher's job and pay the ping cost
in front of the first request.

The acquire-time fresh-dial path **does not** ping. Reasoning: the
caller is already waiting for a request; adding a ping-RTT in front
of their work just for symmetry would harm latency. If FPM is dead
on this socket, the first real request surfaces it (the same
fallback used by the lazy mode). The replenisher's background dials
eat the ping cost; user-facing acquires don't.

### 3.3 Stale eviction

A connection is stale when **any** of these holds:

- `created_at + max_lifetime < now`
- `used >= max_requests`
- `last_used_at + idle_timeout < now`

Checked at three points:

1. **`acquire()`** — pop, check, drop-and-retry if stale.
2. **`Drop` on `PoolLease`** — returning conn checked before
   re-publishing.
3. **`housekeeping()` task** — periodic background sweep.

The housekeeping sweep is the **source-of-truth**: pool is the
algorithm owner, the periodic task is just a thin trigger
(``). Cadence rule: `interval`
must be `≤ idle_timeout / 2` so claviron's staleness check always
fires before FPM's `pm.process_idle_timeout` invalidates the worker
on the other side.

`fcgi_housekeep` carries its own task entry and takes that interval
from `fpm.yaml`'s `housekeep:` block — handed to it by
`PhpFcgiService::configure`, before `TaskService` boots — so the cadence
sits beside the `idle_timeout` it is bound to and the rule can be checked
when the config loads — `PhpFcgiService::rebuild` warns, naming both numbers
and how long a connection could then sit unswept. Two files could not
check each other, which is why the number is not in `tasks.yaml`; a
line there under the same name still replaces the entry whole, which
is the operator's override and the only reason to write one.

One cadence covers every pooled entry. Per-entry cadences would need a
task name each, and the sweep costs microseconds even when it runs
tighter than the slowest pool needs, so it is set from the tightest
`idle_timeout` and the rest are swept early.

### 3.4 `purge_idle()` — FPM restart recovery

On `unexpected EOF` during a request (the canonical "FPM was restarted
under us" symptom) the proxy layer reports the break to the pool. A
confirmed one drops every conn currently in the idle queue and frees
their `live` slots; leased connections are **untouched** — when they next
`Drop`, their stale check naturally fails (server-side teardown
invalidates them) so they close instead of returning. The replenisher
fires once `live < min_live`, refilling against the new FPM master.

There is no restart signal from FPM itself, so the cause is inferred from
request errors — and **two causes produce the same error**. An FPM
restart kills every child at once, so every pooled connection is dead. A
single child reaching its own `pm.max_requests` kills exactly one, and
the next pooled connection serves normally. Purging on the first break
treats the second as the first: one rotation, which is scheduled and
routine, takes a queue of healthy connections with it.

So `report_stale_keepalive()` answers with evidence rather than
suspicion. One break retires its own connection and returns
`Suspected`; the queue is dropped on a second reused connection breaking
with none having succeeded in between (`Confirmed`), and a reused
connection that completes its request clears the count — the queue
speaking for itself. The threshold is two because one break is the
connection's own evidence and two is the smallest number that can be the
queue's; higher would only make more requests pay a retry to learn what
the second already said.

The wait for that second opinion costs one request a retry, which §5.2
was already spending on a fresh connection before the client saw
anything — and only because that retry is now *actually* fresh. It used
to call `acquire()` and be handed a connection out of the same queue; it
worked because the eager purge had emptied the queue a microsecond
earlier. Take the purge away without fixing the retry and a graceful
`kill -USR2` costs one 502, which is how this was found. That is what changed the old bias — purge eagerly, false
positives are cheap — into this one: a false positive is a re-warm of the
*whole* queue against an FPM that is fine, and the failure it was
guarding against is already invisible to the client.

### 3.5 Lazy instantiation

`PhpFcgiService::get_entry(name)` is the only path into a pool:

```rust
// fast path — already built
if let Some(entry) = self.entries.read().await.get(name).cloned() {
    return Some(entry);
}
// slow path — build under write lock, double-check
```

Pools that `fpm.yaml` declares but no vhost route ever touches stay
**un-instantiated forever**. Zero idle FCGI sockets, zero housekeep
work, zero per-worker memory cost for unused entries. Touching a
pool for the first time costs one TCP/Unix dial, plus (if `min_live >
0`) the replenisher filling the floor behind it.

This matters in deployments with many declared FPM endpoints that
only some hosts actually use — the fpm.yaml is operator-shared,
the workload per host varies, lazy load means the workload chooses
which pools matter.

### 3.6 Dead on arrival — one chance, no inner loop

> **A connection that fails its ping is dropped. We do not retry the
> same socket; we do not loop trying to "fix" it.** Each fresh dial
> gets exactly one shot at proving itself live; if it fails, it
> dies and the live slot is released. Whatever made it
> dead-on-arrival is the kernel's or FPM's problem to resolve, not
> ours.

The FCGI keep-alive socket is fragile in a way that's invisible at
the TCP/Unix layer. We've seen all of:

- TCP connect succeeds, FPM master is up, but the worker that
  inherited the listener fd has just exited — the connect lands
  on a half-open socket with no peer to read.
- Unix-socket connect succeeds against an old FPM master's
  socket file that was unlink-then-rebound; the new master never
  saw our connection.
- FPM is restarting; the child that picks up our connect is in
  the brief window before it loads opcache and never finishes
  initialising for our request.
- The kernel sends RST when we read; our `accept()` sees a
  half-open conn no userspace owns.

Common signature: `ping.path → /ping → pong` round-trip fails. Our
response:

```rust
if ping fails {
    drop(conn);              // socket closes, peer side reaps
    release_slot();          // free the live counter
    return Err(...);         // surface to replenisher
}
```

**No inner retry on the same socket.** No "let's wait 100ms and try
the same conn once more". The replenisher backs off and dials a
**fresh** conn — possibly a different FPM child via the kernel's
SO_REUSEPORT-style accept distribution; that's the kernel's
diagnostic, not ours. Replenisher's exponential backoff caps at 5s
and stops once `live ≥ min_live` (or shutdown is signalled), so a
permanently-dead FPM doesn't burn CPU forever — it spins at 5s
intervals while emitting one warn log per failure. Operator's
problem to fix.

The acquire-time fresh-dial path takes the same one-shot stance
without the ping. The caller is already waiting; we don't add a
ping-RTT in front of their request. If FPM is dead on this socket,
the **first real request** surfaces it — `mark_unrecoverable()`
on the lease, `purge_idle()` to defend the rest of the pool, and
a single retry on a **different** lease (see §5.2). One retry is
the entire budget; if it fails too, the request errors out to the
client. Faster fail than a long retry chain that would still time
out somewhere later.

The contract reads, end-to-end:

| Failure point | Retry? | Where the next chance lives |
|---|---|---|
| Fresh dial in the replenisher | ❌ same conn | replenisher backoff, **new** dial |
| Ping in the replenisher | ❌ same conn | replenisher backoff, **new** dial |
| Acquire-time fresh dial | ❌ | caller surfaces `Connect` error to client |
| Buffered request, stale keepalive | ✅ once | new lease, **different** conn |
| Streaming request | ❌ | response already half-written; client retries upstream |

What this avoids:
- Hot loops that retry the same broken socket.
- Ten-second tail latencies because a dead-on-arrival conn was
  given three chances by three different layers.
- Phantom "the pool fixed itself" stories that obscure real
  operational issues — a permanently-dead FPM produces a steady
  warn-log stream until ops fixes it.

### 3.7 Multi-pool balancing

`PhpRuntime.fpm: ["primary", "fallback"]` activates the balancer
(`balancer.rs`). Per request:

1. Walk the list, resolving each name through `get_entry` — which
   **instantiates** it. This is load-bearing: `entries` is lazy and
   nothing else builds a balanced route's candidates, so ranking a map
   that no request has populated would find it empty and report the
   whole list missing.
2. Gather `free_slots = max_live - live` for pooled entries. Pick the
   entry with **highest `free_slots`**.
3. Tie-break round-robin via a process-wide `AtomicUsize` so
   equally-loaded pools alternate.

Used for:
- **Failover** — primary FPM unhealthy, secondary picks up.
- **Weighted load split** — give heavier-capacity FPM higher
  `max_live` and the balancer naturally favours it.
- **Geographic / zone affinity** when listing two FPM endpoints
  representing local + remote.

OneShot entries have no ceiling to rank, so they are not ranked: a
one-shot in the list is the **overflow** path, taken only when no pool
in the list has a slot left, or when the list names no pool at all.
"Always available, never warm" is what makes it the right spill target
and the wrong preference — ranking it first would leave the pool it was
listed beside permanently cold. With no one-shot listed, a full pool is
still picked and `acquire` queues the caller for `acquire_timeout`,
which is a wait the operator configured rather than an error.

---

## 4. The connection lifecycle

```
   FcgiEndpoint (Unix path | TCP addr)
            │
            ▼
   ┌────────────────────┐
   │  open()            │  TCP / Unix dial under connect_timeout
   │  ⤷ FcgiConnection  │  + assigned monotonic `id` from pool's atomic counter
   └────────────────────┘
            │
            ▼  ping_keepalive(client, ping_path)?     [replenisher only]
            │  └─► /ping FCGI → expect "pong"
            │
            ▼
   ┌────────────────────┐
   │  push_idle()       │  joins the idle queue, ready to lease
   └────────────────────┘
            │
            ▼  acquire() → PoolLease
   ┌────────────────────┐
   │  request execute   │  exec_buffered() / exec_streaming()
   │  conn.mark_used()  │  → used += 1, last_used_at = now
   └────────────────────┘
            │
            ▼  PoolLease::drop()
   ┌────────────────────┐
   │  reusable +        │  YES → push back if idle_count < max_idle
   │     not stale?     │  NO  → drop, release_slot()
   └────────────────────┘
```

`FcgiConnection::id` is read-only after construction. Paired with
`used` it lets observers answer "same conn?" (id stable) and "same
worker still?" (id+used both stable since last sample) — useful
for tracing across keep-alive retries.

`mark_unrecoverable()` on a `PoolLease` skips the re-publish on
drop (used by the proxy when an `unexpected EOF` proves the conn
is broken even if not stale by time).

**A streamed response returns its connection under a verdict.** The
pump reports `StreamEnd::Completed` only when the FCGI stream reached
its natural end; every other exit — the HTTP body receiver going away,
shutdown, an FPM error, headers that never arrived — is
`StreamEnd::Abandoned` and marks the lease unrecoverable. An FCGI
keep-alive socket carries one response's records until END_REQUEST and
`ResponseStream` does not drain them on drop, so a connection
re-published mid-response would hand this response's tail to whoever
leases it next. Client disconnect mid-stream is the ordinary case here
(an SSE tab closing), not an exotic one.

---

## 5. Per-request flow — `proxy.rs::try_fcgi_proxy`

```
Request<RouteBody>
    │
    │  1. URL prefix check    (runtime.path mismatch → Unhandled)
    │  2. try_files gate      (static file or non-entry ext → Unhandled)
    │  3. determine_script_name → script + path_info
    │  4. validate_script     (fs::metadata existence) → Unhandled if missing
    │  5. balancer.pick       → resolved pool name
    │  6. mode = runtime.resolve_exec_mode(&parts)
    │
    ▼
mode = Buffered                          mode = Streaming
    │                                            │
collect_body_with_limit                  no body collection
   (effective HTTP policy cap)                  │
    │                                            │
exec_buffered:                           exec_streaming:
  pool.acquire()  (or one-shot dial)       pool.acquire()
  build CGI params                         build CGI params
  send req + body                          tokio::spawn pump
  read response (full)                     attach mpsc body to RouteResponse
  release lease (reusable)                 release lease on stream end
    │                                            │
    └────────────────► RouteResponse ◄───────────┘
```

### 5.1 Mode resolution — `PhpRuntime::resolve_exec_mode`

Owned by `php` so both runners resolve a request's mode the
same way; this crate calls it and never re-derives the policy.
`runtime.resolve_exec_mode(&parts)`:

- `ExecMode::Buffered` / `ExecMode::Stream` — operator-pinned,
  short-circuits.
- `ExecMode::Resolve` — inspect headers:
  1. **`x-fastcgi-mode`** explicit override (`buffered` / `streaming`).
  2. **`Accept: …event-stream…`** — SSE; buffered would hang to
     `exec_timeout`. Force streaming.
  3. **`Content-Type: multipart/form-data`** or
     **`application/octet-stream`** — likely large upload. Force
     streaming.
  4. **`Content-Length > 64 KiB`** — threshold to avoid buffering
     uploads in RAM.
  5. **No `Content-Length` on a body-bearing method** (POST / PUT /
     PATCH) — chunked H1/H2/H3, unknown size; buffering would
     accumulate the whole upload before dispatch.
  6. Default — Buffered.

Header-driven rather than route-driven because the same script can
serve both small JSON POSTs and large file uploads; per-request
inspection costs ~10 byte-window scans on the headers.

### 5.2 Buffered path retry semantics

Buffered + pooled is **retryable on stale keepalive**, under two
conditions that the same failure answers differently.

**Did FPM say anything?** A connection wraps its stream in an observer
that records the first byte FPM writes, cleared at the start of each
request. `saw_response_byte()` false means FPM never answered, and only
then is the request sent again: if it had started answering, the request
ran, and replaying it would execute the work a second time — a second
row, a second charge — to save one error the client has already lost
either way.

The evidence is per **request**, not per connection, which a client with
a background connection driver cannot do: here `Client::execute` reads
inside the call, so between two requests on a keep-alive connection there
is no read outstanding to race the clear, and no fence is needed before
reading the flag. Kept for the connection's life instead, the first
successful response would veto every replay after it — on a connection
configured for 900 of them, that is the same as having no replay.

**Where did the connection come from?** `PoolLease::is_fresh()`. A
connection out of the idle queue that breaks says the queue is stale; a
connection dialled a moment ago that breaks says FPM is. Only the first
is evidence about the *other* connections in the queue, so only the first
purges it.

So the proxy:

1. Read both answers while nothing is still reading.
2. `mark_unrecoverable()` on the lease (skip re-publish).
3. `pool.report_stale_keepalive()` — only if the lease came from the
   idle queue, and it drops the queue only once a second reused
   connection agrees (§3.4).
4. Stop if FPM had answered; otherwise `acquire_fresh()` — a connection
   this pool has never handed out — and retry once. Not `acquire()`: the
   queue is under suspicion and has not been condemned, and the next
   connection out of it has the same provenance as the one that just
   broke. At the ceiling the fresh acquire makes its own room by retiring
   one of the suspect connections, which is the one thing that cannot
   cost a healthy one.

What remains unguarded is a script that writes nothing before dying —
output buffering makes that the common shape — where "FPM never
answered" cannot distinguish a request that never arrived from one that
ran and died silently. No client-side evidence can; it is the reason
replay is bounded to one attempt rather than made safe.

Streaming has no retry — the body is already half-streamed when
the failure surfaces. We surface the error to the client and let
them retry at a higher layer, and the connection is retired rather
than pooled (see §4).

It does have a **head bound**. `exec_timeout` is deliberately not
applied to a streamed body — an SSE stream or a large download is
meant to run long — which left no bound at all on the wait *before*
the body began, so an FPM that accepted the request and then said
nothing held the caller for ever. `head_timeout` covers that stretch
and nothing after it, defaulting to `exec_timeout`.

The pump enforces it rather than the caller: a timeout around the
caller's wait alone would leave the pump holding the lease on a
silent socket, so the slot would never come back. The deadline is
armed only while the headers are outstanding and disarms the moment
they are sent.

### 5.3 OneShot path

`FpmEntry::OneShot` bypasses pool entirely:

```
fresh dial(connect_timeout) → execute → drop
```

No keep-alive, no ping, no slot accounting. `exec_timeout` still
caps the buffered path; streaming runs to natural body end.

Use cases: low-traffic admin endpoints, ops dashboards, remote
FPMs where TCP RTT dominates and keeping a pool warm doesn't pay off.

### 5.4 A streamed reply over HTTP/3 holds an FPM child for `idle_timeout`

A streamed reply keeps its FPM child and connection until the body ends.
When the client is gone the body ends once the edge knows it: at once for
a client that closes, but for one that dies without a close only when QUIC
gives up on it — the HTTP/3 listener's `quic.idle_timeout`. Nothing on the
FastCGI side can learn it sooner; the edge in the middle has no other
signal (`crates/h3/FINDINGS.md`).

So on a listener that serves FastCGI over HTTP/3, `idle_timeout` is the
longest a dead client holds a child, and it is chosen against FPM's
capacity: how many children `pm.max_children` leaves free, times how long
each may be held. There is no switch to turn HTTP/3 streaming off; the
timeout is the setting.

---

## 6. FPM `/status` integration — the metrics provider

PHP-FPM exposes a per-pool status endpoint when `pm.status_path` is
configured. Output looks like:

```
pool:                 default
process manager:      dynamic
listen queue:         0
max active processes: 64
total processes:      24
active processes:     3
idle processes:       21
slow requests:        0
accepted conn:        12483
```

`FpmMetricsProvider` (gated on `metrics`, registered in every role) declares
fixed `Scope::Master` and `Dispatch::One`: the master or a standalone process
scrapes every FPM entry that has `status_path` configured; workers don't scrape.
The collector's One selection is provider-owned, not an operator-selectable
Each/Pool default. Its host must install a dispatcher; the collector supplies
`fpm.yaml::metrics.dispatch_policy` through `Dispatched`. Enabled metrics require
this policy; disabled/absent metrics start no collector controller. Each running
generation retains its captured lease policy until drain, while a later start
can adopt a newly prepared policy. The provider invents no lease or renewal
duration, and no service policy is needed in `orbit.yaml`.

- Scrape cadence from `fpm.yaml::metrics.interval` (default 60s), which is
  also the scrape's own budget: `collect_loop` awaits the scrape inline, so
  one unanswered `/status` would otherwise end the family for the life of
  the process — and does not need an operator setting to say so, because a
  scrape that has not answered within one interval is superseded by the
  next. A pool that does not answer publishes `claviron_fpm_up 0` rather
  than vanishing. `connect_timeout` does not cover this: the kernel accepts
  into the listen backlog whether or not an FPM child is free to read.
- Per tick: parse status and update this crate's `FPM_METRICS` store, which
  holds `claviron_fpm_*{pool=...}` gauges.
- In supervisor mode, the master publishes one keyed kind `216` Orbit row per
  FPM pool. This crate's worker family collects those rows by pool key and
  renders the canonical `FpmMetricsSnapshot` Prometheus formatter.
- In standalone mode, the same scrape stays in local `FPM_METRICS`; no Orbit
  row is published because there is no worker consumer.

The workers don't talk to `/status` themselves — single source per
FPM, run from master, prevents thundering herds against a shared
endpoint and avoids `pm.dynamic` accounting noise from the same
backend being polled N times per cluster.

Pool gauges describe the process-local pool: `claviron_fcgi_live`,
`claviron_fcgi_idle`, and `claviron_fcgi_max_live`. In supervisor mode each
worker publishes scalar capacity and request/stream counters through Orbit; the
rendering worker sums fresh samples across workers. Because capacity is local,
`claviron_fcgi_max_live_aggregate` is the sum of worker ceilings.

The twelve counters describing what each pool did — queueing, timeouts, dials,
ping failures, expiry, replay and stale-queue verdicts — remain process-local.
Standalone metrics render them with a `pool` label. Supervisor mode does not
publish or aggregate them across Orbit.

The FCGI hot path also owns its buffered request and streaming-body counters.
Standalone mode reads those atomics directly. Worker-side metrics capture at
`metrics.publish_interval` and publish the request/stream counters together
with pool capacity scalars (`live`, `idle`, `max_live`) through this crate's
kind `215` Orbit family. Worker rendering accepts samples only within
`metrics.max_age`, floored at twice the cadence the family is written at —
a sample is one whole period old by construction, so a bound under the
cadence calls every sample stale for most of every period. The two
families are written an order of magnitude apart (kind `215` every
`publish_interval`, kind `216` every `interval`), so each carries its own. Full `claviron_fcgi_*{pool=...}` rows stay local to
standalone mode until a bounded row/chunk Orbit family exists. There is no
runner-agnostic PHP counter family.

Kinds `215` and `216` use managed `Reuse` / `Preserve` lifecycle.
Their freshness checks, rather than a master boot reset, exclude retained
samples.

Each family's freshness bound is taken against **its own** publication
cadence (`metrics::fcgi::freshness_bound`), not against `max_age` alone.
A sample is a whole cadence old by construction — written at the start of
the period, with the next one not yet due — so a bound under that cadence
calls every sample stale for most of every period and the family renders
empty. The two families are written at very different rates, kind `215`
every `publish_interval` and kind `216` every `interval`, so one number
cannot bound both: `max_age` is the operator's floor and the cadence sets
the minimum.

### 6.1 Reading connection pressure

A child serves one connection at a time, so every retained FCGI connection can
pin an FPM child whether or not it is currently serving. Compare the aggregated
process-local pool census with FPM's own `/status` gauges to size `max_live`,
`max_idle` and the number of workers. Orbit transports those metrics; it does
not own or cap the connections.

---

## 7. PhpFpmAdapter

This is the native entry for configured PHP hosts and explicitly composed
handlers. It holds an Arc<PhpFcgiService>, Arc<PhpRuntime> and optional exchange
capture sender. try_handle consumes that handle to move its existing references
into dispatch; handle retains the existing terminal API. Prefix selection is
external to try_handle and must occur before it is called.

The adapter knows script/front-controller resolution and its runtime's existing
try_files behavior. It does not own host ACL, redirects or explicit static mount
selection. Those remain native host/file steps ahead of PHP. It neither creates
Axum layers nor changes the engine's pooling, fallback or timing policy.

## 8. SAPI vs FCGI — risk matrix for the operator

The two runners are **functionally interchangeable** at the routing
layer (same `runner: …` switch) but have very different failure
profiles. Knowing them is what lets you pick correctly.

### 8.1 Failure isolation

| Scenario | FCGI (this crate) | SAPI (`php-embed`) |
|---|---|---|
| **Zend bailout** in user code (PHP fatal, segfault, OOM) | Kills one FPM child process. FPM master reaps + spawns a replacement. claviron sees `unexpected EOF`, purges idle pool, retries on a fresh conn. **Single request fails; system recovers.** | Kills the *thread* (one of N). Other threads keep serving. **Single request fails; system recovers.** Pre-multi-thread (single global PHP thread) bailout poisoned the entire SAPI pool — that's the design pressure that drove the multi-thread rewrite. |
| **OOM in PHP** | OOM-killer claims one FPM child. Master respawns. | OOM-killer claims the **whole claviron worker** (libphp.so is part of the worker process). claviron supervisor restarts the worker. Bigger blast radius — every concurrent SAPI request on that worker dies. |
| **Long-running PHP request** (lock wait, slow query) | Occupies one FPM child for the duration; pool's other children keep serving. | Occupies one PHP thread for the duration; pool's other threads keep serving. **Tighter contention by design** (fewer threads than FPM has children — see embed §5). |
| **claviron worker crash** (rust panic) | FPM survives. claviron worker restarts; pool re-warms via ping; FPM is unaware. | libphp.so + claviron worker die together; supervisor restarts; opcache cold-starts. Restart cost is higher. |
| **PHP segfault from a buggy extension** | Kills one FPM child. FPM master respawns. | Kills the **claviron worker process**. Supervisor respawns. |

### 8.2 Performance profile

| Metric | FCGI | SAPI |
|---|---|---|
| Throughput on Laravel + Postgres row-lock workload (50 concurrency, 10 user IDs) | 194 req/s (15 children, 27 conns) | **308 req/s** (1 process, 8 threads) |
| p95 latency | 733ms | **249ms** |
| p99 latency | 1353ms | **267ms** |
| Tail blow-out (max latency) | 2581ms | **292ms** |

(Numbers from `claviron-php-embed/ARCHITECTURE.md §5` — same
workload, same hardware.) SAPI wins on tightness because it shares
opcache + JIT across threads and admits fewer concurrent requests
(lower DB lock contention as a side effect). FCGI wins on
**stability under unknown extensions** — every PHP child is its
own process; segfaults don't take the runtime down.

> ⚠️ **Numbers are workload + 1-worker + network-bound specific.**
> Three caveats stack:
>
> 1. **Workload**: Postgres `SELECT FOR UPDATE` row lock with only
>    10 distinct user IDs bottlenecks throughput at the DB lock
>    queue, not the runtime floor.
> 2. **1 worker**: a single claviron worker was used; raising
>    `claviron worker_count` to 16 (the typical production figure)
>    cuts latency further on both runners.
> 3. **Network**: bench was on a **VPN + DNAT path with ~160ms
>    ICMP RTT**. Every measured number includes that baseline —
>    p50 of 171ms means ~11ms runtime cost; p99 of 267ms (SAPI)
>    means ~107ms runtime tail; p99 of 1353ms (FPM) means
>    ~1.2s of runtime + lock-queue tail. Subtract the network
>    floor before reading the table as a runtime-cost comparison.
>
> With a Rust bench client (no Python overhead), user-based locks
> (no row-lock pile-up), and a low-RTT path, both runners reach
> 20k+ req/s on the same hardware. Treat the table as evidence
> of **relative tail behaviour** under contention — not as
> either runner's ceiling.

### 8.3 Operational profile

| Aspect | FCGI | SAPI |
|---|---|---|
| Deploy / upgrade PHP | `apt upgrade php-fpm; systemctl restart php-fpm` — claviron unaware. | New libphp.so → recompile claviron worker binary → rolling restart. |
| Add / remove a PHP extension | `php-fpm.conf` + restart FPM. | Recompile claviron with new extension list. |
| Multiple PHP versions | Trivial — one FPM pool per version, route via `fpm:` selection. | One libphp.so per binary; running multiple versions = multiple binaries. |
| Pool isolation | FPM-side `[pool]` blocks (per-customer chroot, per-customer user). | Threads share one process — no chroot per route, no per-route uid. |
| Memory ceiling per request | FPM's `pm.max_children × memory_limit`. | Worker process RAM (libphp.so opcache + N threads' TSRM). |
| Distributed / remote PHP | Trivial — TCP endpoint to another box. | Process-local; remote means SAPI on the other box, talked to via FCGI from this one. |

### 8.4 Decision shortcut

Pick **SAPI** when:
- Single-tenant, well-known extension set (you control what loads).
- Workload is row-locked / DB-lock dominated; you want tighter
  admission control.
- p99 / tail latency matters more than peak throughput.
- You can tolerate a worker restart per PHP segfault.

Pick **FCGI** when:
- Multi-tenant or unaudited 3rd-party extensions in play.
- You need OS-level isolation per route (chroot, uid).
- You manage PHP separately from claviron (different teams,
  separate upgrade cadence).
- Multiple PHP versions on the same host.
- Remote PHP backends (different machine).

Mix-and-match is supported — a single vhost can have some routes
via SAPI and others via FCGI, layered in the same axum router. The
switch is per-route, not per-host.

### 8.5 Runner introspection — `$_SERVER` keys + canonical helpers

Both runners inject claviron-specific keys into PHP's `$_SERVER`
so the application (and benchmarks / probes / sanity checks) can
tell which runtime served a given request without sniffing
sapi-name strings. Two ready-made helper scripts live in
`public/`:

```php
// public/conn.php — FCGI runner introspection
[
    'php_runner_pid'      => getmypid(),
    'fcgi_conn_id'        => $_SERVER['CLAVIRON_FCGI_CONN_ID']    ?? null,
    'fcgi_conn_request'   => $_SERVER['CLAVIRON_CONN_REQUEST']    ?? null,
    'fcgi_mode'           => $_SERVER['CLAVIRON_FCGI_EXEC_MODE']  ?? null,  // "buffered" | "streaming"
    'fcgi_entry'          => $_SERVER['CLAVIRON_FCGI_ENTRY']      ?? null,  // fpm.yaml entry name
    'claviron_runner'     => $_SERVER['CLAVIRON_RUNNER']          ?? null,  // "fcgi" | "sapi"
    'claviron_worker_count' => $_SERVER['CLAVIRON_WORKER_COUNT']  ?? null,
    'claviron_php_runner' => $_SERVER['CLAVIRON_PHP_RUNNER']      ?? null,
    'sapi_thread_id'      => $_SERVER['CLAVIRON_THREAD_ID']       ?? null,  // null on FCGI
    'zend_thread_id'      => function_exists('zend_thread_id') ? zend_thread_id() : null,
];

// public/sapi-conn.php — SAPI runner introspection
[
    'php_pid'             => getmypid(),                              // worker process PID
    'sapi_thread_id'      => $_SERVER['CLAVIRON_THREAD_ID']    ?? null,
    'zend_thread_id'      => function_exists('zend_thread_id') ? zend_thread_id() : null,
    'claviron_runner'     => $_SERVER['CLAVIRON_RUNNER']       ?? null,
    'claviron_worker_count' => $_SERVER['CLAVIRON_WORKER_COUNT'] ?? null,
    'claviron_php_runner' => $_SERVER['CLAVIRON_PHP_RUNNER']   ?? null,
    'claviron_hostname'   => $_SERVER['CLAVIRON_HOSTNAME']     ?? null,
];
```

The pair forms the bijection invariant exploited by the test
scripts:

- **FCGI**: `(claviron_run_identifier, fcgi_conn_id) → fpm_pid`.
  Same composite key always lands on the same FPM child for the
  duration of that pooled connection.
- **SAPI**: `(claviron_runner, sapi_thread_id) → (php_pid,
  zend_thread_id)`. Same composite key always lands on the same
  dedicated PHP thread for the lifetime of the worker.

A violation of the bijection means something broke (TSRM rebind,
pool reuse, identity drift) — the probes bail before running
statistics on inconsistent data.

### 8.6 Benchmarking — `examples/php_probe.rs`

Rust load generator wired through
`/Volumes/Liberum/claviron-extra/crates/claviron-bench-client`. Driven by
`config/benchmark.yaml`
which lists named request profiles; each profile carries a
URL, protocol (`h1` / `h2` / `h3`), method, body, and timeout
budget.

```sh
# Run the default `php_probe` from config/benchmark.yaml
cargo run --example php_probe

# Override profile + concurrency
cargo run --example php_probe -- --probe sapi_probe --connections 50 --concurrency 10

# Burst mode — fire each slot's `count` iterations in parallel
cargo run --example php_probe -- --burst --count 100
```

Why it's worth knowing about:

- **Rust client, no Python overhead** — important when the
  benchmark itself becomes the bottleneck. `httpx` over Python
  asyncio caps out around 2-3k req/s before tokio-side claviron
  even breaks a sweat. The Rust probe pushes 20k+ on the same
  hardware, surfacing the runtime's real ceiling.
- **h1 / h2 / h3** all supported. SAPI's tail benefit is most
  visible over h2 / h3 with multiplexed streams; h1's
  per-conn-per-request shape masks the dispatch fairness.
- **Per-request timeouts** for connect / wait / total — failure
  modes are visible in the report (timeouts, status histogram,
  per-error-class buckets) instead of being absorbed into a
  generic "errors %" bucket.

### 8.7 Concurrency probes — `tests/*.py`

Four Python scripts live in `tests/` for properties harder to
prove with a load generator alone:

| Script | What it checks | Bijection / target |
|---|---|---|
| `tests/pool_chi2.py` | Chi-square uniformity of FCGI pool dispatch over a single keep-alive H1 session. Detects LIFO bias, replenisher churn, or broken `Drop` paths that favour a subset of conns. | `(run_id, fcgi_conn_id)` → uniform across pool? |
| `tests/sapi_chi2.py` | Chi-square uniformity of SAPI thread-pool dispatch over a single H2 session. Detects sticky-thread bias, broken round-robin, TSRM rebind. | `(claviron_runner, sapi_thread_id)` → uniform across threads? |
| `tests/sapi_probe.py` | Laravel `/sapi-probe/user-action` row-lock concurrency probe. Sustained concurrency via semaphore-bounded asyncio (not batch-and-wait); asserts each user's cache row equals its HTTP-2xx count. End-to-end accounting under contention. | per-user `cache.value == 2xx_count` |
| `tests/atomic_pipeline.py` | Single-statement atomic increment on `public.test_locks`. Fire N concurrent H2 requests, validate counter delta equals exactly N. Detects drops / doubles / torn requests anywhere in the chain (HTTP → tokio → mpsc → PHP thread → libpq → axum). | `delta == N` |

All four use `httpx[http2]` (Python) and `psycopg` for direct DB
verification — they bypass the application layer when checking
invariants. They're shellable with `uv run --script` (PEP 723
self-installing dependencies) so a fresh checkout can run them
without setting up a Python env.

These probes are the **operational truth** behind the
performance and fairness claims in §8.2 — when the SAPI thread
pool's chi-square drifts from uniform, the multi-thread
admission control story collapses; the test catches the
regression before the latency tail does.

---

## 9. Components — one-line summary

| File | Responsibility |
|------|----------------|
| `lib.rs` | `register()` providers; re-exports `PhpFcgiService`, `PhpFpmAdapter`, `build_fcgi_router` |
| `service.rs` | `PhpFcgiService` (`Provider` + `Reloadable`) — owns the entries map, lazy `get_entry`, `resolve_entry`, executes buffered + streaming |
| `config.rs` | `PhpServiceConfig` / `FpmClient` / `PoolConfig` — `fpm.yaml` shape + sizing knobs; strict records inside a permissive container |
| `pool.rs` | `FcgiPool` — three-tier pool, replenisher, housekeeping, the stale-keepalive verdict and its purge, `PoolLease` |
| `connection.rs` | `FcgiEndpoint` (Unix / TCP), `FcgiConnection` (id + used + stale check) |
| `ping.rs` | `ping_keepalive` — `/ping` → `pong` validator on the replenisher path |
| `proxy.rs` | `try_fcgi_proxy` — script resolution + CGI params + mode resolution + dispatch |
| `balancer.rs` | `pick_spacious_entry` — most-free-slots + RR tiebreak, one-shot as overflow |
| `bindings.rs` | `fcgi_dispatch` axum middleware + `build_fcgi_router` |
| `adapter.rs` | `PhpFpmAdapter` — bypass adapter (B2B / FW-secured / direct HttpExchange) |
| `metrics/fcgi/provider.rs` | `FcgiMetricsProvider` (per-worker pool capture + kind `215` publication) |
| `metrics/fcgi/requests.rs` | FCGI request/stream atomics, RAII guards, snapshot, and Prometheus formatter |
| `metrics/fcgi/store.rs` | FCGI local store, snapshots, merge rules, and Prometheus formatters |
| `metrics/fcgi/orbit.rs` | Fixed-width kind `215` codec for fleet request/stream counters and capacity scalars |
| `metrics/fpm/provider.rs` | `FpmMetricsProvider` (master + standalone scraper of FPM `/status`) |
| `metrics/fpm/store.rs` | FPM local store, keyed kind `216` codec, and Prometheus formatter |
| `metrics/fpm/status.rs` | `status_request` — FCGI request to FPM `/status`, returns parsed JSON |
| `metrics/provider.rs` | FCGI/FPM `MetricsFamily` composition plus managed `Reuse` / `Preserve` ring registration |
| `tasks.rs` | `fcgi_housekeep` — periodic pool sweep, registered with the cadence `fpm.yaml` asks for |
| `error.rs` | `GatewayError`, `ExecuteError`, `ConnectError` |

---

## 10. Status & remaining work

The crate is in production. Open items:

Pool behaviour:

- **Per-pool gauges are still standalone-only.** `claviron_fcgi_live`,
  `claviron_fcgi_idle` and the configured tiers carry a `pool` label only
  where they do not have to cross Orbit; in fleet mode they are summed
  into the `_aggregate` scalars. It matters far less than it did — the
  ledger's per-endpoint rows are read rather than carried (§6), and the
  event counters add — but a deployment with several FPM endpoints
  cannot see which of them is saturated without a bounded row/chunk
  family.

Elsewhere:

- **Streaming-path `HttpExchange` capture.** Today only buffered
  dispatch fans out to `exchange_tx`; streaming bodies never land
  in a `Bytes` we could clone. Two options worth a future pass:
  a tee adapter that buffers a copy of the body bytes alongside
  the streamed response (memory cost), or a hash + size pair
  that captures shape without content (cheap; doesn't satisfy
  audit consumers that need full payload).
- **Per-pool `pm.status` cadence.** Today the scraper runs every
  pool at the same interval. Some FPMs are cheap to poll
  (Unix-socket localhost), some expensive (cross-zone TCP). A
  per-entry `metrics_interval` override would help.
- **Worker-side health gauge.** `FcgiMetricsProvider` exposes
  pool counters but no `is_healthy` gauge. Combined with the
  scraper's view of FPM, a synthesised "is this FPM responsive
  right now?" signal would be a useful summary metric.
- **Hot reload drops rather than drains.** A reconfigured entry is
  removed so the next request rebuilds it; requests in flight finish on
  the old pool because a lease holds it alive, but its idle connections
  are not drained deliberately — they close when the last lease lets go.
  Good enough, and not the same as a graceful drain.
- **TLS to remote FPM.** `tcp:` endpoints are plaintext; TLS-
  wrapped FCGI for cross-zone PHP backends is a future ask.

---

## 11. Out of scope for this document

- **`PhpRuntime` and `vhost.yaml` parsing** — see
  `claviron-edge-config/ARCHITECTURE.md`.
- **The libphp.so / SAPI side** — see
  `claviron-php-embed/ARCHITECTURE.md`. The two crates dance:
  shared shape, opposite tradeoffs.
- **Worker process health and rotation policy** — see
  `claviron-supervisor/ARCHITECTURE.md`; PHP metric aggregation itself stays in
  this crate and does not depend on supervisor control state.
- **The vhost dispatch pipeline (axum router stacking, bypass,
  421 guard)** — see `claviron-edge/ARCHITECTURE.md`.

## Abandoned response ownership

The response pump selects receiver closure during pooled request setup,
response-record reads (including silent post-header SSE), and fresh replay
acquisition. Body-channel backpressure also observes receiver drop. An abandoned
pooled connection is marked unrecoverable before releasing its lease; unread
response bytes cannot become another request's response. Dial admission has a
scoped reservation, so cancelling acquisition before a socket/lease exists
also returns capacity. The one-shot setup future remains caller-owned. No
progress poll or SSE body deadline is introduced, and closing a socket makes
no claim that remote PHP execution has synchronously stopped.

## Replenisher lifetime

`PoolConfig.replenish` owns the positive initial backoff and cap (compatibility
100 ms / 5 s); null disables proactive repair, as does a zero warm floor.
`PoolOpts` retains that typed policy. No retry duration is synthesized inside
the repair loop. Its configured timer and active dial/probe both select the
pool's child cancellation token; the scoped dial reservation releases capacity
when interrupted. A task tracker owns all repair generations. Start/close are
serialized, preventing spawn-after-drain races, and terminal paths reset the
observed replenishing flag. Rechecking the warm floor after releasing the
start flag preserves repair without a periodic progress poll.

`PhpFcgiService` is now runnable to own this background lifetime. On service
cancellation it rejects new acquisitions, removes entries, cancels each pool's
repair and awaits its tracker before returning. Restart reopens lazy admission.
Configuration removal/replacement similarly retires only affected pools outside
the entries lock. Already leased responses remain owned by their pumps; retired
pools never requeue their connections. Pool cancellation does not cancel the
parent application or another pool.
