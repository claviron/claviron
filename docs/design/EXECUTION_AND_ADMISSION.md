# Execution, admission and worker count

Four small contracts decide where work runs, how much of it runs, how much may
wait and how many processes serve it. Each concept has one name, one owner and
one place in configuration. Engines, runners and bindings implement the same
contracts; none of them adds a private variant.

## Execution placement

Two independent questions.

**Whose executor.**

| Word | Meaning |
| --- | --- |
| `inherit` | None of its own. It runs on the executor of whatever starts it. |
| `dedicated` | One of its own, kept for as long as it exists. |

**How many threads that executor has.** `current` is one thread, `multi` is
several. In code these are `ExecutorKind::CurrentThread` and `MultiThread`.

Placement is what an operator chooses, so it is the word in configuration:

```yaml
defaults:
  listener:
    execution: inherit      # the process runtime
listeners:
  public:
    execution: dedicated    # a thread of its own
```

The type is `Execution`, converted once into Continuo's `ExecutionSpec`.
`dedicated` is `ExecutorSpec::Dedicated(CurrentThread)`: the runtime keeps one
executor per runnable across restarts and releases it when execution is
finalized. A listener, a runner's Orbit door and any other runnable use the
same word with the same meaning.

A task spawned on the same executor is not another placement; it inherits.
Blocking application code needs threads it may block, owned and counted by its
engine; an unowned blocking pool is not a placement.

## Application execution

An application owner declares where it executes an exchange in its
`BackendContract`, beside the request identity it needs and who completes a
streamed reply. A runner's `Owner` declares the same. The declaration is
mandatory and made once, during preparation.

| Owner | Declares |
| --- | --- |
| Rust router, forwarding owners | `Inherit` |
| Lua | `Inherit` |
| PHP application, Deno, ASGI | `Dedicated(CurrentThread)` per instance or loop |
| Native module, PHP script pool | `Dedicated(MultiThread)` with its thread count |

An engine offers an `execution` key only where it supports a choice, and
refuses a value it cannot honor. None offers one today.

## Admission

| Key | Meaning |
| --- | --- |
| `max_in_flight` | Exchanges that run at once. |
| `max_queued` | Exchanges that may wait, running ones not included. `0` is no queue. |

There is one waiting room per process, and the layer in front of the
application owns it.

| Where the application runs | Runs at once | Waiting room |
| --- | --- | --- |
| Behind Link, engine with a fixed capacity | the engine's own key, such as `php.application.instances` | `orbit.link.max_queued` |
| Behind Link, asynchronous engine | `orbit.link.max_in_flight` | `orbit.link.max_queued` |
| Inside an Edge | the engine's own key | the engine's `max_queued` |

Writing the other layer's key is refused with a message that names the owner.
A Link receiver publishes `max_in_flight + max_queued` as its admission window,
which stays below its inbox capacity. A caller never waits for a receiver: a
full window is refused at once, and a queued exchange is withdrawn when the
route's `requests.queue_timeout` ends before it starts.

A queue is counted in exchanges but its budget is time. Size it so that a full
queue drains within `requests.queue_timeout` at the receiver's rate.

Deno has no waiting room: a request starts on an isolate at once, so its one
bound is `max_in_flight`.

## Worker count

A Link fleet's process count is declared once, with one type:

```yaml
workers: { policy: static, count: 6 }
workers: { policy: dynamic, min: 4, max: 16, step: 1, retire_idle: off }
```

`dynamic` grows the fleet when a caller finds no receiver that would start its
exchange now, and needs receivers that declare `max_in_flight`. The decision
belongs to the receiving side, because a new worker adds capacity while a
caller's choice of receiver does not. The caller's choice is a separate key on
the route:

```yaml
orbit:
  - { application: example, fleet: example, balance: first }
```

Who owns the count:

| Application | Owner | Declared in |
| --- | --- | --- |
| A runner with a supervisor | the runner's master | `orbit.link.workers` |
| A binding, or a runner without a supervisor | a Link task | `tasks.<name>.link.workers` |

## Link tasks

A Link task runs the processes of one Link fleet for an application that has
no supervisor of its own. It never creates the fleet or chooses its geometry.

```yaml
tasks:
  example:
    link:
      fleet: example
      kind: worker
      workers: { policy: dynamic, min: 4, max: 16, step: 1, retire_idle: off }
      command: { program: ./example }
```

| `kind` | The command is | Count owned by |
| --- | --- | --- |
| `worker` | one worker of the fleet | the task |
| `runner` | a runner that owns its workers | the runner |

`workers` is `static` for a fixed count or `dynamic` for one that grows on
pressure. A process that serves a Link fleet is run by a Link task either
way, so the fleet has one owner of its count; an ordinary task runs commands
that serve no fleet.

## Bindings

A binding owns its I/O executor, so its placement is always `dedicated` and its
option selects the kind, `current` or `multi`. It declares the same
`max_in_flight` and `max_queued`. `inherit`, where Orbit I/O would run on the
host's own loop, is reserved.

## Management

A Link application and a Link task are reported inside the applications and
tasks services as `link_application` and `link_task`. Neither is a separate
feature or capability.
