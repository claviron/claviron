# Operational log

Claviron is a library, and what it builds is a server. Its log is what an
operator reads to know what the server is: which processes run, what they
serve and where, and what changed. A line that does not answer one of those
is noise at `info`, however true it is.

## Levels

| Level | A line at this level says |
| --- | --- |
| `error` | An operation failed and something does not work. |
| `warn` | Service is degraded, or the operator has to act. |
| `info` | A fact about what this server is, or a change to it. |
| `debug` | A detail of how that came about, for whoever is looking. |
| `trace` | One request, one frame, one wake. |

`info` is the operator's view and has to be complete on its own: a server
booted at `info` shows every process it started, every address it accepts on
and every application it serves.

## What is a fact

- A process started or ended: the server, a worker, a task's command. With
  its name, its process id and why it ended.
- Something this process owns began or stopped serving: a listener with its
  address, protocols and execution placement; an application with its
  capacity; a Link fleet with its shape and worker policy.
- A count the owner decides changed: a worker added or retired, and what
  asked for it.
- A peer this process depends on came or went: an application became
  reachable, or stopped being reachable.
- Configuration that defines what is served was loaded, with what it holds.

## One owner states a fact once

A fact is stated by the process that owns it, once.

- In a supervised fleet the master states what holds for every worker. A
  worker does not repeat at `info` what each of its siblings also says; that
  is `debug`. A worker states what is true of itself alone: that it failed,
  or that it was replaced.
- A fact about a name is stated for the name, not for each of its parts. An
  application is reachable when its first receiver is, and unreachable when
  its last one left; each receiver in between is `debug`.
- Work repeated for a structural reason, such as one pipeline per listener,
  states its result once, in the component that owns the thing the result is
  about.

## Waiting is a state, not a warning

Starting before something one depends on is ordinary. A route whose
destination has no owner yet says so once, as a fact, and says so again when
the owner arrives. It is not a warning, because nothing is wrong and nothing
is asked of the operator.

It is still a fact worth stating: a misspelled fleet or application name
looks exactly like an owner that has not started, and this line is where the
operator sees the name the server is waiting for.

## A refusal the operator can act on is a warning

When this process refuses work because one of its own declared limits is
reached, it says so with the limit's name and value. The line opens an
episode and is not repeated while the episode lasts; the episode ends with an
event, such as the limit's resource falling idle, never with a timer.

A refusal that is the declared behavior of an admission window, such as a
full receiver answering at once, is not a warning. It is counted, and it is
growth pressure where the owner declared a growth policy.

## What is never logged

- An optional configuration file or block that is absent. Absence is the
  default, at every level.
- The same fact at two levels, or in two components.
- A value the operator did not declare and cannot change.

## Departures

A peer that announced its shutdown and left is `info`. A peer that left
without announcing it is `warn`.
