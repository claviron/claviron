---
name: claviron-testing
description: Design focused Claviron regression tests or diagnose failing tests with feature, lifecycle, and process isolation in mind. Use when adding tests, changing a tested contract, or investigating failures; not for routine documentation edits.
---

# Claviron tests and verification

Respect the requested verification scope first. A no-build task does not permit
compilation to validate a skill or documentation edit. Use the project's current
contribution guidance and check-tool help; do not assume a maintainer-local
script, interpreter, or live deployment exists.

## Pick the smallest boundary that proves the behavior

For a reported failure, compare the assertion with the current production
contract. Decide whether the behavior regressed or the fixture/expectation is
stale before changing either. Do not weaken a production guard, add an unlimited
fallback, or suppress a test to obtain a green result.

Test the parser, algorithm, codec, or state transition directly with minimal
inputs. A library test must not boot Claviron, providers, a language engine, or
a process fleet just to reach that code. Put genuine runtime/end-to-end work in
a separate executable harness, not behind `#[ignore]` in the ordinary test suite.

## Placement and isolation

- Keep test bodies under each crate's `tests/`, mirroring source modules.
  A production module may contain the `#[cfg(test)]` / `#[path]` inclusion for a
  private unit test, not inline test bodies.
- Follow `autotests = false` where private tests are included this way. Declare
  genuine integration targets explicitly to avoid compiling the same file twice.
- Coordinate races with channels, barriers, joins, explicit wakes, or controlled
  time. Do not coordinate by sleeping or require the whole suite to run serially.
- Isolate process-global foreign runtime state in the explicit runtime harness.
  PHP ZTS does not make SAPI initialization and shutdown independent per test.
- Keep foreign-language fixtures in files with their real extensions, rather
  than embedding PHP/JS/Python programs into Rust string literals.

Use synthetic hosts such as `example.test` and isolated endpoints when a transport
harness needs sockets. Do not load an application's private configuration, touch
its DB, reuse its running fleet, or clear its SHM as test setup.

## Check the affected capability

Use the current project's prescribed Cargo selection. Where package-scoped
checks apply, narrow to the changed package and useful test-name filter. A
feature-boundary change needs the affected enabled and disabled paths; a portable
change does not automatically need every operating system or language engine.

Report the exact commands, selected features, environment limits, and observed
results. Distinguish successful compilation, a functional assertion, runtime
boot, and live-client behavior. Do not describe unrun tests or a source review as
verified execution.

## Source map

Read `CONTRIBUTING.md`, the affected crate README, its manifest test targets,
and the nearest existing regression test. Read local architecture documents
when present, but keep new fixtures usable without private deployment material.
