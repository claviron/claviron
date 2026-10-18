# Contributing to Claviron

Bug reports, focused fixes, documentation, and reproducible benchmarks are
welcome. Discuss changes to public APIs, lifecycle contracts, or crate boundaries
in an issue before starting a large implementation.

## Find the right place

Start with the [crate guide](README.md#crate-guide-and-feature-relationships).
Read the affected crate's README, `ARCHITECTURE.md` where present, and public
type documentation. Read the owning dependency's contract when a change crosses
crate boundaries.

- The root crate is the public facade; subsystem implementations live in
  `crates/`. Expose downstream capabilities through the facade.
- Application business logic, deployment configuration, and credentials belong
  in application repositories.
- Language engines and managed extensions maintained in companion repositories
  should receive their changes there.

## AI skills

Repository-local skills provide focused guidance for coding agents:

- [Services](.agents/skills/claviron-services/SKILL.md): DI, configuration, and lifecycle.
- [Routing](.agents/skills/claviron-routing/SKILL.md): Edge, Runner, and Rust interception.
- [PHP](.agents/skills/claviron-php/SKILL.md): execution modes and native framework bridges.
- [Orbit](.agents/skills/claviron-orbit/SKILL.md): shared state, Link, and SHM ownership.
- [Testing](.agents/skills/claviron-testing/SKILL.md): regression tests and verification scope.

They are usable without the unpublished architecture documents.

## Development setup

Use stable Rust with rustfmt and Clippy. Select the features needed by the crate
you are changing; the facade has no default features. Native integrations may
need additional tools. Embedded PHP, for example, requires a matching ZTS embed
build and `PHP` / `PHP_CONFIG` settings; see its
[requirements](crates/php-embed/README.md#requirements-and-optional-capabilities).

### Local Python tools

The root `pyproject.toml` manages a local, non-packaged Python 3.15+ environment;
it does not define a Python distribution of Claviron. `uv sync` installs the
`dev` group. Use `uv sync --group test` for HTTP/database test clients and pytest,
or `uv sync --group bench` for numerical analysis tools. These environments do
not build the Rust workspace or start an application.

Gufi owns Python execution and provides the native `gufi` module inside its
interpreter. That module is separate from this repository's local project name;
it is not supplied by a Python package installed from this checkout.

### VS Code

Open this repository for library work. Its recommended extension is
[rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer);
Python, PHP and Lua extensions are not suggested for every Claviron contributor.
Extension recommendations are optional and do not install anything automatically.

`Terminal: Run Task` exposes format/lint, check-only, full-test and filtered-test
tasks backed by the public scripts below. Nothing runs when the folder opens.
Editor formatting uses `rustfmt.toml`; the example settings leave check-on-save
disabled so save operations do not launch another Cargo check. Language-server
workspace analysis and proc-macro support can still require compilation.

Only `.vscode/extensions.json`, `tasks.json` and `settings.example.json` are
shared. To opt into the example extension settings, copy or merge
`settings.example.json` into the ignored `.vscode/settings.json`. It selects
language tools without automatic formatting or fixes on save. Keep personal
interpreter paths, credentials, terminal profiles and themes in local/user
settings. The scripts remain the validation authority regardless of editor.

For applications, open the individual [liberum](liberum/README.md#editor-workspaces)
folder or its optional multi-root workspace instead. Each application has its
own extension suggestions and tasks; its Cargo commands never select the library
workspace by accident.

## Keep changes focused

Use existing feature combinations before introducing a new feature. Preserve
optional dependency boundaries: adding an adapter must not silently select an
engine, listener, or serving composition.

Keep operational policy in typed configuration. Preserve the owning component's
cancellation, drain, and resource-ownership contracts. Explain safety invariants
next to unsafe code. When behavior or a boundary changes, update its architecture
document and public examples with the implementation.

Write code comments, documentation, logs, and commit messages in English. Keep
unrelated formatting and dependency updates out of a functional change.

## Checks and tests

Contributors and maintainers use the same repository scripts. They require Bash
3.2+ and an installed Rust toolchain with rustfmt and Clippy, not personal shell
helpers. They locate the repository root and preserve PATH, target directory,
build flags and Cargo configuration.

```sh
# Format sources and lint; use --check for a non-mutating CI check.
./scripts/format.sh
./scripts/format.sh --check

# Optionally limit formatting; Clippy still checks the entire workspace.
./scripts/format.sh -p claviron-http

# Run tests, or select a test-name substring without changing the feature set.
./scripts/test.sh
./scripts/test.sh host_binding -- --nocapture
```

Formatting uses the repository's stable `rustfmt.toml`. Clippy selects
`--workspace --all-features --all-targets` and treats warnings as errors. Explicit
`format.sh --fix` runs `cargo fix` before formatting; it can modify staged and
unstaged sources, so review its diff. It cannot be combined with `--check`.

Tests select `--workspace --all-features --no-fail-fast`, including the declared
test targets and doctests, with normal parallelism. A filter narrows test names,
not compilation scope. Arguments after `--` go to the test harness; ignored
tests are not enabled automatically. Tests do not run formatting or Clippy.

All-feature runs require the native dependencies described in the crate READMEs,
including the PHP ZTS embed build; the scripts do not install tools, build those
dependencies or start a deployment. SQLx validation defaults to offline mode;
explicit `SQLX_OFFLINE=false` opts into live database validation.

For feature changes, also check the affected enabled/disabled combinations.
Validate platform-specific changes on that platform. Documentation-only edits
need link and formatting checks, not a workspace rebuild.

Tests should demonstrate a regression or meaningful contract:

- Keep test bodies under the crate's `tests/` directory, mirroring source paths.
  Private unit tests can be included from their owning module with `#[path]`.
  Follow the crate's `autotests = false` and explicit test-target arrangement.
- Library tests exercise parsing, algorithms, and state transitions without
  booting a server, provider fleet, or embedded language engine. Runtime and
  end-to-end checks belong in a separate executable harness.
- Use explicit signals or controlled time for coordination, not sleeps. Tests
  must remain independent and runnable in parallel.
- Use small fixtures with reserved names such as `example.test`. Do not load
  personal deployment configuration or require live external services.
- Keep PHP, JavaScript, and other foreign-language fixtures in their own files.
- Keep sanitized `.env` fixtures and their environment variants under
  `crates/*/tests/fixtures/` in version control. They are test inputs, not local
  deployment configuration; never put real credentials in them or exclude them
  merely because their names contain `.env`.

Benchmarks should state the platform, build profile, workload, concurrency, and
comparison boundary. Separate transport measurements from application throughput.

## Registry package contents

Each package uses an explicit `include` list: Rust sources, README, and any
required build script or runtime assets. Preserve PHP adapters and entry points,
embedded Lua modules, and their license notices. Add new non-Rust build inputs
to the owning package's list when introducing them.

Tests, fixtures, benchmarks, application examples, local configuration and
development tools stay in Git, not registry packages. Run their checks from a
checkout. Use `cargo package --list --workspace` to inspect package contents
without building or publishing.

Internal Rust dependencies retain local paths for development and use crates.io
when packaged. Select `--registry crates-io` explicitly when publishing so a
maintainer's local default cannot redirect the upload to a private registry.

## Reports and pull requests

Report suspected vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

For a bug, include a minimal reproduction, expected and observed behavior,
platform, selected features, and relevant versions. Remove credentials and
private application data from logs and screenshots.

Keep a pull request about one change. Describe why it is needed, affected APIs
or configuration, and how it was verified. Include exact checks and results;
state anything not tested. Update the relevant documentation and add a focused
regression test when appropriate.

Publishing and release version changes are handled by maintainers. Contributions
use the project's [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) license terms;
retain existing third-party notices.
