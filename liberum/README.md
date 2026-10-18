# liberum

An open playground where applications built on Claviron come to life.
Independent applications share one edge through Orbit Link:

- `edge-server`: HTTP ingress, host admission and static assets.
- `fastapi-app`: Python/ASGI through Gufi, including WebSockets.
- [orbit-app](orbit-app/README.md): React/Vite and Hono on Deno, Node or Bun,
  using the local `orbit-js` addon, with streaming demo events.
- [octane-app](octane-app/README.md): Laravel through Lumenize, with native
  broadcasting, notifications, Pusher WebSockets and Rust interception.

Start applications independently, then use `scripts/edge.sh` from this directory.
The `scripts/` directory contains example launchers, not library install hooks.
Each Rust application has its own workspace and build output; it is not a member
of the library workspace. JavaScript and PHP dependencies are installed in their
respective application directories.

## Examples and scaffold sources

These are public examples and sources for future application scaffolds, not
private user deployments. Their `.env` files contain shareable demo defaults;
never put real credentials or personal data in them. Environment ignore rules
are scoped to the repository root, not these example directories.

Claviron's example and scaffold sources belong here. Lumenize owns its scaffold
sources and their installation through the Lumenize binary in its own repository.
This layout does not yet provide a scaffold generator.

## Editor workspaces

Open one application folder in VS Code to work on that application alone, or
open [liberum.code-workspace](liberum.code-workspace) for all five applications
as separate roots. The library root is deliberately not part of that workspace.
Rust tasks use each application's own Cargo manifest, not the library scripts.
No task starts, installs dependencies or runs a server automatically.

| Folder | Optional extensions | Tasks and settings |
| --- | --- | --- |
| `edge-server` | rust-analyzer | Rust format, format check, Clippy and tests |
| `fastapi-app` | rust-analyzer; Microsoft Python, Pyrefly and Ruff | Rust tasks; Python navigation/types and manual Ruff formatting |
| `octane-app` | rust-analyzer; PHP Intelephense | Rust tasks and PHP tests; PHP 8.3 syntax baseline |
| `lua-app` | Lua Language Server | Lua 5.5 and Claviron type declarations; manual editor formatting |
| `orbit-app` | None beyond built-in JavaScript/JSX support | Existing npm dev/build scripts; manual editor formatting |

Recommendations are suggestions, not an extension pack. Install only the tools
for the application and languages you work on. The multi-root view exposes each
folder's recommendations; it does not require installing them all. Do not run
competing language servers for one language: Pyrefly supplies Python types and
navigation, Ruff supplies lint/format; Intelephense replaces VS Code's basic PHP
completion. Orbit supports several JavaScript runtimes, so no Deno-only language
configuration is imposed on Node/Bun users.

Each `.vscode/settings.example.json` is an opt-in template. Copy or merge it into
that folder's ignored `.vscode/settings.json`; do not overwrite existing local
settings. Keep formatter rules in the application's existing config files:
`rustfmt.toml`, `pyproject.toml` and Octane's `.editorconfig`. Templates disable
format-on-save to avoid incidental rewrites of fixtures or third-party Lua code.
They do not establish new PHP/Python/Lua lint policies.

Install dependencies separately as described in each application's README.
Select the FastAPI environment after preparing it; no absolute Python path is
shared. For `PHP: Test`, expose the application's matching ZTS embed PHP binary
as `PHP` to VS Code; there is no fallback to system/Homebrew PHP. Octane HTTP
tests also need a local app key and built frontend assets. Lua has no standalone
test runner here, and Orbit has no npm test script, so no test task is invented.
All-feature Rust tasks require the native dependencies of that application.

## Release baseline

These applications consume released packages, not moving sibling checkouts:

| Dependency | Required release |
| --- | --- |
| Claviron | `0.2.0-alpha.18` |
| Lumenize | `0.2.0-alpha.18` |
| Gufi | `0.1.0-alpha.2` |
| `@claviron/orbit` | `0.1.0` |
| `claviron/sapi`, `claviron/laravel` | `0.1.0` |

Publish these releases before installing the examples. Rust requirements use
exact versions; the JavaScript and PHP integration requirements are pinned as
well. A native Orbit addon must be available for the consumer's OS and CPU.
No launcher silently falls back to a sibling source tree.

## Shared demonstration data

[database/demo.sqlite](database/README.md) is one pre-migrated SQLite file with
synthetic users, todos and records. Octane owns migrations; FastAPI and Orbit
read the same file with their own process-local connections. Orbit Link does
not transport DB handles or transactions. Edge only routes requests and does
not need a connection to this database. FastAPI and Orbit expose `/api/db` to
compare the data seen by their independent processes.

No PostgreSQL server is needed for the common demo. Lua's native SQLite adapter
is not yet exposed; its DB diagnostic reports 501 instead of using PostgreSQL implicitly.
See the shared database README for explicit migration commands, snapshot
handling and the restriction against sharing a live file across OS/VM hosts.

The migration from local dependencies is not complete until Cargo, npm and
Composer lockfiles have been regenerated against the published releases.
Existing local-path lock entries are not registry release evidence. The optional
Python/PHP benchmark tools still use a sibling `bench` checkout; they are a
separate development-only dependency, not a published example runtime.

Use an explicit local override only when testing a core change deliberately;
do not commit that override as the examples' normal dependency source. Core
pull requests therefore do not implicitly update these application runtimes.

The edge declares `orbit.test` and `octane.test` alongside the existing FastAPI
transport-comparison hosts. Configure a local listener and resolve those names
to loopback for browser use. Launchers do not modify host resolution or restart
an already-running edge. Demo routes are unauthenticated and belong on a local
listener, not a public interface.


