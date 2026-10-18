# Claviron fuzz targets

This standalone cargo-fuzz workspace tests Claviron-owned functional contracts
through the public facade with only `http` enabled. It does not boot providers,
language engines, services or listeners, and is outside the library test suite.
Hyper's HTTP parser and Quinn's QUIC implementation belong to their own fuzz
suites; these targets exercise the policy Claviron adds around them.

| Target | Contract | Input format |
| --- | --- | --- |
| `request_authority` | Version-specific authority validation, duplicate/missing Host rejection, identity stability and request immutability | First line URI, remaining lines individual Host values |
| `forwarding` | Version-specific smuggling guard, Connection-nominated and fixed hop-by-hop removal, preservation of other fields, cleanup idempotence | One `name:value` header per line; duplicates preserved |
| `route_table` | Prefix normalization, longest segment-prefix selection against an independent oracle, stable ties | First line lookup path, remaining lines route prefixes |

Invalid outer HTTP values are rejected before invoking Claviron, matching the
typed transport boundary. Authority runs every accepted input against HTTP/1.0,
HTTP/1.1, HTTP/2 and HTTP/3. Route inputs must be UTF-8. No deployment files or
local hostnames are used.

## Run

Install tooling if needed; the project's ordinary Rust toolchain stays unchanged:

```sh
rustup toolchain install nightly --profile minimal
cargo install cargo-fuzz --locked
```

From the repository root:

```sh
cargo fuzz list
cargo +nightly fuzz run request_authority fuzz/corpus/request_authority fuzz/seeds/request_authority -- -max_len=4096
cargo +nightly fuzz run forwarding fuzz/corpus/forwarding fuzz/seeds/forwarding -- -max_len=4096
cargo +nightly fuzz run route_table fuzz/corpus/route_table fuzz/seeds/route_table -- -max_len=4096
```

Create the writable corpus directories before the first run:

```sh
mkdir -p fuzz/corpus/request_authority fuzz/corpus/forwarding fuzz/corpus/route_table
```

`seeds/` is the checked-in starting input set; generated corpus, coverage and
crash artifacts are ignored. Keep the evolving corpus as the first directory
so libFuzzer writes new discoveries there, for example:

```sh
cargo +nightly fuzz run forwarding fuzz/corpus/forwarding fuzz/seeds/forwarding -- -max_len=4096 -runs=10000
```

Omit `-runs` for continuous fuzzing. The input bound
and run count are harness settings, not production configuration.

Reproduce and minimize a crash:

```sh
cargo +nightly fuzz run forwarding fuzz/artifacts/forwarding/crash-<hash>
cargo +nightly fuzz tmin forwarding fuzz/artifacts/forwarding/crash-<hash>
```
