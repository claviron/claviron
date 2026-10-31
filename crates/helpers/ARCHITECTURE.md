# Helpers — Architecture

The crate that keeps small cross-cutting conventions from being
rediscovered, reimplemented, and subtly diverged across the workspace.
It has no runtime provider, no registry hook, no async task, no global
state, and no application domain of its own. Its job is quieter: turn
the human-facing shapes operators write in config files, HTTP payloads,
and tokens into stable typed forms that the rest of Claviron can depend
on.

This crate is intentionally small, but it carries several important
architectural contracts:

- **Human input, typed output** — durations such as `10s`, byte sizes
  such as `300 MiB`, one-or-many YAML fields, optional socket addresses,
  bind addresses, regex sets, and lower-cased string lists are all
  accepted at config boundaries and normalized before the owning crate
  sees them. Consumers get `Duration`, `SocketAddr`, `RegexSet`, `u64`,
  or `Vec<T>` instead of parsing strings themselves.
- **Explicit environment expansion** — YAML env substitution only
  expands `${NAME}` placeholders, including references inside looked-up
  values. Bare `$NAME` stays literal. Missing variables and cycles fail
  loud; all missing names are reported together. This
  matters because config reload often happens after the runtime has
  scrubbed libc environment variables; callers can pass an explicit
  snapshot through `expand_env_with`.
- **Encoding conventions in one place** — base64url-without-padding is
  the default helper for ACME / JWT-style wire values, standard base64
  remains available for places that need the canonical alphabet, and
  UUID MessagePack-style binary serialization has one serde module
  rather than many local visitors.

---

## 1. Position in the layering

```
                         application / feature crates
                 (HTTP, gRPC, DB, JWT, ACME, chat, tasks, ...)
                                      │
                                      │ use serde hooks and tiny helpers
                                      ▼
                    ┌────────────────────────────────────┐
                    │ this crate                         │
                    │                                    │
                    │ de::*                              │
                    │   duration / byte-size parsers     │
                    │   one-or-many config adapters      │
                    │   socket/bind address adapters     │
                    │   regex and lowercase adapters     │
                    │                                    │
                    │ yaml::*                            │
                    │   ${ENV_KEY} expansion             │
                    │   snapshot-friendly lookup         │
                    │                                    │
                    │ base64 / time / uuid                │
                    │   shared serialization contracts   │
                    └────────────────────────────────────┘
                                      │
                                      ▼
              serde + time + humantime + uuid + base64ct +
              regex + byte-unit
```

`helpers` sits below nearly every feature crate that needs
human-friendly configuration or shared serialization helpers. It does
not depend on `runtime`, `http`, `db`, or any
domain crate. That direction matters: helpers can be used anywhere
without creating a dependency cycle, and domain crates never need to
copy-paste a parser just because a lower layer cannot depend upward.

### 1.1 What this crate is not

- **Not a foundation runtime.** It does not own `State`, providers,
  reloadables, run modes, cancellation tokens, or task lifecycles.
- **Not a config loader by itself.** It expands environment placeholders
  and supplies serde field adapters, but each owning crate still chooses
  which files to read, how to merge defaults, and when reload is legal.
- **Not a domain validation layer.** It does not know what makes a user,
  host, certificate, domain, token, or task valid, and it must not pull
  app-facing validation crates into the Claviron core.
- **Not a general-purpose utilities dump.** The useful test for adding
  something here is whether more than one crate needs the same boundary
  convention. If it is specific to one feature crate, it belongs there.

---

## 2. The central role — boundary normalization

Most of this crate lives at boundaries where operators or clients speak
in convenient external shapes and the implementation wants exact Rust
types.

```rust
#[serde(default = "default_timeout", deserialize_with = "deserialize_duration")]
pub request_timeout: Duration,

#[serde(default, deserialize_with = "one_or_many")]
pub listeners: Vec<ListenerConfig>,

#[serde(default = "default_bind_address", deserialize_with = "deserialize_bind_address")]
pub bind: SocketAddr,
```

The owning crate declares the semantic field. `helpers`
declares how the syntax is accepted. That split keeps configuration
structs readable: they show the actual domain shape while delegating
the messy compatibility layer to a shared module.

The rule for AI agents working in this repository is simple:

**If a config field wants a human-friendly representation, check
`claviron_helpers::de` before writing a new serde visitor.** The
workspace already has conventions for durations, optional durations,
byte sizes, socket addresses, bind addresses, regexes, lower-cased
lists, and one-or-many fields.

---

## 3. Deserialization helpers

`src/de.rs` is the largest module because it is the shared syntax layer
for config files.

### 3.1 Durations

The duration helpers all use `humantime::parse_duration`, so config
files can say `500ms`, `2s`, `10m`, `1h`, and similar readable values.

- `deserialize_duration<D, T>` parses an optional string into any
  `T: From<std::time::Duration> + Default`. Missing field becomes
  `T::default()`. This is why crates can use the same function for
  plain `Duration` fields and wrapper types that implement `From`.
- `deserialize_optional_duration` parses missing/null as `None`.
- `deserialize_duration_secs` parses an optional duration and stores
  whole seconds as `Option<u64>`, useful for HTTP cache policy fields
  and other wire/config values that want seconds rather than a
  `Duration`.

The architectural stance: **operators write time as time, not as raw
integer units.** Each owning crate decides its defaults; this crate
only standardizes the parser.

### 3.2 Bytes

`deserialize_byte_size` parses a `byte_unit::Byte` string and returns
`u64` bytes. This keeps values like memory thresholds readable in YAML
while giving supervisor/metrics style code a cheap numeric value to
compare.

### 3.3 Addresses

There are two address helpers with different contracts:

- `deserialize_optional_socket_addr` accepts null/missing as `None`,
  accepts direct `SocketAddr` strings like `127.0.0.1:9000`, and falls
  back to `ToSocketAddrs` hostname resolution. This is useful for
  outbound or optional upstream endpoints such as PHP-FPM.
- `deserialize_bind_address` accepts a full socket address, a bare IP,
  or text. Bare IPs get port `50051`. The error message explicitly names
  the gRPC bind-address expectation because this helper was shaped by
  gRPC/DNS/chat server config.

The subtle point for future edits: `deserialize_bind_address` bakes in
`50051` as the default port. Do not use it for a protocol whose default
port is different unless that is intentionally part of the public
configuration contract.

### 3.4 Regexes and normalized strings

- `deserialize_optional_regex` compiles an optional single regex.
- `deserialize_regex_set` compiles a list into `RegexSet`, which lets
  consumers test many patterns efficiently.
- `deserialize_lowercase_vec` lowercases each string with
  `to_ascii_lowercase`, which is appropriate for protocol/header-like
  ASCII identifiers, not for locale-sensitive human text.

### 3.5 One or many

Two helpers let config schemas be pleasant without making runtime
types optional or nested:

```yaml
tls:
  key_path: site.key
  cert_path: site.crt

# equivalent to
tls:
  - key_path: site.key
    cert_path: site.crt
```

- `one_or_many<T>` accepts either `T` or `Vec<T>` and always returns
  `Vec<T>`.
- `opt_one_or_many<T>` accepts null/missing, one item, or many items
  and returns `Option<Vec<T>>`.

These helpers are used by host config, HTTP, gRPC, chat, and PHP
configuration. They are part of the public ergonomics of Claviron's
YAML files: singleton syntax is accepted where the runtime still wants
a vector.

Input shape selects the decoder directly: a sequence is a list, while a
mapping or scalar is one item. The optional adapter also accepts null.
Neither adapter retries an invalid item as another untagged variant; item
deserialization and validation errors propagate without being replaced by a
generic variant-matching error. This is a cold configuration boundary, not
runtime work.

---

## 4. YAML env expansion

`src/yaml.rs` exists for one specific reason: config files need
environment variables, but expansion must be explicit and predictable.

Supported placeholder syntax:

```yaml
database_url: ${DATABASE_URL}
```

Not supported:

```yaml
database_url: $DATABASE_URL
```

Bare `$DATABASE_URL` is deliberately left untouched. That prevents
regex patterns, shell snippets, PHP variables, money strings, and other
literal dollar usage from becoming accidental environment lookups.

### 4.1 `expand_env`

`expand_env(input)` resolves placeholders through `std::env::var`.
This is the boot-time path: Claviron bootstrap loads `.env` values into
the process environment before config files are read, so plain process
env lookup is enough.

### 4.2 `expand_env_with`

`expand_env_with(input, lookup)` takes a caller-provided key/value
lookup function. This is the reload-safe path. Runtime boot can scrub
libc environment variables after capturing an environment snapshot; a
reload path can then expand `${NAME}` from that snapshot instead of
from live process env.

That is why downstream code in vhosts, db, domains, and JWT calls
`expand_env_with(&raw, |k| snap.get(k))`. The helper does not know what
an `EnvSnapshot` is. It only knows that lookup is an injected function.
This keeps `helpers` below `runtime` in the dependency
graph while still supporting runtime's scrubbed-env model.

### 4.3 Failure model

References inside environment values are resolved recursively through the same
lookup. For example, `${SYSINFO_LIB_PATH}` may refer to `${CLAVIRON_HOME}`,
whose value is `${HOME}`. This works with systemd `EnvironmentFile` values
without requiring a second dotenv load or mutation of the process environment.
Only referenced values are resolved; this is not a rewrite of the whole
environment snapshot. Literal bare dollar expressions remain unchanged.

The function fails when:

- a `${` is not closed,
- the variable name is empty or contains non `[A-Za-z0-9_]`
  characters,
- one or more referenced variables are missing,
- a value refers to itself or forms an indirect reference cycle.

Cycle errors include the variable-name chain, not environment values. Malformed
placeholder errors likewise do not include the raw input, which may hold secrets.

Missing variables are deduplicated and reported together:

```text
missing env vars: DATABASE_URL, JWT_SECRET
```

Pure comment lines are dropped before expansion. A commented-out sample
like `# password: ${EXAMPLE_SECRET}` therefore does not trigger a
missing-var error. End-of-line comments are not parsed specially; the
helper is a small preprocessor, not a YAML parser. Comment-like lines inside
environment values are data and are preserved.

---

## 5. Encoding and serialization helpers

### 5.1 Base64

`src/base64.rs` exposes three small functions:

- `base64_encode` uses `Base64UrlUnpadded`.
- `base64_standard_encode` uses standard base64.
- `base64_decode` decodes `Base64UrlUnpadded`.

The default encode/decode pair is the URL-safe, unpadded variant
needed by ACME/JWS/JWT-style values. The standard alphabet remains
available for places that explicitly need it, such as tooling output or
protocols that expect standard base64.

### 5.2 Time

`src/time.rs` provides serde modules for RFC3339 / ISO-8601
`OffsetDateTime`:

- `time::iso8601` serializes/deserializes a required `OffsetDateTime`.
- `time::iso8601_option` does the same for `Option<OffsetDateTime>`.

This gives API-facing models a consistent JavaScript-compatible string
format without each model carrying its own formatter.

### 5.3 UUID binary serde

`src/uuid.rs` serializes `Uuid` as its 16 raw bytes and deserializes
from either byte buffers or byte arrays. This is a binary transport
helper, not the normal JSON-facing UUID representation. It is used
where compact MessagePack-like payloads need UUIDs without string
inflation.

For AI agents: do not apply this module to a JSON API field unless the
consumer explicitly expects binary UUID bytes. For JSON, the standard
`uuid` serde string representation is usually the right contract.

---

## 6. Error helpers

`src/err.rs` exposes `concise_error`, which truncates display strings
to 300 bytes and appends an ellipsis. Domain providers use it when an
external API or database error needs to become a compact user-facing
status message.

Validation response shaping, including `validator::ValidationErrors`
conversion, belongs in application or auth-layer crates. Do not pull
validator into this core helper crate for app-facing message bags.

One sharp edge: `concise_error` truncates by byte length through
`String::truncate`. That requires the limit to land on a UTF-8 boundary.
At the current fixed value, this can panic if the 300th byte falls
inside a multibyte character. See §9.

---

## 7. Components

| File | Approx LoC | Role |
|------|----------:|------|
| `lib.rs`      |   8 | Public module declarations. No facade logic; consumers import modules directly. |
| `de.rs`       | 201 | Serde field adapters for human-friendly config syntax: durations, bytes, addresses, regexes, lower-casing, one-or-many lists. |
| `yaml.rs`     | 159 | `${ENV_KEY}` expansion with explicit lookup injection, missing-var aggregation, and pure-comment-line skipping. |
| `uuid.rs`     |  76 | Binary serde adapter for `Uuid` as exactly 16 bytes. |
| `time.rs`     |  65 | RFC3339 serde modules for required and optional `OffsetDateTime`. |
| `base64.rs`   |  13 | URL-safe unpadded and standard base64 helpers. |
| `err.rs`      |  12 | Short user-facing error summarization. |
| `strings.rs`  |   7 | `capitalize_first`; tiny string helper. |

---

## 8. How to use this crate safely

For humans:

- Prefer readable config values. Use `10s`, `1m`, `300 MiB`, singleton
  YAML objects, and `${ENV_KEY}` placeholders where supported by the
  owning crate.
- Treat `${ENV_KEY}` as explicit opt-in. A plain dollar sign is literal.
- Missing env vars fail at config-load time by design. That is a feature:
  the process should not boot or reload with silently empty secrets.

For AI models and future maintainers:

- Before adding a serde deserializer in another crate, search this crate.
  Duplicate parsers create tiny compatibility fractures that are painful
  for operators.
- Keep helpers dependency-light. If a helper needs `runtime` or
  a domain crate, it probably belongs somewhere else.
- Keep function names concrete. This crate is shared vocabulary; vague
  helpers here become vague APIs everywhere.
- Be careful with protocol-specific defaults. `deserialize_bind_address`
  defaults bare IPs to port `50051`; that is not a universal server port.
- Use `expand_env_with` on reload paths that depend on runtime's captured
  environment snapshot.

---

## 9. Status & remaining work

The crate is stable and widely used. A few small improvements would make
it safer and clearer:

- **Make `concise_error` UTF-8 boundary safe.** Replace direct
  `String::truncate(300)` with a character-boundary-aware truncation.
  The current implementation can panic on long non-ASCII error strings
  if byte 300 lands inside a code point.
- **Document default-port ownership for `deserialize_bind_address`.**
  The helper currently names gRPC in its error message and defaults bare
  IPs to `50051`, but DNS/chat also use it. A generic helper could accept
  the default port as a parameter, or protocol-specific wrappers could
  live in the owning crates.
- **Add tests for `de.rs`.** `yaml.rs` has good focused tests; the serde
  adapters would benefit from regression tests around one-or-many,
  optional duration, byte size parsing, and bind-address defaults.
- **Consider symmetric standard base64 decode.** Today the decode helper
  only accepts URL-safe unpadded base64. That is correct for current
  call sites, but if standard-base64 consumers grow, a named
  `base64_standard_decode` would avoid ambiguous local decoding.

None of these change the crate's role. `helpers` is the shared
edge-normalization toolbox: small functions at the outside of the
system, keeping the inside typed, consistent, and less surprising.

---

## 10. Out of scope

- **Provider lifecycle.** Boot, reload, shutdown, task orchestration, and
  run modes belong to `runtime` and crates that implement
  providers.
- **Domain-specific config validation.** This crate can parse common
  syntax; it does not decide whether a domain, vhost, certificate, DB
  pool, JWT issuer, or chat setting makes sense.
- **Full templating.** YAML expansion is not a template language. There
  are no defaults, filters, conditionals, includes, shell expansion, or
  nested expressions.
- **Transport-specific serialization policy.** Helpers exist for common
  encodings, but each API/protocol crate owns its public wire contract.
