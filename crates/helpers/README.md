# claviron-helpers

Shared configuration syntax, serialization, and small boundary conventions.

This crate keeps common input handling consistent across Claviron without
owning a runtime, provider, HTTP handler, or application domain. It has no
registry bootstrap or background service. Downstream code uses
`claviron::helpers`, which the facade exposes without a separate helper feature.

Use it when several components need the same boundary convention, not as a
place to put feature-specific business rules.

## Human-readable input, typed values

The `de` module provides Serde field adapters for:

- Duration strings, optional durations, and whole-second values.
- Human-readable byte sizes.
- Socket/bind addresses, regexes/regex sets, and ASCII-lowercased lists.
- A singleton or a sequence normalized into `Vec<T>` or `Option<Vec<T>>`.

For example, a consumer with Serde can accept one path or a list through the
same typed field:

```rust
use serde::Deserialize;

#[derive(Deserialize)]
struct Mounts {
    #[serde(default, deserialize_with = "claviron::helpers::de::one_or_many")]
    paths: Vec<String>,
}
```

Input shape selects singleton versus list decoding directly. Invalid items
retain their deserialization errors rather than being retried as another
untagged shape. The optional adapter also accepts null.

Parsing does not choose whether a duration is appropriate or whether an
address should be allowed. Each owning schema supplies its defaults and
semantic validation. In particular, the legacy `deserialize_bind_address`
assigns port 50051 to a bare IP; do not treat it as a universal protocol default.
`deserialize_optional_socket_addr` can use synchronous system hostname
resolution, so it is not a substitute for the managed async resolver capability.

## Sparse overrides distinguish omission from null

`overrides::Override<T>` separates `Inherit` from an explicit `Value(T)`.
Use a Serde field default for omitted values. `Override<Option<T>>` can then
represent explicit disabling as `Value(None)`, while non-nullable types reject
null through their own deserializer. Lists replace rather than merge; an
explicit empty list clears an inherited list.

`resolve(inherited)` applies that choice. Required and optional duration
adapters preserve the same distinction. This module defines replacement
syntax; the owner still decides where inheritance is legal.

## Explicit environment expansion

`yaml::expand_env_with(input, lookup)` expands `${NAME}` through a supplied
key/value lookup, including recursive references inside looked-up values.
It does not load dotenv files or mutate the process environment. The simpler
`expand_env` variant reads the live process environment instead.

Bare `$NAME` remains literal. Missing names are deduplicated and reported
together; malformed placeholders and reference cycles fail rather than
silently substituting empty text. Cycle errors report names, not environment
values. Pure comment lines are removed before expansion, but inline comments
are not parsed specially.

This is textual preprocessing, not a YAML parser, escaping layer, shell, or
template engine. It supplies no includes, conditions, filters, or shell-style
default expressions. Enclosing configuration owners still choose files,
preparation, validation, and reload. Claviron's shared catalog and environment
preparation live in [runtime](../runtime/README.md), using
[Congif](https://crates.io/crates/congif) and
[Dotenw](https://crates.io/crates/dotenw).

## Encoding and wire conventions

- `base64::base64_encode` and `base64_decode` use URL-safe unpadded Base64.
  `base64_standard_encode` is a separate standard-alphabet encoder.
- `time::iso8601` and `iso8601_option` adapt `time::OffsetDateTime` to RFC3339
  strings. They do not impose UTC conversion, fixed fractional precision, or
  an application's timezone rules.
- The `uuid` Serde adapter uses 16 raw UUID bytes for compact binary payloads.
  Do not substitute it for a JSON API's expected UUID string without changing
  that wire contract deliberately.

These helpers encode values; they do not validate JWT signatures, decrypt
credentials, or define an application's response format.

## Locks and small utilities

`sync::Unpoisoned` and `LockUnpoisoned` recover guards after lock poisoning.
They are appropriate only when the protected value remains valid after a
holder panics. They do not repair a partially applied multi-field invariant;
use the ordinary poison signal where such corruption is possible.

`strings::capitalize_first` uppercases the first Unicode character without
implementing locale-aware title casing. The ASCII lowercase config adapter
likewise is not a human-language normalization service.

`err::concise_error` is a legacy byte-limited display summary, not redaction or
a validation message bag. Its current fixed-byte truncation can panic on a
long string when the cut falls inside a UTF-8 character. Do not assume it is a
Unicode-safe arbitrary-error formatter.

## Related crates

- [Runtime](../runtime/README.md): state, configuration, and lifecycle ownership.
- [HTTP](../http/README.md): request/response policy using shared declarations.
- [Resolver](../resolver/README.md): explicit async DNS capability.
- [Sys](../sys/README.md): OS-facing primitives rather than input syntax.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
