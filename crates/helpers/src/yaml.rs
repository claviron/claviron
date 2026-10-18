//! YAML loading helpers — env-var expansion before serde parses.
//!
//! `${ENV_KEY}` placeholders inside any YAML string are replaced with
//! the matching env var value, recursively resolving references inside
//! that value. Missing vars and reference cycles fail loud (no
//! silent fallback to "") so a broken `.env` produces a useful error
//! at config-load time instead of a hard-to-debug runtime symptom.
//!
//! Single supported format: `${NAME}`. Bare `$NAME` is left alone, so
//! YAML strings holding regex patterns, money signs, or PHP variable
//! names stay literal — operators opt in to expansion explicitly.
//!
//! Values come from the launcher environment or a supplied snapshot.
//! This also resolves nested references left literal by systemd's
//! `EnvironmentFile`; the helpers do not load dotenv files themselves.

use std::env;

/// Expand every `${NAME}` in `input` using the live process env
/// (`std::env::var`). Use [`expand_env_with`] when the runtime has
/// already scrubbed libc and you need to resolve from a snapshot
/// (e.g. during reload — see `claviron_runtime::env_scrub::EnvSnapshot`).
pub fn expand_env(input: &str) -> Result<String, String> {
    expand_env_with(input, |name| env::var(name).ok())
}

/// Generic placeholder expansion: same syntax as [`expand_env`] but
/// the lookup function is supplied by the caller. Lets reload paths
/// resolve `${ENV_KEY}` against an `EnvSnapshot` (or any other
/// key-value source) once libc env has been wiped.
///
/// Errors list every missing var (not just the first) so a single
/// broken config produces a single useful diagnostic. References inside
/// looked-up values use the same lookup; cycles report variable names,
/// never their potentially sensitive values.
pub fn expand_env_with<F>(
    input: &str,
    lookup: F,
) -> Result<String, String>
where
    F: Fn(&str) -> Option<String>,
{
    let mut out = String::with_capacity(input.len());
    let mut missing: Vec<String> = Vec::new();
    let mut active: Vec<String> = Vec::new();

    // Process line by line. Pure-comment lines (`#…`, leading
    // whitespace tolerated) are dropped entirely — `${ENV_KEY}`
    // literals in documentation comments don't reach the lookup
    // function, and the YAML parser sees a clean source. End-of-line
    // comments after a value are NOT recognised here; pin sensitive
    // values to their own line if the comment text would mention
    // `${...}` patterns.
    for line in input.split_inclusive('\n') {
        if line.trim_start().starts_with('#') {
            continue;
        }
        out.push_str(&expand_value(line, &lookup, &mut active, &mut missing)?);
    }

    if !missing.is_empty() {
        return Err(format!("missing env vars: {}", missing.join(", ")));
    }
    Ok(out)
}

fn expand_value<F>(
    input: &str,
    lookup: &F,
    active: &mut Vec<String>,
    missing: &mut Vec<String>,
) -> Result<String, String>
where
    F: Fn(&str) -> Option<String>,
{
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(idx) = rest.find("${") {
        out.push_str(&rest[..idx]);
        let after = &rest[idx + 2..];
        let Some(close) = after.find('}') else {
            return Err("unclosed `${` in env expansion".to_string());
        };
        let name = &after[..close];
        if !is_valid_var_name(name) {
            return Err(format!(
                "invalid env var name in `${{...}}`: `{name}`"
            ));
        }
        if let Some(start) = active.iter().position(|key| key == name) {
            let mut cycle = active[start..].to_vec();
            cycle.push(name.to_string());
            return Err(format!(
                "cyclic env reference: {}",
                cycle.join(" -> ")
            ));
        }
        match lookup(name) {
            Some(value) => {
                active.push(name.to_string());
                out.push_str(&expand_value(&value, lookup, active, missing)?);
                active.pop();
            }
            None if !missing.iter().any(|key| key == name) => {
                missing.push(name.to_string())
            }
            None => {}
        }
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

fn is_valid_var_name(s: &str) -> bool {
    !s.is_empty()
        && s.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
#[path = "../tests/yaml.rs"]
mod tests;
