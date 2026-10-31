//! Boot-compiled path ACLs. No router, file lookup or native transport here.

use std::borrow::Cow;
use std::net::IpAddr;
use std::sync::Arc;

use claviron_http::ext::PathPrefix;

use crate::service::RequestRejection;
use crate::config::acl::{AccessLimiter, AclConfig};

pub(crate) struct CompiledAcl {
    rules: Box<[(PathPrefix, AccessLimiter)]>,
}

impl CompiledAcl {
    pub(crate) fn compile(config: &AclConfig) -> Option<Arc<Self>> {
        Self::from_entries(
            config.rules().map(|(path, rule)| (path.clone(), rule.clone())),
        )
    }

    pub(crate) fn from_entries(
        entries: impl IntoIterator<Item = (PathPrefix, AccessLimiter)>
    ) -> Option<Arc<Self>> {
        let mut rules: Vec<_> = entries.into_iter().collect();
        if rules.is_empty() {
            return None;
        }
        rules.sort_unstable_by(|(a, _), (b, _)| {
            b.len().cmp(&a.len()).then_with(|| a.as_str().cmp(b.as_str()))
        });
        Some(Arc::new(Self { rules: rules.into_boxed_slice() }))
    }

    pub(crate) fn check(
        &self,
        path: &str,
        ip: IpAddr,
    ) -> Result<(), RequestRejection> {
        let paths = comparison_paths(path)?;
        let selected = self.select(&paths.separated);
        // An escaped slash reads two ways: as a separator, or as a character
        // inside one segment (what npm's `@scope%2fname` means). Refused only
        // when the two readings would be judged by different rules, which is
        // when it could smuggle a path past its rule (`/admin%2fx`).
        if let Some(literal) = &paths.literal
            && selected.map(|rule| rule as *const _)
                != self.select(literal).map(|rule| rule as *const _)
        {
            return Err(RequestRejection::AmbiguousPath);
        }
        if selected.is_some_and(|(_, rule)| !rule.is_allowed(ip)) {
            return Err(RequestRejection::AccessDenied);
        }
        Ok(())
    }

    fn select(
        &self,
        path: &str,
    ) -> Option<&(PathPrefix, AccessLimiter)> {
        self.rules.iter().find(|(prefix, _)| prefix.has_path_segment(path))
    }
}

/// The path an ACL compares, and, when it holds an escaped slash, the other
/// reading of it.
pub(crate) struct ComparisonPaths<'a> {
    /// Escaped slashes as separators; every check below applies to it.
    pub(crate) separated: Cow<'a, str>,
    /// Escaped slashes kept as text inside their segment (`%2F`), present only
    /// when there were any.
    pub(crate) literal: Option<String>,
}

/// Common paths borrow the raw URI. Escaped text is decoded once for ACL
/// comparison only, so `/adm%69n` cannot bypass `/admin`. Escaped slashes
/// are read both ways (see [`CompiledAcl::check`]); nested escapes, escaped
/// backslashes and delimiters, dot segments and alternate separators fail
/// closed instead of relying on a downstream framework's normalization rules.
pub(crate) fn comparison_paths(
    raw: &str
) -> Result<ComparisonPaths<'_>, RequestRejection> {
    let (path, literal) = if raw.contains('%') {
        let mut decoded = Vec::with_capacity(raw.len());
        let mut literal = Vec::with_capacity(raw.len());
        let mut slashes = false;
        let mut bytes = raw.bytes();
        while let Some(byte) = bytes.next() {
            if byte == b'%' {
                let high = bytes
                    .next()
                    .and_then(hex)
                    .ok_or(RequestRejection::AmbiguousPath)?;
                let low = bytes
                    .next()
                    .and_then(hex)
                    .ok_or(RequestRejection::AmbiguousPath)?;
                let byte = high * 16 + low;
                if matches!(byte, b'\\' | b'%' | b'?' | b'#') {
                    return Err(RequestRejection::AmbiguousPath);
                }
                decoded.push(byte);
                if byte == b'/' {
                    slashes = true;
                    literal.extend_from_slice(b"%2F");
                } else {
                    literal.push(byte);
                }
            } else {
                decoded.push(byte);
                literal.push(byte);
            }
        }
        let decoded = String::from_utf8(decoded)
            .map_err(|_| RequestRejection::AmbiguousPath)?;
        let literal = if slashes {
            Some(
                String::from_utf8(literal)
                    .map_err(|_| RequestRejection::AmbiguousPath)?,
            )
        } else {
            None
        };
        (Cow::Owned(decoded), literal)
    } else {
        (Cow::Borrowed(raw), None)
    };
    if !path.starts_with('/')
        || path.contains(['\\', '?', '#'])
        || path.contains("//")
        || path.bytes().any(|b| b.is_ascii_control())
        || path.split('/').any(|part| matches!(part, "." | ".."))
    {
        return Err(RequestRejection::AmbiguousPath);
    }
    Ok(ComparisonPaths { separated: path, literal })
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
#[path = "../../../../tests/pipeline/host/access/acl.rs"]
mod tests;
