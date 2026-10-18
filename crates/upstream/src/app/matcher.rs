//! Owns runtime route selector matching and wildcard exclusions.

use regex::Regex;

#[derive(Clone, Debug)]
pub(crate) struct CompiledUpstreamRoute {
    pub(crate) profile: String,
    pub(crate) selector: RouteSelector,
    pub(crate) excludes: Vec<ExcludeMatcher>,
}

impl CompiledUpstreamRoute {
    pub(crate) fn matches(
        &self,
        path: &str,
    ) -> bool {
        self.selector.matches(path)
            && !self.excludes.iter().any(|exclude| exclude.matches(path))
    }
}

#[derive(Clone, Debug)]
pub(crate) enum RouteSelector {
    Exact(String),
    PathPrefix(String),
    Regex(Regex),
    RootFallback,
}

impl RouteSelector {
    pub(crate) fn matches(
        &self,
        path: &str,
    ) -> bool {
        match self {
            Self::Exact(exact) => path == exact,
            Self::PathPrefix(prefix) => {
                path == prefix
                    || path
                        .strip_prefix(prefix)
                        .is_some_and(|rest| rest.starts_with('/'))
            }
            Self::Regex(regex) => regex.is_match(path),
            Self::RootFallback => true,
        }
    }

    pub(crate) fn rank(&self) -> SelectorRank {
        match self {
            Self::Exact(_) => SelectorRank::Exact,
            Self::PathPrefix(_) => SelectorRank::Prefix,
            Self::Regex(_) => SelectorRank::Regex,
            Self::RootFallback => SelectorRank::Root,
        }
    }

    pub(crate) fn prefix_len(&self) -> usize {
        match self {
            Self::PathPrefix(prefix) => prefix.len(),
            _ => 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SelectorRank {
    Exact,
    Prefix,
    Regex,
    Root,
}

#[derive(Clone, Debug)]
pub(crate) enum ExcludeMatcher {
    Exact(String),
    Prefix(String),
    Wildcard { parts: Vec<String>, starts_anchored: bool, ends_anchored: bool },
}

impl ExcludeMatcher {
    pub(crate) fn matches(
        &self,
        path: &str,
    ) -> bool {
        match self {
            Self::Exact(exact) => path == exact,
            Self::Prefix(prefix) => {
                path == prefix
                    || path
                        .strip_prefix(prefix)
                        .is_some_and(|rest| rest.starts_with('/'))
            }
            Self::Wildcard { parts, starts_anchored, ends_anchored } => {
                wildcard_matches(path, parts, *starts_anchored, *ends_anchored)
            }
        }
    }
}

fn wildcard_matches(
    path: &str,
    parts: &[String],
    starts_anchored: bool,
    ends_anchored: bool,
) -> bool {
    if parts.is_empty() {
        return true;
    }

    if starts_anchored && !path.starts_with(&parts[0]) {
        return false;
    }
    if ends_anchored
        && !path.ends_with(parts.last().expect("parts is not empty"))
    {
        return false;
    }

    let mut cursor = 0;
    for (index, part) in parts.iter().enumerate() {
        if index == 0 && starts_anchored {
            cursor = part.len();
            continue;
        }
        let Some(relative) = path[cursor..].find(part) else {
            return false;
        };
        cursor += relative + part.len();
    }
    true
}
