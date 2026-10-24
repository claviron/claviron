//! Signed credential permission declarations carried as an application URI SAN.
//!
//! This namespace is an application format, not a standard X.509 permission
//! extension. Reading it does not establish trust or grant access.
use std::collections::BTreeSet;

use super::SSLError;
use crate::permission::{Permission, Permissions};

/// Application URI namespace. A versioned payload contains exact names separated
/// by commas; the permission-name grammar excludes commas and URI escapes.
pub const URI_PREFIX: &str = "urn:claviron:permissions:v1:";
const NAMESPACE: &str = "urn:claviron:permissions:";

pub fn is_permission_uri(uri: &str) -> bool {
    uri.starts_with(NAMESPACE)
}

pub fn to_uri(permissions: &Permissions) -> String {
    let names = permissions.iter().map(Permission::as_str).collect::<Vec<_>>();
    format!("{URI_PREFIX}{}", names.join(","))
}

/// Preserve absent versus explicitly empty scope. Reject duplicate declarations,
/// unsupported versions and malformed names instead of ignoring a restriction.
pub fn from_uris<'a>(
    uris: impl IntoIterator<Item = &'a str>
) -> Result<Option<Permissions>, SSLError> {
    let mut result = None;
    for uri in uris {
        if !is_permission_uri(uri) {
            continue;
        }
        if result.is_some() {
            return Err(SSLError::Other(
                "multiple certificate permission declarations".into(),
            ));
        }
        let names = uri.strip_prefix(URI_PREFIX).ok_or_else(|| {
            SSLError::Other("unsupported certificate permission format".into())
        })?;
        let mut permissions = BTreeSet::new();
        if !names.is_empty() {
            for name in names.split(',') {
                permissions.insert(
                    Permission::new(name)
                        .map_err(|error| SSLError::Other(error.to_string()))?,
                );
            }
        }
        result = Some(Permissions(permissions));
    }
    Ok(result)
}

#[cfg(test)]
#[path = "../../tests/permission_certificate.rs"]
mod tests;
