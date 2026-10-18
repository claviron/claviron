//! Schema-independent authenticated subject contract.

use std::any::Any;
use std::borrow::Cow;

use serde_json::{Map, Value};

/// A subject that has already been authenticated by an application.
///
/// The contract deliberately contains no password, ORM entity, UUID type, or
/// database connection. Applications decide how subjects are stored and use
/// [`Self::auth_subject`] as the stable external identifier carried by tokens.
pub trait Authenticated: Any + Send + Sync + 'static {
    /// Stable external subject, normally serialized as JWT `sub`.
    fn auth_subject(&self) -> Cow<'_, str>;

    /// Human-readable login/diagnostic identifier.
    fn auth_identifier(&self) -> &str;

    /// Display label for optional presentation-layer projections.
    fn auth_display_name(&self) -> &str {
        self.auth_identifier()
    }

    /// Optional application-local numeric key.
    ///
    /// Auth and JWT do not interpret this value. Consumers such as chat may
    /// use it for their own relational storage when the host application has
    /// such a key.
    fn auth_local_id(&self) -> Option<i64> {
        None
    }

    /// Optional application-defined claims added by token issuers.
    fn extra_claims(&self) -> Map<String, Value> {
        Map::new()
    }

    /// Runtime downcast hook for a composition root that needs its concrete
    /// application model after generic authentication middleware ran.
    fn as_any(&self) -> &dyn Any;
}

impl dyn Authenticated {
    /// Borrow the application-specific concrete subject when its type is
    /// known by the downstream composition root.
    pub fn downcast_ref<T: Authenticated>(&self) -> Option<&T> {
        self.as_any().downcast_ref::<T>()
    }
}
