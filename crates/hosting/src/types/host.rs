use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vhost {
    pub id: i32,
    pub name: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VhostAlias {
    pub id: i32,
    pub vhost_id: i32,
    pub hostname: String,
    pub created_at: OffsetDateTime,
}

/// Storage-neutral import request used by config loaders and tools.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VhostImport {
    pub name: String,
    pub aliases: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedVhost {
    pub vhost: Vhost,
    pub aliases: Vec<VhostAlias>,
}
