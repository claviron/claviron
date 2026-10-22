use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Server {
    pub id: i32,
    pub name: String,
    pub created_at: OffsetDateTime,
    pub enabled: bool,
}
