use super::{Operation, Resource};

/// The relationship owner is an opaque subject or role, not a user CRUD dependency.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Owner {
    User,
    Role,
}

/// Explicit access relationships, separate from editable profile fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Relation {
    UserRoles,
    UserPermissions,
    RolePermissions,
}

impl Relation {
    pub const fn owner(self) -> Owner {
        match self {
            Self::UserRoles | Self::UserPermissions => Owner::User,
            Self::RolePermissions => Owner::Role,
        }
    }

    pub const fn member(self) -> Resource {
        match self {
            Self::UserRoles => Resource::Role,
            Self::UserPermissions | Self::RolePermissions => {
                Resource::Permission
            }
        }
    }
}

/// Nonempty public reference in the relationship's owner resource.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssignmentKey {
    pub relation: Relation,
    pub reference: String,
}

/// The complete, explicitly exposed set of assignments, with an independent
/// nonempty revision. References belong to `key.relation.member()`.
/// This is not a flattened list of effective/inherited permissions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignments {
    pub key: AssignmentKey,
    pub revision: String,
    pub members: Vec<String>,
}

/// Discoverable relationship operations, independently of user CRUD schemas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssignmentSchema {
    pub relation: Relation,
    pub revision: String,
    pub operations: Vec<Operation>,
}
