use async_trait::async_trait;

use super::{
    AssignmentKey, AssignmentSchema, Assignments, Create, Delete, List,
    ManagementError, Page, Record, RecordKey, ReplaceAssignments, Resource,
    Result, Schema, Update,
};
use crate::Authenticated;

/// An instance-scoped management capability implemented by the application.
/// A provider manages roles, permissions and opaque subject assignments without teaching the
/// transport about guards, virtual hosts, types, or database predicates.
///
/// The host authenticates the actor before calling. Every method independently
/// enforces that actor's current authority and this instance's scope; neither
/// possession of a record reference nor a prior schema/list result is a grant.
/// Management scope must cover related role/permission writes too. Implementors
/// provide safe projections, prevent mass assignment, and reject invalid input
/// or stale revisions without partial mutations. Schema revisions and record
/// revisions are compared at the same boundary as the write.
///
/// Optional operations fail closed. Advertise only supported capabilities in
/// `schema`; an unavailable resource may return `Unsupported`. Providers own
/// validation and persistence. No default database, role model, actor mapping,
/// authentication fallback, registration, or background work is installed.
#[async_trait]
pub trait AuthorizationManager: Send + Sync + 'static {
    async fn schema(
        &self,
        actor: &dyn Authenticated,
        resource: Resource,
    ) -> Result<Schema>;

    async fn list(
        &self,
        actor: &dyn Authenticated,
        request: List,
    ) -> Result<Page>;

    async fn get(
        &self,
        actor: &dyn Authenticated,
        key: RecordKey,
    ) -> Result<Record>;

    async fn create(
        &self,
        _actor: &dyn Authenticated,
        _request: Create,
    ) -> Result<Record> {
        Err(ManagementError::Unsupported)
    }

    async fn update(
        &self,
        _actor: &dyn Authenticated,
        _request: Update,
    ) -> Result<Record> {
        Err(ManagementError::Unsupported)
    }

    async fn delete(
        &self,
        _actor: &dyn Authenticated,
        _request: Delete,
    ) -> Result<()> {
        Err(ManagementError::Unsupported)
    }

    async fn assignment_schema(
        &self,
        _actor: &dyn Authenticated,
        _relation: super::Relation,
    ) -> Result<AssignmentSchema> {
        Err(ManagementError::Unsupported)
    }

    async fn assignments(
        &self,
        _actor: &dyn Authenticated,
        _key: AssignmentKey,
    ) -> Result<Assignments> {
        Err(ManagementError::Unsupported)
    }

    async fn replace_assignments(
        &self,
        _actor: &dyn Authenticated,
        _request: ReplaceAssignments,
    ) -> Result<Assignments> {
        Err(ManagementError::Unsupported)
    }
}
