use super::*;
use async_trait::async_trait;
use std::{any::Any, borrow::Cow, sync::Arc};

struct Actor;
impl crate::Authenticated for Actor {
    fn auth_subject(&self) -> Cow<'_, str> {
        Cow::Borrowed("service:example")
    }
    fn auth_identifier(&self) -> &str {
        "example"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
struct Directory;
#[async_trait]
impl AuthorizationManager for Directory {
    async fn schema(
        &self,
        _: &dyn crate::Authenticated,
        resource: Resource,
    ) -> Result<Schema> {
        Ok(Schema {
            resource,
            revision: "schema-v1".into(),
            record: Document::new(),
            create: None,
            update: None,
            operations: vec![Operation::List, Operation::Read],
        })
    }
    async fn list(
        &self,
        _: &dyn crate::Authenticated,
        _: List,
    ) -> Result<Page> {
        Ok(Page { records: vec![], next_cursor: None })
    }
    async fn get(
        &self,
        _: &dyn crate::Authenticated,
        _: RecordKey,
    ) -> Result<Record> {
        Err(ManagementError::NotFound)
    }
}
#[tokio::test]
async fn authorization_management_needs_no_user_manager_or_database() {
    let manager: Arc<dyn AuthorizationManager> = Arc::new(Directory);
    for resource in [Resource::Role, Resource::Permission] {
        assert!(manager.schema(&Actor, resource).await.is_ok());
        let page = manager
            .list(
                &Actor,
                List {
                    resource,
                    page_size: 1.try_into().unwrap(),
                    cursor: None,
                    query: None,
                },
            )
            .await
            .unwrap();
        assert!(page.records.is_empty());
        assert!(matches!(
            manager
                .create(
                    &Actor,
                    Create {
                        resource,
                        schema_revision: "schema-v1".into(),
                        data: Document::new()
                    }
                )
                .await,
            Err(ManagementError::Unsupported)
        ));
    }
    let key = AssignmentKey {
        relation: Relation::UserRoles,
        reference: "opaque-subject".into(),
    };
    assert_eq!(
        manager.assignments(&Actor, key.clone()).await,
        Err(ManagementError::Unsupported)
    );
    assert_eq!(
        manager.assignment_schema(&Actor, key.relation).await,
        Err(ManagementError::Unsupported)
    );
    assert_eq!(
        manager
            .replace_assignments(
                &Actor,
                ReplaceAssignments {
                    key,
                    revision: "set-v1".into(),
                    members: vec![]
                }
            )
            .await,
        Err(ManagementError::Unsupported)
    );
}
#[test]
fn authorization_relations_keep_user_references_without_user_crud() {
    assert_eq!(Relation::UserRoles.owner(), Owner::User);
    assert_eq!(Relation::UserRoles.member(), Resource::Role);
    assert_eq!(Relation::UserPermissions.owner(), Owner::User);
    assert_eq!(Relation::UserPermissions.member(), Resource::Permission);
    assert_eq!(Relation::RolePermissions.owner(), Owner::Role);
    assert_eq!(Relation::RolePermissions.member(), Resource::Permission);
}
