use std::any::Any;
use std::borrow::Cow;
use std::num::NonZeroU32;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use super::*;
use crate::Authenticated;

struct Actor;

impl Authenticated for Actor {
    fn auth_subject(&self) -> Cow<'_, str> {
        Cow::Borrowed("operator")
    }

    fn auth_identifier(&self) -> &str {
        "operator"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn object(value: Value) -> Document {
    value.as_object().unwrap().clone()
}

// An application-owned read-only projection, without a database or runtime.
struct Directory;

#[async_trait]
impl UserManager for Directory {
    async fn schema(
        &self,
        actor: &dyn Authenticated,
        resource: Resource,
    ) -> Result<Schema> {
        if actor.auth_subject() != "operator" {
            return Err(ManagementError::Forbidden);
        }
        if resource != Resource::User {
            return Err(ManagementError::Unsupported);
        }
        Ok(Schema {
            resource,
            revision: "schema-v1".into(),
            record: object(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": { "label": { "type": "string" } },
                "additionalProperties": false
            })),
            create: None,
            update: None,
            operations: vec![Operation::List, Operation::Read],
        })
    }

    async fn list(
        &self,
        actor: &dyn Authenticated,
        request: List,
    ) -> Result<Page> {
        self.schema(actor, request.resource).await?;
        Ok(Page {
            records: vec![self.get(actor, key()).await?],
            next_cursor: None,
        })
    }

    async fn get(
        &self,
        actor: &dyn Authenticated,
        key: RecordKey,
    ) -> Result<Record> {
        self.schema(actor, key.resource).await?;
        if key.reference != "public-user" {
            return Err(ManagementError::NotFound);
        }
        Ok(Record {
            key,
            revision: "record-v1".into(),
            data: object(json!({ "label": "Example user" })),
        })
    }
}

fn key() -> RecordKey {
    RecordKey { resource: Resource::User, reference: "public-user".into() }
}

#[tokio::test]
async fn user_management_accepts_a_model_without_email_or_database_id() {
    let manager: Arc<dyn UserManager> = Arc::new(Directory);
    let schema = manager.schema(&Actor, Resource::User).await.unwrap();
    assert!(
        !schema.record["properties"].as_object().unwrap().contains_key("email")
    );
    let page = manager
        .list(
            &Actor,
            List {
                resource: Resource::User,
                page_size: NonZeroU32::new(1).unwrap(),
                cursor: None,
                query: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records[0].key.reference, "public-user");
    assert_eq!(
        page.records[0].data,
        object(json!({ "label": "Example user" }))
    );
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn user_management_optional_writes_fail_closed() {
    let manager: &dyn UserManager = &Directory;
    assert!(matches!(
        manager
            .create(
                &Actor,
                Create {
                    resource: Resource::User,
                    schema_revision: "schema-v1".into(),
                    data: Document::new(),
                }
            )
            .await,
        Err(ManagementError::Unsupported)
    ));
    assert!(matches!(
        manager
            .update(
                &Actor,
                Update {
                    key: key(),
                    revision: "record-v1".into(),
                    schema_revision: "schema-v1".into(),
                    changes: Document::new(),
                }
            )
            .await,
        Err(ManagementError::Unsupported)
    ));
    assert_eq!(
        manager
            .delete(&Actor, Delete { key: key(), revision: "record-v1".into() })
            .await,
        Err(ManagementError::Unsupported)
    );

    let key = AssignmentKey {
        relation: Relation::UserRoles,
        reference: "public-user".into(),
    };
    assert_eq!(
        manager.assignments(&Actor, key.clone()).await,
        Err(ManagementError::Unsupported)
    );
    assert_eq!(
        manager
            .replace_assignments(
                &Actor,
                ReplaceAssignments {
                    key,
                    revision: "assignments-v1".into(),
                    members: vec![],
                }
            )
            .await,
        Err(ManagementError::Unsupported)
    );
}

#[test]
fn user_management_changes_preserve_absent_null_and_exact_integer_values() {
    let changes: Document = serde_json::from_str(
        r#"{"birth_date":null,"enabled":false,"counter":18446744073709551615}"#,
    )
    .unwrap();
    assert!(!changes.contains_key("label"));
    assert_eq!(changes["birth_date"], Value::Null);
    assert_eq!(changes["enabled"], false);
    assert_eq!(changes["counter"].as_u64(), Some(u64::MAX));
}

#[test]
fn user_management_relations_have_explicit_resource_boundaries() {
    assert_eq!(Relation::UserRoles.owner(), Resource::User);
    assert_eq!(Relation::UserRoles.member(), Resource::Role);
    assert_eq!(Relation::RolePermissions.owner(), Resource::Role);
    assert_eq!(Relation::RolePermissions.member(), Resource::Permission);
    assert_eq!(Relation::UserPermissions.owner(), Resource::User);
    assert_eq!(Relation::UserPermissions.member(), Resource::Permission);
}

#[test]
fn user_management_validation_feedback_keeps_field_paths_and_safe_messages() {
    let error = ManagementError::Validation(vec![Violation {
        path: "/email".into(),
        code: "already_used".into(),
        message: "This address is already assigned.".into(),
    }]);
    assert_eq!(error.to_string(), "management input failed validation");
    let ManagementError::Validation(errors) = error else { unreachable!() };
    assert_eq!(errors[0].path, "/email");
    assert_eq!(errors[0].code, "already_used");
}
