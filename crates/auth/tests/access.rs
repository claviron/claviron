use super::*;
use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

struct Operator;
impl crate::Authenticated for Operator {
    fn auth_subject(&self) -> Cow<'_, str> {
        "opaque-subject".into()
    }
    fn auth_identifier(&self) -> &str {
        "operator"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
struct Policy;
#[async_trait]
impl AccessPolicy for Policy {
    async fn authorize(
        &self,
        peer: &PeerIdentity,
        target: &AccessTarget,
    ) -> Result<Option<AnyAuthenticated>, AccessError> {
        match (target.service.as_str(), target.operation.as_str()) {
            ("vpn", "connect") => Ok(None),
            ("grpc", "/example.Admin/Inspect")
                if peer.subject_uris == ["urn:example:subject:one"] =>
            {
                Ok(Some(Arc::new(Operator)))
            }
            ("grpc", _) => Err(AccessError::Forbidden),
            _ => Err(AccessError::Unsupported),
        }
    }
}
fn peer() -> PeerIdentity {
    PeerIdentity {
        certificate: Arc::from([]),
        subject_dn: "CN=operator".into(),
        issuer_dn: "CN=example CA".into(),
        common_names: vec!["operator".into()],
        subject_uris: vec!["urn:example:subject:one".into()],
    }
}

#[tokio::test]
async fn access_policy_distinguishes_service_access_from_application_binding() {
    let peer = peer();
    let vpn =
        AccessTarget { service: "vpn".into(), operation: "connect".into() };
    assert!(Policy.authorize(&peer, &vpn).await.unwrap().is_none());
    let inspect = AccessTarget {
        service: "grpc".into(),
        operation: "/example.Admin/Inspect".into(),
    };
    let actor = Policy.authorize(&peer, &inspect).await.unwrap().unwrap();
    assert_eq!(actor.auth_subject(), "opaque-subject");
    assert_eq!(actor.auth_identifier(), "operator");
    let reload = AccessTarget {
        service: "grpc".into(),
        operation: "/example.Admin/Reload".into(),
    };
    assert!(matches!(
        Policy.authorize(&peer, &reload).await,
        Err(AccessError::Forbidden)
    ));
}

#[tokio::test]
async fn access_identity_does_not_infer_binding_from_cn_or_issuer_name() {
    let mut peer = peer();
    peer.subject_uris.clear();
    let target = AccessTarget {
        service: "grpc".into(),
        operation: "/example.Admin/Inspect".into(),
    };
    assert!(matches!(
        Policy.authorize(&peer, &target).await,
        Err(AccessError::Forbidden)
    ));
    let unknown =
        AccessTarget { service: "custom".into(), operation: "inspect".into() };
    assert!(matches!(
        Policy.authorize(&peer, &unknown).await,
        Err(AccessError::Unsupported)
    ));
}
