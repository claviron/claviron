use serde_json::json;

use super::*;

#[tokio::test]
async fn local_invocation_uses_the_registered_typed_handler() {
    let router = InvocationRouter::new();
    router.register(Ping { worker: Some(3) }).unwrap();

    let response = router
        .invoke("claviron.ping.v1", json!({ "echo": { "ready": true } }))
        .await
        .unwrap();

    assert_eq!(response["worker"], 3);
    assert_eq!(response["echo"], json!({ "ready": true }));
    assert!(router.invoke("missing", json!(null)).await.is_err());
}
