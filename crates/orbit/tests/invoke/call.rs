use super::*;

struct Echo;

#[async_trait]
impl Route for Echo {
    async fn call(
        &self,
        request: Request,
    ) -> Result<Vec<u8>> {
        assert_eq!(request.operation, "read");
        Ok(request.payload)
    }
}

#[tokio::test]
async fn invoke_client_uses_a_registered_route_without_transport_knowledge() {
    let mut client = Client::new();
    client.register("example-service", Arc::new(Echo)).unwrap();
    assert!(client.register("example-service", Arc::new(Echo)).is_err());
    assert_eq!(
        client
            .call("example-service", "read", b"value".to_vec())
            .await
            .unwrap(),
        b"value"
    );
    assert_eq!(
        client.call("unknown", "read", vec![]).await,
        Err(Error::UnknownTarget)
    );
}
