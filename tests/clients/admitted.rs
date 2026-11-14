use std::convert::Infallible;

use async_trait::async_trait;

use super::{AdmissionError, AdmittedPool, key};
use crate::clients::{ActiveRequestPolicy, ClientManager, PoolConfig};

struct Numbers;

#[async_trait]
impl ClientManager for Numbers {
    type Client = u32;
    type Error = Infallible;

    async fn create(&self) -> Result<u32, Infallible> {
        Ok(7)
    }

    async fn recycle(
        &self,
        _client: &mut u32,
    ) -> Result<(), Infallible> {
        Ok(())
    }
}

fn config(scope: &str) -> PoolConfig {
    serde_json::from_value(serde_json::json!({
        "scope": scope,
        "min_idle": 0,
        "max_idle": 1,
        "max_live": 1,
        "max_concurrency": 1,
        "max_waiting": 1,
    }))
    .unwrap()
}

#[test]
fn a_budget_key_is_the_same_for_every_process_naming_it() {
    assert_eq!(
        key("acme https://ca.example/directory"),
        key("acme https://ca.example/directory")
    );
    assert_ne!(
        key("acme https://ca.example/directory"),
        key("acme https://other.example/directory")
    );
}

#[tokio::test]
async fn a_process_pool_admits_locally() {
    let pool =
        AdmittedPool::new(None, "test", Numbers, &config("process")).unwrap();
    let lease = pool.acquire(ActiveRequestPolicy::Preserve).await.unwrap();
    assert_eq!(**lease, 7);
}

#[test]
fn a_fleet_pool_without_the_table_is_refused_not_narrowed() {
    let refused = AdmittedPool::new(None, "test", Numbers, &config("fleet"));
    assert!(matches!(refused, Err(AdmissionError::FleetUnavailable(_))));
}
