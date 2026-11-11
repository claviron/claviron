use futures_util::FutureExt;

use super::*;

#[test]
fn current_thread_driver_preparation_creates_no_execution() {
    let driver = CurrentThreadDriver::default();
    let owner = ExecutionOwner::of::<()>("first");
    let spec = execution_spec(Execution::Dedicated);
    let prepared = driver.prepare(&owner, spec).unwrap();
    assert!(
        prepared
            .execute_child(
                Box::pin(async { Ok(()) }),
                crate::RunContext::new(Default::default()),
            )
            .now_or_never()
            .unwrap()
            .is_err()
    );
    assert!(driver.drain().now_or_never().unwrap().is_ok());
    struct Domain;
    let shared = ExecutionSpec::default()
        .with_executor(ExecutorSpec::Shared(ExecutionDomain::of::<Domain>()));
    let first = driver.prepare(&owner, shared.clone()).unwrap();
    let second = driver
        .prepare(&ExecutionOwner::of::<()>("second"), shared.clone())
        .unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert!(driver.finalize().now_or_never().unwrap().is_ok());
    assert!(driver.prepare(&owner, shared).is_err());
}

#[tokio::test]
async fn dedicated_executor_stays_with_its_owner_across_generations_and_drain()
{
    async fn run(prepared: &dyn PreparedExecution) -> std::thread::ThreadId {
        let (id, thread) = tokio::sync::oneshot::channel();
        prepared
            .execute(
                Box::pin(async move {
                    let current = std::thread::current();
                    assert_eq!(current.name(), Some("claviron-current-thread"));
                    id.send(current.id()).unwrap();
                    Ok(())
                }),
                crate::RunContext::new(Default::default()),
            )
            .await
            .unwrap();
        thread.await.unwrap()
    }

    let driver = CurrentThreadDriver::default();
    let first = ExecutionOwner::of::<()>("first");
    let second = ExecutionOwner::of::<()>("second");
    let spec = execution_spec(Execution::Dedicated);
    let initial = driver.prepare(&first, spec.clone()).unwrap();
    let restarted = driver.prepare(&first, spec.clone()).unwrap();
    let other = driver.prepare(&second, spec.clone()).unwrap();
    assert!(Arc::ptr_eq(&initial, &restarted));
    assert!(!Arc::ptr_eq(&initial, &other));

    let initial_thread = run(initial.as_ref()).await;
    driver.drain().await.unwrap();
    assert_eq!(initial_thread, run(restarted.as_ref()).await);
    assert_ne!(initial_thread, run(other.as_ref()).await);

    driver.finalize().await.unwrap();
    assert!(driver.prepare(&first, spec).is_err());
    assert!(
        restarted
            .execute(
                Box::pin(async { Ok(()) }),
                crate::RunContext::new(Default::default()),
            )
            .await
            .is_err()
    );
}

#[test]
fn current_thread_driver_rejects_unimplemented_executor_and_poll_policies() {
    use crate::PollPriority;
    use std::num::NonZeroUsize;
    let driver = CurrentThreadDriver::default();
    let owner = ExecutionOwner::of::<()>("owner");
    let base = execution_spec(Execution::Inherit);
    let dedicated = execution_spec(Execution::Dedicated);
    for spec in [
        base.clone(),
        base.with_executor(ExecutorSpec::Dedicated(
            ExecutorKind::MultiThread {
                threads: NonZeroUsize::new(2).unwrap(),
            },
        )),
        dedicated
            .with_scheduling(SchedulingPolicy::Priority(PollPriority::High)),
    ] {
        assert!(driver.prepare(&owner, spec).is_err());
    }
    assert!(driver.executors.lock().unwrap().residents().is_empty());
}
