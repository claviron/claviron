use futures_util::FutureExt;

use super::*;

#[test]
fn current_thread_driver_preparation_creates_no_execution() {
    let driver = CurrentThreadDriver::default();
    let spec = ExecutionSpec::default()
        .with_executor(ExecutorSpec::Dedicated(ExecutorKind::CurrentThread));
    let first = driver.prepare(spec.clone()).unwrap();
    let second = driver.prepare(spec).unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(driver.domains.lock().unwrap().shared.is_empty());
    assert!(
        first
            .execute_child(
                Box::pin(async { Ok(()) }),
                crate::RunContext::new(Default::default()),
            )
            .now_or_never()
            .unwrap()
            .is_err()
    );
    assert!(driver.executions.lock().unwrap().is_empty());
    assert!(driver.drain().now_or_never().unwrap().is_ok());
    struct Domain;
    let shared = ExecutionSpec::default()
        .with_executor(ExecutorSpec::Shared(ExecutionDomain::of::<Domain>()));
    let first = driver.prepare(shared.clone()).unwrap();
    let second = driver.prepare(shared.clone()).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert!(driver.drain().now_or_never().unwrap().is_ok());
    assert!(driver.finalize().now_or_never().unwrap().is_ok());
    assert!(driver.prepare(shared).is_err());
}

#[test]
fn current_thread_driver_rejects_unimplemented_executor_and_poll_policies() {
    use crate::PollPriority;
    use std::num::NonZeroUsize;
    let driver = CurrentThreadDriver::default();
    let base = ExecutionSpec::default();
    let dedicated = base
        .clone()
        .with_executor(ExecutorSpec::Dedicated(ExecutorKind::CurrentThread));
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
        assert!(driver.prepare(spec).is_err());
    }
    assert!(driver.executions.lock().unwrap().is_empty());
}
