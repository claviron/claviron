use std::sync::Arc;
use std::time::Duration;

use claviron_orbit::{Fleet, InvocationBus};

use super::*;

#[test]
fn invocation_log_appends_in_each_programs_cwd() {
    let defaults: crate::PhpLaunchConfig = serde_yaml::from_str("{}").unwrap();
    assert_eq!(defaults.log, claviron_tasks::LogConfig::default());
    let config: crate::PhpLaunchConfig =
        serde_yaml::from_str("log: storage/logs/invoke.log").unwrap();
    assert!(config.validate().is_ok());
    let directory = tempfile::tempdir().unwrap();
    for app in ["first", "second"] {
        let cwd = directory.path().join(app);
        let log = cwd.join("storage/logs/invoke.log");
        std::fs::create_dir_all(log.parent().unwrap()).unwrap();
        std::fs::write(&log, format!("{app}\n")).unwrap();
        for (counter, explicit_cwd) in [(1, Some(cwd.clone())), (2, None)] {
            let invocation = PhpProgramInvocation {
                kind: "job".into(),
                script: cwd.join("artisan"),
                argv: vec![
                    "claviron:errand".into(),
                    "--output=jobs.log".into(),
                    "--append".into(),
                ],
                cwd: explicit_cwd,
                env: Vec::new(),
            };
            let decoded = PhpProgramInvocation::decode_invocation(
                &invocation.encode_invocation().unwrap(),
            )
            .unwrap();
            let id = InvocationId::from_net_id(orbitive::NetId64::make(
                241, 0, counter,
            ));
            let program =
                decoded.program(id, 0, 0, None).log(config.log.clone());
            assert_eq!(program.args, invocation.argv);
            let output =
                program.log.open(program.cwd.as_deref(), &program.name);
            assert!(output.write_stdout(b"stdout\n"));
            assert!(output.write_stderr(b"stderr\n"));
            assert!(output.flush_stdout());
            assert!(output.flush_stderr());
        }
        assert_eq!(
            std::fs::read_to_string(log).unwrap(),
            format!("{app}\nstdout\nstderr\nstdout\nstderr\n")
        );
    }
}

#[tokio::test]
async fn ring_reader_drains_a_burst_without_waiting_for_admission() {
    let fleet =
        Arc::new(Fleet::join("php-invocation-reader", 1).expect("fleet"));
    let bus = Arc::new(InvocationBus::new(fleet));
    let mut subscription =
        bus.clone().subscribe(OPERATION).expect("subscription");
    let cancellation = CancellationToken::new();
    let reader_cancellation = cancellation.clone();
    let (sender, receiver) = mpsc::channel(2);

    let reader = tokio::spawn(async move {
        receive_invocations(&mut subscription, sender, &reader_cancellation)
            .await
    });

    bus.submit(OPERATION, b"first").expect("first invocation");
    bus.submit(OPERATION, b"second").expect("second invocation");

    tokio::time::timeout(Duration::from_secs(1), async {
        while receiver.len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reader did not drain the ring burst");

    cancellation.cancel();
    reader.await.expect("reader task").expect("reader result");
}

#[tokio::test]
async fn ring_reader_continues_with_retained_invocations_after_lag() {
    let fleet =
        Arc::new(Fleet::join("php-invocation-reader-lag", 1).expect("fleet"));
    let bus = Arc::new(InvocationBus::new(fleet));
    let mut subscription =
        bus.clone().subscribe(OPERATION).expect("subscription");

    for sequence in 0..=INVOCATION_RING_CAPACITY {
        bus.submit(OPERATION, &sequence.to_le_bytes()).expect("invocation");
    }

    let cancellation = CancellationToken::new();
    let reader_cancellation = cancellation.clone();
    let (sender, receiver) = mpsc::channel(INVOCATION_RING_CAPACITY);
    let reader = tokio::spawn(async move {
        receive_invocations(&mut subscription, sender, &reader_cancellation)
            .await
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        while receiver.len() != INVOCATION_RING_CAPACITY {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reader stopped after reporting lag");

    cancellation.cancel();
    reader.await.expect("reader task").expect("reader result");
}
