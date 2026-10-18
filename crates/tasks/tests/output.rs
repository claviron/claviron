use super::*;

#[test]
fn embedded_output_uses_the_command_log_shape_and_task_cwd() {
    let root = tempfile::tempdir().unwrap();
    let config: LogConfig = serde_yaml::from_str(
        "stdout: logs/worker.log\nstderr: logs/error.log\n",
    )
    .unwrap();
    let output = config.open(Some(root.path()), "worker");

    assert!(output.write_stdout(b"worker\n"));
    assert!(output.write_stderr(b"error\n"));
    assert!(output.flush_stdout());
    assert!(output.flush_stderr());

    assert_eq!(
        std::fs::read(root.path().join("logs/worker.log")).unwrap(),
        b"worker\n"
    );
    assert_eq!(
        std::fs::read(root.path().join("logs/error.log")).unwrap(),
        b"error\n"
    );
}

#[test]
fn one_path_shares_both_embedded_streams() {
    let root = tempfile::tempdir().unwrap();
    let config: LogConfig = serde_yaml::from_str("worker.log\n").unwrap();
    let output = config.open(Some(root.path()), "worker");

    assert!(output.write_stdout(b"out\n"));
    assert!(output.write_stderr(b"err\n"));
    assert!(output.flush_stdout());

    assert_eq!(
        std::fs::read(root.path().join("worker.log")).unwrap(),
        b"out\nerr\n"
    );
}
