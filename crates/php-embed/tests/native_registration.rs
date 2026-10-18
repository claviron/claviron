#[path = "support/native_runtime.rs"]
mod native_runtime;

#[tokio::test]
async fn native_registration_survives_zts_requests() {
    native_runtime::run_fixture(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/registration/native.php"),
        None,
    )
    .await;
}
