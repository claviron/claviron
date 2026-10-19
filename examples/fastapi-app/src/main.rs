mod app;

#[tokio::main]
async fn main() -> gufi::Result<()> {
    // The launch decides which doors open and whose word counts; the
    // application decides nothing about its own deployment. One visible line,
    // and `--help` explains itself.
    gufi::Runner::new("main:app")
        .with_options(gufi::GufiOptions::from_args())
        .with_interceptor(app::routes::rust_interceptor)
        .run(app::configure)
        .await
}
