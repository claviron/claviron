use std::sync::Arc;

use claviron_laravel::broadcast::BroadcastExtension;
use claviron_php_embed::extension::{FunctionEntries, PhpEmbedExtension};

#[path = "support/native_runtime.rs"]
mod native_runtime;

// Exercise the real function table and argument validation without a live
// broadcast fleet. Transport configuration is deliberately absent.
struct LaravelFunctions(BroadcastExtension);

impl PhpEmbedExtension for LaravelFunctions {
    fn name(&self) -> &'static str {
        "laravel-registration-test"
    }

    fn append_functions(
        &self,
        entries: &mut FunctionEntries,
    ) -> Result<(), String> {
        self.0.append_functions(entries)
    }
}

#[tokio::test]
async fn laravel_registration_and_exceptions_survive_zts_requests() {
    native_runtime::run_fixture(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/native_registration.php"),
        Some(Arc::new(LaravelFunctions(BroadcastExtension))),
    )
    .await;
}
