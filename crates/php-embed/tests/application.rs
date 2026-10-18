use std::time::Duration;

use super::*;
use crate::test_support::ScriptFixture;
use crate::{ApplicationPool, PhpEmbedRuntime};
use claviron_http::empty_body;
use claviron_runtime::AppState;

#[test]
fn configuration_is_local_and_validated() {
    let script = ScriptFixture::new();
    let duplicate = ScriptFixture::new();
    let app = PhpApplication::new("web", script.path())
        .pool(ApplicationPool::elastic().min_idle(2).max_idle(2).max_size(8));
    assert!(app.validate().is_ok());
    assert!(app.clone().pool(ApplicationPool::fixed(0)).validate().is_err());
    assert!(
        app.clone()
            .pool(ApplicationPool::fixed(1).backoff(Duration::ZERO))
            .validate()
            .is_err()
    );
    let state = AppState::standalone("application-catalog-test", ".");
    PhpEmbedRuntime::builder()
        .application(PhpApplication::new("web", script.path()))
        .build()
        .register(&state)
        .unwrap();
    let runtime = state.registry_ref().resolve::<PhpEmbedRuntime>().unwrap();
    assert!(
        runtime
            .register_application(PhpApplication::new("web", duplicate.path()))
            .is_err()
    );
}

// #[test]
// fn persistent_application_isolated_process() {
//     const CHILD: &str = "CLAVIRON_APPLICATION_TEST_CHILD";
//     if std::env::var_os(CHILD).is_none() {
//         let (_, module) = module_path!().split_once("::").unwrap();
//         let ini = tempfile::tempdir().unwrap();
//         std::fs::write(
//             ini.path().join("php.ini"),
//             "max_execution_time=0\ndisplay_errors=0\nlog_errors=1\n"
//         )
//         .unwrap();
//         let output = std::process::Command::new(std::env::current_exe().unwrap())
//             .args([
//                 "--exact",
//                 &format!("{module}::persistent_application_isolated_process"),
//                 "--nocapture"
//             ])
//             .env(CHILD, "1")
//             .env("PHPRC", ini.path())
//             .env("PHP_INI_SCAN_DIR", "")
//             .output()
//             .unwrap();
//         assert!(
//             output.status.success(),
//             "{}\n{}",
//             String::from_utf8_lossy(&output.stdout),
//             String::from_utf8_lossy(&output.stderr)
//         );
//         assert!(String::from_utf8_lossy(&output.stdout).contains("test result: ok. 1 passed;"));
//         return;
//     }
//     tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//         tokio::time::timeout(Duration::from_secs(25), exercise()).await.unwrap();
//     });
// }

#[allow(
    dead_code,
    reason = "used by the temporarily disabled isolated-process test"
)]
async fn wait_for(mut predicate: impl FnMut() -> bool) {
    while !predicate() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[allow(
    dead_code,
    reason = "used by the temporarily disabled isolated-process test"
)]
fn request(uri: &str) -> Request<RouteBody> {
    Request::builder()
        .uri(uri)
        .extension(claviron_http::policy::HttpPolicy::unlimited())
        .body(empty_body())
        .unwrap()
}

/// Interrupting one owner leaves every other one alone.
///
/// The three services share one process and one registry of PHP threads, so
/// "whose work is this" is a filter rather than a structural boundary — and a
/// filter is exactly the kind of thing that can be dropped in a refactor
/// without failing to compile. Before `ExecutionOwner` existed, the drain
/// signalled everything: one application's stop ended another's request, a
/// script request, or a daemon. Nothing but a test like this notices if that
/// comes back.
///
/// Two pools, a request held inside each, and an interrupt aimed at one of
/// them. It also covers the activity filter: a pool with no request in flight
/// is signalled zero times, because signalling an idle thread leaves flags
/// `arm()` never clears and the next request there dies of a timeout that was
/// never its own.
#[allow(
    dead_code,
    reason = "used by the temporarily disabled isolated-process test"
)]
async fn interrupt_is_scoped_to_its_owner(script: &std::path::Path) {
    use crate::engine::interrupt::interrupt_owned_by;

    let make = |name: &'static str, dir: &std::path::Path| {
        ApplicationWorkers::new(
            PhpApplication::new(name, script)
                .args(["literal argument"])
                .environment("APP_ENV", "application-test")
                .environment("TEST_DIR", dir.to_string_lossy())
                .pool(
                    ApplicationPool::fixed(1)
                        .maintenance_interval(Duration::from_millis(5)),
                ),
        )
        .unwrap()
    };

    let mine = tempfile::tempdir().unwrap();
    let theirs = tempfile::tempdir().unwrap();
    let mine_pool = Arc::new(make("scoped-mine", mine.path()));
    let theirs_pool = Arc::new(make("scoped-theirs", theirs.path()));

    for pool in [&mine_pool, &theirs_pool] {
        wait_for(|| {
            pool.maintain();
            pool.snapshot().ready == 1
        })
        .await;
    }

    let held_mine = {
        let pool = Arc::clone(&mine_pool);
        tokio::spawn(async move { pool.execute(request("/held")).await })
    };
    let held_theirs = {
        let pool = Arc::clone(&theirs_pool);
        tokio::spawn(async move { pool.execute(request("/held")).await })
    };
    wait_for(|| {
        mine.path().join("entered").exists()
            && theirs.path().join("entered").exists()
    })
    .await;

    assert_eq!(
        interrupt_owned_by(mine_pool.owner()),
        1,
        "one owner, one active execution"
    );
    assert!(
        held_mine.await.unwrap().is_err(),
        "the interrupted owner's request ends"
    );

    // The other pool never noticed. It is still holding its request, and it
    // finishes normally once released.
    assert!(
        !held_theirs.is_finished(),
        "an unrelated owner's request keeps running"
    );
    std::fs::write(theirs.path().join("release"), "yes").unwrap();
    assert!(
        held_theirs.await.unwrap().is_ok(),
        "an unrelated owner's request completes"
    );

    // Idle now, so there is nothing to interrupt.
    wait_for(|| {
        theirs_pool.maintain();
        theirs_pool.snapshot().ready == 1
    })
    .await;
    assert_eq!(
        interrupt_owned_by(theirs_pool.owner()),
        0,
        "an idle owner is never signalled"
    );

    for pool in [&mine_pool, &theirs_pool] {
        pool.stop();
        wait_for(|| pool.drained()).await;
    }
}
