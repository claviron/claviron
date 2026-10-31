use super::*;
use crate::ext::PathPrefix;
use http::{Method, Request};
use std::sync::atomic::{AtomicUsize, Ordering};

// Deliberately not Clone: selecting an owner only borrows the prepared table.
struct Owner(&'static str);

struct Check {
    calls: Arc<AtomicUsize>,
    allowed: bool,
}

impl RouteCondition for Check {
    fn accepts(
        &self,
        _: &RuleContext<'_>,
    ) -> bool {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.allowed
    }
}

fn entry(
    prefix: &str,
    owner: &'static str,
    conditions: Vec<Arc<dyn RouteCondition>>,
) -> (PathPrefix, ConfiguredRoute<Owner>) {
    (
        serde_yaml::from_str(prefix).unwrap(),
        ConfiguredRoute { pipeline: Owner(owner), conditions },
    )
}

fn select<'s, B>(
    dispatch: &'s RouteDispatch<Owner>,
    request: &Request<B>,
) -> &'s Owner {
    dispatch.select(&RuleContext {
        method: request.method(),
        uri: request.uri(),
        headers: request.headers(),
    })
}

#[test]
fn only_the_longest_segment_entry_is_inspected() {
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatch = RouteDispatch {
        entries: Arc::new(RouteTable::new([
            entry(
                "/api",
                "parent",
                vec![Arc::new(Check { calls: calls.clone(), allowed: false })],
            ),
            entry("/api/users", "users", Vec::new()),
        ])),
        fallback: Owner("fallback"),
        rejected: Owner("rejected"),
    };
    let request =
        Request::builder().uri("/api/users/1?view=summary").body(()).unwrap();
    let owner = select(&dispatch, &request);
    assert_eq!(owner.0, "users");
    assert!(std::ptr::eq(
        owner,
        &dispatch.entries.find("/api/users").unwrap().pipeline,
    ));
    let request = Request::builder().uri("/apix").body(()).unwrap();
    assert!(std::ptr::eq(select(&dispatch, &request), &dispatch.fallback));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[test]
fn a_rejected_entry_does_not_try_a_parent_or_later_conditions() {
    let parent = Arc::new(AtomicUsize::new(0));
    let denied = Arc::new(AtomicUsize::new(0));
    let later = Arc::new(AtomicUsize::new(0));
    let dispatch = RouteDispatch {
        entries: Arc::new(RouteTable::new([
            entry(
                "/",
                "parent",
                vec![Arc::new(Check { calls: parent.clone(), allowed: true })],
            ),
            entry(
                "/private",
                "private",
                vec![
                    Arc::new(Check { calls: denied.clone(), allowed: false }),
                    Arc::new(Check { calls: later.clone(), allowed: true }),
                ],
            ),
        ])),
        fallback: Owner("fallback"),
        rejected: Owner("rejected"),
    };
    let request = Request::builder().uri("/private/item").body(()).unwrap();
    assert!(std::ptr::eq(select(&dispatch, &request), &dispatch.rejected));
    assert_eq!(parent.load(Ordering::Relaxed), 0);
    assert_eq!(denied.load(Ordering::Relaxed), 1);
    assert_eq!(later.load(Ordering::Relaxed), 0);
}

struct WriteOnly;

impl RouteCondition for WriteOnly {
    fn accepts(
        &self,
        request: &RuleContext<'_>,
    ) -> bool {
        *request.method == Method::POST
            && request.uri.query() == Some("mode=write")
            && request
                .headers
                .get("x-example-mode")
                .is_some_and(|value| value == "write")
    }
}

struct OriginalBody {
    bytes: Box<[u8]>,
    dropped: Arc<AtomicUsize>,
}

impl Drop for OriginalBody {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn selection_borrows_the_head_without_retaining_or_adapting_the_body() {
    let dispatch = RouteDispatch {
        entries: Arc::new(RouteTable::new([entry(
            "/api",
            "writer",
            vec![Arc::new(WriteOnly)],
        )])),
        fallback: Owner("fallback"),
        rejected: Owner("rejected"),
    };
    let dropped = Arc::new(AtomicUsize::new(0));
    let bytes = vec![0, 255, 13, 10].into_boxed_slice();
    let address = bytes.as_ptr();
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api?mode=write")
        .header("x-example-mode", "write")
        .body(OriginalBody { bytes, dropped: dropped.clone() })
        .unwrap();
    let owner = select(&dispatch, &request);
    let (head, body) = request.into_parts();
    // The table's selected borrow survives moving the original request.
    assert_eq!(owner.0, "writer");
    assert_eq!(head.uri.path_and_query().unwrap().as_str(), "/api?mode=write");
    assert_eq!(body.bytes.as_ptr(), address);
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(body);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);

    let request = Request::builder().uri("/api?mode=write").body(()).unwrap();
    assert!(std::ptr::eq(select(&dispatch, &request), &dispatch.rejected));
}
