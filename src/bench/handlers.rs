// Native workload implementation. Adapters own only registration/selection.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::Query;
use axum::http::header::{
    CACHE_CONTROL, CONTENT_TYPE, HeaderName, HeaderValue,
};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use claviron_runtime::activity::ACTIVITY;

/// What the caller wants this route to do: how long to take, and how much
/// to send back.
#[derive(serde::Deserialize, Default)]
pub struct Ask {
    min: Option<u64>,
    max: Option<u64>,
    payload: Option<usize>,
}

impl Ask {
    /// What to wait, or nothing when neither bound was given.
    fn duration(&self) -> Option<Duration> {
        let (low, high) = match (self.min, self.max) {
            (None, None) => return None,
            (Some(only), None) => (only, only),
            (None, Some(high)) => (0, high),
            (Some(low), Some(high)) if low > high => (high, low),
            (Some(low), Some(high)) => (low, high),
        };
        Some(Duration::from_millis(if low == high {
            low
        } else {
            low + spread(high - low + 1)
        }))
    }
}

/// Largest synthetic fixture in the shared native workload contract.
/// All adapters expose this same one-MiB fixture ceiling for comparable runs;
/// it is not a transport body limit or deployment admission policy.
const PAYLOAD_MAX: usize = 1024 * 1024;

/// The whole reply for a message of `bytes` characters, built once per size.
///
/// Built once and kept, rather than formatted per request: a benchmark must
/// not spend the CPU it is trying to measure, and a `format!` of a megabyte
/// on every request would be most of what the run reported. A process sees
/// one or two sizes in its life, so the handful of strings this keeps are
/// kept for good on purpose.
fn body(bytes: usize) -> &'static str {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    static ALPHABET: &[u8] =
        b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    static SOURCE: OnceLock<Vec<u8>> = OnceLock::new();
    static BODIES: OnceLock<Mutex<HashMap<usize, &'static str>>> =
        OnceLock::new();

    let bytes = bytes.min(PAYLOAD_MAX);
    let mut bodies = BODIES
        .get_or_init(Default::default)
        .lock()
        .expect("the body cache is never poisoned");
    bodies.entry(bytes).or_insert_with(|| {
        // Printable and unpatterned, so nothing compresses it or optimises
        // it away. None of these characters needs escaping in JSON.
        let source = SOURCE.get_or_init(|| {
            (0..PAYLOAD_MAX)
                .map(|_| ALPHABET[spread(ALPHABET.len() as u64) as usize])
                .collect()
        });
        let message = std::str::from_utf8(&source[..bytes])
            .expect("the alphabet is ASCII");
        Box::leak(
            format!(r#"{{"from":"rust","message":"{message}"}}"#)
                .into_boxed_str(),
        )
    })
}

/// A number below `bound`, cheaply and without a dependency.
///
/// xorshift over a counter seeded once per process. It is not random enough
/// to hide anything and does not need to be: its whole job is to stop a
/// thousand identical waits from ending at the same instant.
fn spread(bound: u64) -> u64 {
    static STATE: AtomicU64 = AtomicU64::new(0x2545_f491_4f6c_dd1d);
    let mut x = STATE.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x % bound
}

/// Relative native-baseline paths for an application-selected mount.
pub fn baseline_routes() -> Router {
    Router::new()
        .route("/rust", get(rust).post(rust))
        .route("/stats", get(stats))
}

/// One workload router for both Edge and standalone runners.
pub fn routes() -> Router {
    Router::new()
        .route("/bench/plain", get(plain))
        .route("/bench/json", get(json))
        .route("/bench/echo", post(echo))
        .nest("/_bench", baseline_routes())
}

async fn plain() -> &'static str {
    "ok\n"
}

#[derive(serde::Serialize)]
struct JsonReply {
    message: &'static str,
}

async fn json() -> Json<JsonReply> {
    Json(JsonReply { message: "ok" })
}

/// Preserve streaming ownership; the ingress keeps its body policy.
async fn echo(request: axum::extract::Request) -> axum::response::Response {
    let mut response = axum::response::Response::new(request.into_body());
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response
}

async fn rust(
    Query(ask): Query<Ask>,
    received: Bytes,
) -> axum::response::Response {
    // Read before waiting: the request body has to cross before there is
    // anything to answer, and a benchmark that slept first would time the
    // sleep instead of the crossing.
    let received = received.len();

    if let Some(waited) = ask.duration() {
        tokio::time::sleep(waited).await;
    }
    let json = match ask.payload {
        Some(bytes) => body(bytes),
        None => r#"{"from":"rust"}"#,
    };
    // `HeaderValue::from` writes the number into its own inline buffer, so
    // reporting the size costs no allocation. Formatting it would have cost
    // one per request, which is the benchmark paying for its own instrument.
    (
        [
            (CONTENT_TYPE, HeaderValue::from_static("application/json")),
            (CACHE_CONTROL, HeaderValue::from_static("no-store")),
            (
                HeaderName::from_static("x-received"),
                HeaderValue::from(received as u64),
            ),
        ],
        json,
    )
        .into_response()
}

async fn stats() -> impl IntoResponse {
    let body = format!(
        r#"{{"in_flight":{},"total":{},"sessions":{}}}"#,
        ACTIVITY.in_flight(),
        ACTIVITY.total(),
        ACTIVITY.sessions()
    );
    ([(CONTENT_TYPE, "application/json"), (CACHE_CONTROL, "no-store")], body)
}
