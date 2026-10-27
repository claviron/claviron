use http::StatusCode;

use super::{decode_relative_path, serve_index};
use crate::site::FileSite;

#[test]
fn spa_options_preserve_scalar_entry_and_use_existing_cache_policy() {
    use crate::caching::policy::CachePolicy;
    use claviron_http::config::spa::SpaConfig;

    let scalar: SpaConfig = serde_yaml::from_str("home.html").unwrap();
    assert_eq!(scalar.entry, std::path::Path::new("home.html"));
    assert!(!scalar.reject_ajax);
    let legacy = CachePolicy::spa(scalar.caching.as_ref());
    let existing = CachePolicy::dynamic_server_cache();
    assert_eq!(legacy.server_ttl_secs, existing.server_ttl_secs);
    assert_eq!(legacy.client_ttl_secs, existing.client_ttl_secs);

    let options: SpaConfig = serde_yaml::from_str(
        "entry: home.html\nreject_ajax: true\ncaching:\n  client: 0s\n  server: 2m\n  validate: false\n",
    ).unwrap();
    assert!(options.reject_ajax);
    let policy = CachePolicy::spa(options.caching.as_ref());
    assert_eq!(policy.server_ttl_secs, Some(120));
    assert_eq!(policy.client_ttl_secs, Some(0));
    assert!(!policy.validate_timestamps);
    assert!(
        serde_yaml::from_str::<SpaConfig>(
            "entry: home.html\nreject_ajaz: true"
        )
        .is_err()
    );
}

#[tokio::test]
async fn spa_ajax_rejection_is_json_not_the_html_shell() {
    use http_body_util::BodyExt;

    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::ACCEPT,
        http::HeaderValue::from_static("application/json"),
    );
    assert!(!super::is_ajax(&headers), "Accept alone is not an AJAX marker");
    headers.insert(
        "x-requested-with",
        http::HeaderValue::from_static("xmlhttprequest"),
    );
    assert!(super::is_ajax(&headers));
    let response = super::reject_spa_ajax();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response.headers()[http::header::CONTENT_TYPE],
        "application/json"
    );
    assert_eq!(response.headers()[http::header::CACHE_CONTROL], "no-store");
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], br#"{"message":"Not Found"}"#);
}

/// `index: [index.php, index.html]` is the ordinary shape for a host that
/// serves a front controller with a static fallback, and it used to answer
/// 500 on every directory request: `has_static_index()` admitted the list
/// because `index.html` is static, then the loop hit `index.php` first and
/// treated `DynamicFile` as a logic error. The file service runs ahead of
/// PHP in the bypass chain, so that `Err` ended the request.
///
/// Pinned because the fix is a name-only `continue` that nothing would
/// fail to compile without.
#[tokio::test]
async fn serve_index_skips_php_candidates_instead_of_failing_the_request() {
    let dir = std::env::temp_dir()
        .join(format!("claviron-index-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("index.html"), b"static index").unwrap();

    let cfg = Site {
        root: dir.clone(),
        index: vec!["index.php".into(), "index.html".into()],
    };
    assert!(cfg.has_static_index(), "the list must still reach serve_index");

    let uri: http::Uri = "/".parse().unwrap();
    let resp = serve_index(&cfg as &dyn crate::FileSite, &uri)
        .await
        .expect("index.php must be skipped, not fatal");
    assert_eq!(resp.status(), StatusCode::OK);

    std::fs::remove_dir_all(&dir).ok();
}

/// The decoder is the only thing standing between a request target and
/// `Path::join`. Each of these is a rejection a refactor could drop without
/// breaking a build, so they are pinned by name rather than by shape.
#[test]
fn decode_relative_path_rejects_what_must_not_reach_the_filesystem() {
    // Traversal, encoded and plain. `serve_index` used to take the plain
    // form straight to disk.
    assert_eq!(decode_relative_path("a/%2e%2e/b"), None);
    assert_eq!(decode_relative_path("a/../b"), None);
    assert_eq!(decode_relative_path(".."), None);
    assert_eq!(decode_relative_path("%2E%2E/etc/passwd"), None);

    // An encoded separator stays inside its segment or the request is
    // refused — it never becomes one.
    assert_eq!(decode_relative_path("a%2fb"), None);
    assert_eq!(decode_relative_path("a%2f%2e%2e%2fb"), None);
    assert_eq!(decode_relative_path("a%5cb"), None);

    // Dotfiles, before and after decoding.
    assert_eq!(decode_relative_path(".env"), None);
    assert_eq!(decode_relative_path("%2Eenv"), None);
    assert_eq!(decode_relative_path("public/.git/config"), None);

    // NUL truncation, and bytes that are not a path at all.
    assert_eq!(decode_relative_path("app%00.png"), None);
    assert_eq!(decode_relative_path("%FF%FE"), None);
}

#[test]
fn decode_relative_path_serves_names_the_raw_check_refused() {
    // The reason this is not only a hardening change: these are ordinary
    // files that the old encoded-form check made unreachable.
    assert_eq!(
        decode_relative_path("my%20file.txt").as_deref(),
        Some("my file.txt")
    );
    assert_eq!(
        decode_relative_path("img/a%2Bb.png").as_deref(),
        Some("img/a+b.png")
    );
    assert_eq!(
        decode_relative_path("%C3%BCber.css").as_deref(),
        Some("über.css")
    );

    // A dot that is not a leading dot is a normal part of a name.
    assert_eq!(
        decode_relative_path("app.min.js").as_deref(),
        Some("app.min.js")
    );

    // Trailing slash (the `serve_index` shape) and the bare root survive.
    assert_eq!(decode_relative_path("docs/").as_deref(), Some("docs/"));
    assert_eq!(decode_relative_path("").as_deref(), Some(""));
}

/// A site with a root and an index list, nothing else — what `serve_index`
/// reads.
struct Site {
    root: std::path::PathBuf,
    index: Vec<String>,
}

impl crate::FileSite for Site {
    fn root(&self) -> std::path::PathBuf {
        self.root.clone()
    }
    fn indexes(&self) -> &[String] {
        &self.index
    }
    fn try_files(&self) -> bool {
        false
    }
    fn spa(&self) -> Option<&str> {
        None
    }
    fn statics(
        &self
    ) -> Option<&[claviron_http::config::statics::StaticConfig]> {
        None
    }
    fn has_static_index(&self) -> bool {
        self.index
            .iter()
            .any(|name| !claviron_http::config::statics::is_dynamic_file(name))
    }
}

#[tokio::test]
async fn regression_subsecond_updates_do_not_return_stale_not_modified() {
    use crate::caching::{condition::ConditionalRequest, policy::CachePolicy};
    use http::header::{ETAG, IF_NONE_MATCH};
    use http_body_util::BodyExt;
    use std::time::{Duration, UNIX_EPOCH};

    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.path().to_path_buf();
    let epoch = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut previous = String::new();
    for (nanos, bytes) in [(100, b"AAAA"), (200, b"BBBB")] {
        std::fs::write(&path, bytes).unwrap();
        let modified = epoch + Duration::from_nanos(nanos);
        file.as_file()
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .unwrap();
        assert_eq!(
            file.as_file().metadata().unwrap().modified().unwrap(),
            modified
        );
        let mut headers = http::HeaderMap::new();
        if !previous.is_empty() {
            headers.insert(IF_NONE_MATCH, previous.parse().unwrap());
        }
        let response = super::serve_with_policy_and_condition(
            &path,
            path.parent().unwrap(),
            CachePolicy::no_cache(),
            ConditionalRequest::from_headers(&headers),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let current = response.headers()[ETAG].to_str().unwrap().to_owned();
        assert!(current.starts_with("W/"), "metadata is a weak validator");
        assert_ne!(current, previous);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes().as_ref(),
            bytes
        );
        previous = current;
    }
    let mut headers = http::HeaderMap::new();
    headers.insert(IF_NONE_MATCH, previous.parse().unwrap());
    let response = super::serve_with_policy_and_condition(
        &path,
        path.parent().unwrap(),
        CachePolicy::no_cache(),
        ConditionalRequest::from_headers(&headers),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn regression_empty_disk_suffix_returns_a_coherent_empty_representation()
{
    use crate::caching::{condition::ConditionalRequest, policy::CachePolicy};
    use http::header::{CONTENT_LENGTH, CONTENT_RANGE, RANGE};
    use http_body_util::BodyExt;
    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.path().to_path_buf();
    for (range, status) in [
        ("bytes=-1", StatusCode::OK),
        ("bytes=-0", StatusCode::RANGE_NOT_SATISFIABLE),
    ] {
        let mut headers = http::HeaderMap::new();
        headers.insert(RANGE, range.parse().unwrap());
        let response = crate::stream::stream_from_disk(
            &path,
            &file.as_file().metadata().unwrap(),
            &CachePolicy::no_cache(),
            &ConditionalRequest::from_headers(&headers),
        )
        .await;
        assert_eq!(response.status(), status);
        if status == StatusCode::OK {
            assert!(!response.headers().contains_key(CONTENT_RANGE));
            assert_eq!(response.headers()[CONTENT_LENGTH], "0");
        } else {
            assert_eq!(response.headers()[CONTENT_RANGE], "bytes */0");
        }
        assert!(
            response.into_body().collect().await.unwrap().to_bytes().is_empty()
        );
    }
}
