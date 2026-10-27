use std::io::Write;

use http::header::CONTENT_LENGTH;
use http_body_util::BodyExt;

use super::stream_from_disk;
use crate::caching::condition::ConditionalRequest;
use crate::caching::policy::CachePolicy;

#[tokio::test]
async fn full_disk_body_stops_at_advertised_length_after_append() {
    for original in [b"".as_slice(), b"ORIGINAL".as_slice()] {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(original).unwrap();
        let path = file.path().to_path_buf();
        let meta = file.as_file().metadata().unwrap();
        let response = stream_from_disk(
            &path,
            &meta,
            &CachePolicy::default(),
            &ConditionalRequest::default(),
        )
        .await;
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            response.headers()[CONTENT_LENGTH],
            original.len().to_string()
        );

        // The body owns an open reader but has not been polled yet.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"TAIL")
            .unwrap();

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), original);
    }
}
