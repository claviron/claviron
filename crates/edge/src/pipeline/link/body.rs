//! The upload task belongs to its head wait, then to its streamed response.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use claviron_http::{RouteBody, RouteBodyError};
use claviron_orbit::transport::client::LentWrite;
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt;
use tokio::io::AsyncWriteExt;
use tokio_util::task::AbortOnDropHandle;

use super::websocket::Input;

/// Keep both directions independently driven, as on the original Link path.
/// Dropping the owner aborts only this upload. Input resets an unfinished
/// direction when the cancelled task drops it; a completed FIN is preserved.
pub(super) fn upload(
    body: RouteBody,
    input: LentWrite,
    whole: bool,
) -> AbortOnDropHandle<()> {
    // Construct the reset owner before spawning: cancellation before the task's
    // first poll must release the same input as cancellation during a write.
    let mut input = Input::new(input);
    AbortOnDropHandle::new(tokio::spawn(async move {
        if whole {
            if let Ok(collected) = body.collect().await
                && input.write_all(&collected.to_bytes()).await.is_ok()
            {
                let _ = input.shutdown().await;
            }
            return;
        }
        let mut body = body;
        while let Some(frame) = body.frame().await {
            let Ok(frame) = frame else {
                return;
            };
            let Ok(chunk) = frame.into_data() else {
                continue;
            };
            if input.write_all(&chunk).await.is_err() {
                return;
            }
        }
        let _ = input.shutdown().await;
    }))
}

pub(super) fn with_upload(
    body: RouteBody,
    upload: Option<AbortOnDropHandle<()>>,
) -> RouteBody {
    match upload {
        None => body,
        Some(upload) => {
            RouteBody::new(UploadingBody { body, upload: Some(upload) })
        }
    }
}

struct UploadingBody {
    body: RouteBody,
    upload: Option<AbortOnDropHandle<()>>,
}

impl Body for UploadingBody {
    type Data = Bytes;
    type Error = RouteBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RouteBodyError>>> {
        let frame = Pin::new(&mut self.body).poll_frame(cx);
        if matches!(&frame, Poll::Ready(None | Some(Err(_)))) {
            // The response exchange has ended; no detached request upload may
            // retain its body or the Link direction beyond this owner.
            self.upload.take();
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}
