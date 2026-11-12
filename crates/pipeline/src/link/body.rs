//! The upload task belongs to its head wait, then to its streamed response.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use claviron_http::{RouteBody, RouteBodyError};
use claviron_orbit::link::application::body::{
    FlowEvent, Receiver, StreamError,
};
use claviron_orbit::link::application::client::{LentRead, LentWrite};
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt;
use tokio::io::AsyncWriteExt;
use tokio_util::task::AbortOnDropHandle;

use super::websocket::Input;

pub(super) enum ResponseBody {
    ByteStream(LentRead),
    Shared(Receiver),
}

/// Each frame keeps its payload arena extent pinned for as long as the HTTP
/// writer owns any view of its bytes.
pub(super) struct SharedBody {
    receiver: Receiver,
    done: bool,
}

impl SharedBody {
    pub(super) fn new(receiver: Receiver) -> Self {
        Self { receiver, done: false }
    }
}

impl Body for SharedBody {
    type Data = Bytes;
    type Error = RouteBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RouteBodyError>>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            let error = match self.receiver.try_next() {
                Ok(FlowEvent::Start { .. }) => continue,
                Ok(FlowEvent::Data(chunk)) => {
                    return Poll::Ready(Some(Ok(Frame::data(
                        Bytes::from_owner(chunk),
                    ))));
                }
                Ok(FlowEvent::Fin) => {
                    self.done = true;
                    return Poll::Ready(None);
                }
                Ok(FlowEvent::Reset(_)) => StreamError::Reset,
                Err(StreamError::WouldBlock) => {
                    match self.receiver.poll_readable(cx) {
                        Poll::Ready(Ok(())) => continue,
                        Poll::Ready(Err(error)) => error,
                        Poll::Pending => return Poll::Pending,
                    }
                }
                Err(error) => error,
            };
            self.done = true;
            self.receiver.cancel();
            return Poll::Ready(Some(Err(Box::new(error))));
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
}

/// Keep both directions independently driven, as on the original Link path.
/// Dropping the owner aborts only this upload. Input resets an unfinished
/// direction when the cancelled task drops it; a completed FIN is preserved.
pub(super) fn upload(
    body: RouteBody,
    input: LentWrite,
) -> AbortOnDropHandle<()> {
    // Construct the reset owner before spawning: cancellation before the task's
    // first poll must release the same input as cancellation during a write.
    let mut input = Input::new(input);
    AbortOnDropHandle::new(tokio::spawn(async move {
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

/// The rest of a body held whole, after what went into the stream before the
/// frame: written, then ended, under the same owner as a streamed upload.
pub(super) fn upload_rest(
    rest: Bytes,
    input: LentWrite,
) -> AbortOnDropHandle<()> {
    let mut input = Input::new(input);
    AbortOnDropHandle::new(tokio::spawn(async move {
        if input.write_all(&rest).await.is_ok() {
            let _ = input.shutdown().await;
        }
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
