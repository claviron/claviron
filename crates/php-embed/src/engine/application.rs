//! Persistent PHP workers. No PHP request startup/shutdown between messages;
//! adapters receive explicit HTTP data and own framework request-scope cleanup.

use std::cell::RefCell;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::SapiGatewayError;
use crate::event::{
    EventSubscription, PhpEventBridge, ReceivedEvent, RuntimeInput,
};

mod dispatch;
mod request_body;
use dispatch::Control;
mod instances;
mod request;
pub(crate) use instances::ApplicationWorkers;
pub use instances::PhpApplicationSnapshot;
pub(crate) use request::ApplicationRequests;

type Reply =
    oneshot::Sender<Result<Response<ApplicationBody>, SapiGatewayError>>;

/// An application's reply body: whole, from `claviron_application_respond`,
/// or piece by piece after `claviron_application_stream`, ended by
/// `claviron_application_end` (or the next `receive`). An error ends a stream
/// the worker could not finish, so it does not look whole.
pub(crate) enum ApplicationBody {
    Whole(Bytes),
    Stream(mpsc::Receiver<Result<Bytes, String>>),
}

/// Pieces of a streamed reply in flight to the client. Small, as SAPI's and
/// JavaScript's: a client that stops reading is felt at once as a write that
/// waits.
const REPLY_BODY_CAPACITY: usize = 2;

/// A streamed reply being written: where its pieces go and what bounds them.
struct Outgoing {
    sender: mpsc::Sender<Result<Bytes, String>>,
    written: usize,
    max_body_size: Option<usize>,
    write_timeout: Option<Duration>,
    runtime: tokio::runtime::Handle,
    stopping: CancellationToken,
}
pub(crate) struct Job {
    pub request: Request<Bytes>,
    pub reply: Reply,
    /// The route's reply ceiling for this request (`responses.max_body_size`).
    pub max_body_size: Option<usize>,
    /// The route's `responses.write_timeout`: how long a streamed write may
    /// wait for a client that is not reading.
    pub write_timeout: Option<Duration>,
}

pub(crate) struct Inbox {
    rx: mpsc::Receiver<Job>,
    runtime: tokio::runtime::Handle,
    pub current: Option<Job>,
    /// The streamed reply still being written, if the current request chose
    /// to stream.
    outgoing: Option<Outgoing>,
    responded: bool,
    event_handled: bool,
    control: Arc<Control>,
    /// Budget for one request or event, already resolved against the host
    /// default.
    pub execution_timeout: Option<Duration>,
}

thread_local! {
    pub(crate) static INBOX: RefCell<Option<Inbox>> = const { RefCell::new(None) };
}

impl Inbox {
    pub fn receive(
        &mut self,
        mut events: Option<(&PhpEventBridge, &mut EventSubscription)>,
    ) -> Result<Option<ApplicationInput<'_>>, String> {
        // Coming back for work ends a stream the handler did not end itself.
        self.outgoing = None;
        if self.current.is_some() {
            return Err(
                "respond to the current application request before receiving another".into()
            );
        }
        if self.responded {
            self.control.complete(true);
            self.responded = false;
        }
        if self.event_handled {
            self.control.complete(false);
            self.event_handled = false;
        }
        self.control.publish_ready();
        loop {
            let input = match events.as_mut() {
                Some((bridge, subscription)) => {
                    match bridge.receive_with(
                        &mut self.rx,
                        subscription,
                        &self.control.cancel,
                    )? {
                        Some(input) => Some(input),
                        // Stop closes lifecycle-owned senders. A dispatcher that
                        // already claimed this context may still be publishing
                        // its accepted job; drain that mailbox before returning.
                        None => {
                            self.rx.blocking_recv().map(RuntimeInput::Value)
                        }
                    }
                }
                None => self.rx.blocking_recv().map(RuntimeInput::Value),
            };
            let Some(input) = input else { return Ok(None) };
            let job = match input {
                RuntimeInput::Value(job) => job,
                RuntimeInput::Event(event) => {
                    if self.control.claim() {
                        self.event_handled = true;
                        return Ok(Some(ApplicationInput::Event(event)));
                    }
                    // HTTP admission won the race immediately after select.
                    // Preserve the event and receive the already-reserved job.
                    if let Some((_, subscription)) = events.as_mut() {
                        (*subscription).defer(event);
                    }
                    let Some(job) = self.rx.blocking_recv() else {
                        return Ok(None);
                    };
                    job
                }
            };
            if job.reply.is_closed() {
                self.control.release_claim();
                continue;
            }
            self.current = Some(job);
            return Ok(self
                .current
                .as_ref()
                .map(|job| ApplicationInput::Request(&job.request)));
        }
    }

    /// The current request's reply ceiling, from its route.
    pub fn max_body_size(&self) -> Option<usize> {
        self.current.as_ref().and_then(|job| job.max_body_size)
    }

    pub fn respond(
        &mut self,
        response: Response<Bytes>,
    ) -> Result<(), String> {
        let job = self.current.take().ok_or("no active application request")?;
        let _ = job.reply.send(Ok(response.map(ApplicationBody::Whole)));
        // Stay reserved until PHP has finished its after-response/reset work
        // and explicitly asks for the next request.
        self.responded = true;
        Ok(())
    }

    /// Send the head of a reply whose body follows piece by piece.
    pub fn respond_head(
        &mut self,
        head: http::response::Parts,
    ) -> Result<(), String> {
        let job = self.current.take().ok_or("no active application request")?;
        let (sender, receiver) = mpsc::channel(REPLY_BODY_CAPACITY);
        let _ = job.reply.send(Ok(Response::from_parts(
            head,
            ApplicationBody::Stream(receiver),
        )));
        self.outgoing = Some(Outgoing {
            sender,
            written: 0,
            max_body_size: job.max_body_size,
            write_timeout: job.write_timeout,
            runtime: self.runtime.clone(),
            stopping: self.control.cancel.clone(),
        });
        self.responded = true;
        Ok(())
    }

    /// One piece of the streamed reply. Waits while the client is not
    /// reading, up to `write_timeout`; `Ok(false)` means the client is gone,
    /// the wait ran out or the application is stopping, and the handler should stop.
    pub fn write(
        &mut self,
        chunk: &[u8],
    ) -> Result<bool, String> {
        let outgoing =
            self.outgoing.as_mut().ok_or("no streamed reply is open")?;
        outgoing.written = outgoing.written.saturating_add(chunk.len());
        if outgoing.max_body_size.is_some_and(|limit| outgoing.written > limit)
        {
            let _ = outgoing.sender.try_send(Err(
                "application reply exceeds responses.max_body_size".into(),
            ));
            self.outgoing = None;
            return Err(
                "application reply exceeds the route's responses.max_body_size"
                    .into(),
            );
        }
        let piece =
            match outgoing.sender.try_send(Ok(Bytes::copy_from_slice(chunk))) {
                Ok(()) => return Ok(true),
                Err(mpsc::error::TrySendError::Closed(_)) => return Ok(false),
                Err(mpsc::error::TrySendError::Full(piece)) => piece,
            };
        let Outgoing { sender, write_timeout, runtime, stopping, .. } =
            outgoing;
        let deadline = *write_timeout;
        Ok(runtime.block_on(async {
            tokio::select! {
                biased;
                _ = stopping.cancelled() => false,
                sent = sender.send(piece) => sent.is_ok(),
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep(deadline).await,
                        None => std::future::pending().await
                    }
                } => false,
            }
        }))
    }

    /// End the streamed reply.
    pub fn end(&mut self) {
        self.outgoing = None;
    }
}

pub(crate) enum ApplicationInput<'a> {
    Request(&'a Request<Bytes>),
    Event(ReceivedEvent),
}

impl Drop for Inbox {
    fn drop(&mut self) {
        // A stream the worker never ended is cut, not completed.
        if let Some(outgoing) = self.outgoing.take() {
            let _ = outgoing
                .sender
                .try_send(Err("application worker ended mid-reply".into()));
        }
        if let Some(job) = self.current.take() {
            // Two different endings arrive here. The application can exit on its
            // own — a fatal, an `exit()` — which is a bad gateway: something the
            // client asked for went wrong upstream. Or the slot can have been
            // retired underneath it, which is what a stop does, including a
            // drain that ran out of grace and interrupted the execution. That
            // one is 503: the server chose it, nothing is wrong with the
            // request, and a client or balancer may sensibly try elsewhere.
            // Reporting both as "application exited" sent whoever read the log
            // looking for a PHP error that never happened.
            let _ = job.reply.send(Err(if self.control.cancel.is_cancelled() {
                SapiGatewayError::ServiceUnavailable(
                    "application stopped while this request was in flight; it was not replayed"
                        .into()
                )
            } else {
                SapiGatewayError::BadGateway(
                    "application exited without a response; request was not replayed".into()
                )
            }));
        }
    }
}
