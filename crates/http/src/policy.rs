//! What a server will accept of a request and its body, how it hands the
//! body on, and how it hands the reply back: [`HttpPolicy`], one side for
//! each direction — [`RequestPolicy`] under `requests:` and
//! [`ResponsePolicy`] under `responses:` — narrowed the same way at every
//! layer (listener, host, application).
//!
//! Declared here rather than beside the edge's configuration because more
//! than the edge serves requests: gufi and an embedded PHP runtime may run
//! with no edge in front of them and read the same policy.

use std::time::Duration;

use serde::Deserialize;

/// A listener's request policy, resolved: every limit decided.
///
/// The listener is where the transport really is — it accepts the connection
/// and spends the resources — so its policy is the real one, and a host or an
/// application is a virtual division inside it that can only narrow it. Two
/// fields must be decided on every listener, as a value or as `off`:
/// `max_body_size` and `body_read_timeout`. Nothing is left to a number the
/// library invents; `off` is the operator saying "no limit", on purpose.
///
/// Built from a declared [`RequestPolicyOverride`] by [`Self::for_listener`], and
/// narrowed for a host or an application by [`Self::narrow`]. What each field
/// allows a nearer layer to do:
///
/// | field | nearer layer |
/// |---|---|
/// | `max_body_size`, `body_read_timeout`, `buffer_size` | narrows only |
/// | `buffering` | replaces |
/// | `queue_timeout`, `head_timeout` | replaces |
///
/// These are transport concerns, not routing ones: a body that never stops
/// arriving and a body that stops arriving halfway cost the process the same
/// whether the request would have gone to the Axum router, another Rust adapter,
/// an embedded PHP application or an H3 handler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestPolicy {
    /// Largest request body read, in bytes; `None` is `off`.
    ///
    /// A declared `Content-Length` over it is refused with 413 before the
    /// request is dispatched; a streamed body that grows past it fails the
    /// read mid-flight, because by then there is no status line left to
    /// send.
    pub max_body_size: Option<usize>,

    /// Longest pause between two pieces of a request body; `None` is `off`.
    ///
    /// Not a budget for the whole body: a large upload on a slow link is
    /// legitimate and a total would punish it. What it catches is a peer that
    /// has stopped sending and holds the request open — a slow-loris — which
    /// no size limit can express. It applies to each request's body, not to
    /// the connection.
    pub body_read_timeout: Option<Duration>,

    /// Which way a body leans when nothing settles it: handed on as it
    /// arrives, or held first. Resolved per request by [`Self::delivery`].
    /// Not yet read: every backend moves onto it in turn (`docs/REVIEW.md`
    /// §3e).
    pub buffering: Buffering,

    /// The size `buffering` measures against, in bytes; `None` is `off`.
    /// Under `stream_default` a body declared no larger is held whole;
    /// under `buffer_default` a body is held up to it and the rest follows
    /// in pieces. `off`: no threshold — `stream_default` always streams and
    /// `buffer_default` holds up to `max_body_size`.
    pub buffer_size: Option<usize>,

    /// Longest a request waits for something to run it — a PHP thread, an
    /// instance, a pooled connection — before it is refused, as HAProxy's
    /// `timeout queue`. The first of a request's three times: it waits, then
    /// runs until the reply's head, then is bounded by gaps only.
    pub queue_timeout: Option<Duration>,

    /// Longest from the request reaching its application until the reply's
    /// head. The reply's body is not counted, so a streamed reply is not cut
    /// by it; after the head only gaps bound it (`body_read_timeout`,
    /// `responses.write_timeout`, `responses.body_idle_timeout`). Not a
    /// runtime's execution budget — PHP's and JavaScript's
    /// `execution_timeout` interrupt the VM and are the runner's own.
    pub head_timeout: Option<Duration>,
}

impl RequestPolicy {
    /// No limit anywhere — for an embedder or a test that decides none. A
    /// listener's policy comes from [`Self::for_listener`] instead.
    pub const fn unlimited() -> Self {
        Self {
            max_body_size: None,
            body_read_timeout: None,
            buffering: Buffering::StreamDefault,
            buffer_size: None,
            queue_timeout: None,
            head_timeout: None,
        }
    }

    /// A listener's policy from what it declares (its own `requests:` over
    /// the shared listener defaults). Refuses one that leaves a required
    /// limit undecided.
    pub fn for_listener(
        declared: &RequestPolicyOverride
    ) -> Result<Self, &'static str> {
        let Some(max_body_size) = declared.max_body_size else {
            return Err(
                "requests.max_body_size must be declared: a size, or off",
            );
        };
        let Some(body_read_timeout) = declared.body_read_timeout else {
            return Err(
                "requests.body_read_timeout must be declared: a duration, or off",
            );
        };
        Ok(Self {
            max_body_size: max_body_size.value(),
            body_read_timeout: body_read_timeout.value(),
            buffering: declared.buffering.unwrap_or_default(),
            buffer_size: declared.buffer_size.and_then(Limit::value),
            queue_timeout: declared.queue_timeout,
            head_timeout: declared.head_timeout,
        })
    }

    /// This policy with `nearer` — a host's or an application's — applied: a
    /// limit it writes can only make this one smaller, never larger, and `off`
    /// there leaves this one as it is; what is not a limit it replaces.
    pub fn narrow(
        &self,
        nearer: &RequestPolicyOverride,
    ) -> Self {
        Self {
            max_body_size: narrowed(self.max_body_size, nearer.max_body_size),
            body_read_timeout: narrowed(
                self.body_read_timeout,
                nearer.body_read_timeout,
            ),
            buffering: nearer.buffering.unwrap_or(self.buffering),
            buffer_size: narrowed(self.buffer_size, nearer.buffer_size),
            queue_timeout: nearer.queue_timeout.or(self.queue_timeout),
            head_timeout: nearer.head_timeout.or(self.head_timeout),
        }
    }

    /// How one request's body reaches `need`'s backend, decided at the start
    /// from its head alone. Asked only when a body follows: a request
    /// without one (a GET, an SSE subscription) has nothing to deliver and
    /// no size to judge.
    ///
    /// What the backend can take comes first, because no setting makes it
    /// take something else: a backend that needs the whole body gets it
    /// whole, and one that needs the length gets a body of unknown length
    /// held so it can be counted. Otherwise `buffering` leans one way and
    /// `buffer_size` is where it turns:
    ///
    /// - `stream_default`: held whole only when the declared length is
    ///   within `buffer_size`; unknown or larger streams.
    /// - `buffer_default`: held up to `buffer_size`, the rest in pieces;
    ///   streamed from the start only when `open_ended` — the head says for
    ///   certain that the body is a stream (gRPC, say), which the caller
    ///   reads and this does not guess.
    pub fn delivery(
        &self,
        need: BodyNeed,
        declared_length: Option<u64>,
        open_ended: bool,
    ) -> Delivery {
        match need {
            BodyNeed::Whole => return Delivery::Whole { up_to: None },
            BodyNeed::Length if declared_length.is_none() => {
                return Delivery::Whole { up_to: None };
            }
            _ => {}
        }
        match self.buffering {
            Buffering::StreamDefault => {
                match (declared_length, self.buffer_size) {
                    (Some(length), Some(size)) if length <= size as u64 => {
                        Delivery::Whole { up_to: None }
                    }
                    _ => Delivery::Pieces,
                }
            }
            Buffering::BufferDefault if open_ended => Delivery::Pieces,
            Buffering::BufferDefault => {
                Delivery::Whole { up_to: self.buffer_size }
            }
        }
    }
}

/// How one request's body is handed to its backend, decided at the start.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Delivery {
    /// Held before it is handed on. With `up_to`, a body that outgrows it
    /// hands on what is held and the rest follows in pieces; without, the
    /// whole body, bounded by `max_body_size` like any other.
    Whole { up_to: Option<usize> },
    /// Handed on piece by piece as it arrives.
    Pieces,
}

impl Delivery {
    /// Whether the body is collected before it is handed on. A `Whole`
    /// with `up_to` means held up to that size and the rest in pieces;
    /// until a reader can hold a prefix and stream the rest (§3e step 3),
    /// such a body is collected only when its declared length fits and
    /// otherwise handed on as it arrives.
    pub fn takes_whole(
        self,
        declared_length: Option<u64>,
    ) -> bool {
        match self {
            Delivery::Whole { up_to: None } => true,
            Delivery::Whole { up_to: Some(size) } => {
                declared_length.is_some_and(|length| length <= size as u64)
            }
            Delivery::Pieces => false,
        }
    }
}

/// What a backend can take of a request body — a fact about the backend,
/// not a preference, so [`RequestPolicy::delivery`] puts it before `buffering`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyNeed {
    /// Pieces as they arrive, the length known or not: SAPI, Orbit, an
    /// ASGI or JavaScript application.
    Stream,
    /// Pieces as they arrive, but only once the length is known: FastCGI,
    /// whose PHP reads its input only up to `CONTENT_LENGTH`.
    Length,
    /// The whole body in hand before anything runs: an upstream that may
    /// send it again on a retry, PHP's application mode.
    Whole,
}

/// A limit only ever shrinks: `off` or nothing nearer keeps the outer one.
fn narrowed<T: Ord>(
    outer: Option<T>,
    nearer: Option<Limit<T>>,
) -> Option<T> {
    match (outer, nearer) {
        (outer, None | Some(Limit::Off)) => outer,
        (None, Some(Limit::Value(nearer))) => Some(nearer),
        (Some(outer), Some(Limit::Value(nearer))) => Some(outer.min(nearer)),
    }
}

/// What a layer declares under `requests:` — the shared listener defaults, a
/// listener, a host, an application. Every field optional; a listener's must
/// end up deciding the two required limits ([`RequestPolicy::for_listener`]).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestPolicyOverride {
    /// Bytes, a size (`"10 MiB"`), or `off`.
    #[serde(default, deserialize_with = "optional_byte_limit")]
    pub max_body_size: Option<Limit<usize>>,
    /// A duration (`"30s"`) or `off`.
    #[serde(default, deserialize_with = "optional_time_limit")]
    pub body_read_timeout: Option<Limit<Duration>>,
    #[serde(default)]
    pub buffering: Option<Buffering>,
    /// Bytes, a size (`"256 KiB"`), or `off`.
    #[serde(default, deserialize_with = "optional_byte_limit")]
    pub buffer_size: Option<Limit<usize>>,
    #[serde(
        default,
        deserialize_with = "claviron_helpers::de::deserialize_optional_duration"
    )]
    pub queue_timeout: Option<Duration>,
    #[serde(
        default,
        deserialize_with = "claviron_helpers::de::deserialize_optional_duration"
    )]
    pub head_timeout: Option<Duration>,
}

impl RequestPolicyOverride {
    /// `nearer` over `self`, each field `nearer` writes replacing this one:
    /// how a listener's own `requests:` sits over the shared listener
    /// defaults. Not a narrowing — the defaults are a starting point, not a
    /// limit.
    pub fn overlay(
        &self,
        nearer: &Self,
    ) -> Self {
        Self {
            max_body_size: nearer.max_body_size.or(self.max_body_size),
            body_read_timeout: nearer
                .body_read_timeout
                .or(self.body_read_timeout),
            buffering: nearer.buffering.or(self.buffering),
            buffer_size: nearer.buffer_size.or(self.buffer_size),
            queue_timeout: nearer.queue_timeout.or(self.queue_timeout),
            head_timeout: nearer.head_timeout.or(self.head_timeout),
        }
    }
}

/// A declared limit: a value, or `off` for none, on purpose.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Limit<T> {
    Off,
    Value(T),
}

impl<T> Limit<T> {
    pub fn value(self) -> Option<T> {
        match self {
            Self::Off => None,
            Self::Value(value) => Some(value),
        }
    }
}

/// Which way a body leans when nothing settles it: handed on as it arrives,
/// or held first. The choice nginx names request and response buffering,
/// but a default rather than a mode — what the backend can take and what
/// the head says for certain come first ([`RequestPolicy::delivery`],
/// [`ResponsePolicy::delivery`]). Each side has its own; an open-ended
/// reply (SSE) is never held.
///
/// `buffer_default` is how a thread-bound backend is protected from a slow
/// client: the edge takes the body first, so PHP runs only once it is
/// whole and a 3G upload does not hold a worker for minutes. An async
/// application loses nothing to a slow client and takes it as it comes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Buffering {
    /// Handed on as it arrives, unless declared within `buffer_size`.
    #[default]
    StreamDefault,
    /// Held first, up to `buffer_size`, unless the head says it streams.
    BufferDefault,
}

/// Both sides of one request's policy, as the edge hands it to the
/// application in the request's extensions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpPolicy {
    pub requests: RequestPolicy,
    pub responses: ResponsePolicy,
}

impl HttpPolicy {
    /// No limit on either side — for an embedder or a test that decides
    /// none, and for an application with no edge in front of it.
    pub const fn unlimited() -> Self {
        Self {
            requests: RequestPolicy::unlimited(),
            responses: ResponsePolicy::unlimited(),
        }
    }

    /// This policy with a nearer layer's two blocks applied, each by its
    /// own side's rules.
    pub fn narrow(
        &self,
        nearer: &HttpPolicyOverride,
    ) -> Self {
        Self {
            requests: self.requests.narrow(&nearer.requests),
            responses: self.responses.narrow(&nearer.responses),
        }
    }
}

/// What one layer declares for both sides: its `requests:` and its
/// `responses:`, two keys side by side, gathered here for narrowing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HttpPolicyOverride {
    pub requests: RequestPolicyOverride,
    pub responses: ResponsePolicyOverride,
}

/// A reply's policy, resolved: how it is handed back to the client.
///
/// Every reply starts as a stream — a stream can always be made whole, a
/// whole never a stream — and this says when it is held instead: up to
/// `buffer_size` to be sent whole with its length known, which frees a
/// thread-bound backend (an FPM child, a SAPI thread) from a slow reader.
/// Nothing here is required and nothing is invented: unwritten, a reply
/// streams and nothing bounds its time.
///
/// | field | nearer layer |
/// |---|---|
/// | `buffer_size`, `max_body_size`, `write_timeout`, `body_idle_timeout` | narrows only |
/// | `buffering` | replaces |
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponsePolicy {
    /// Which way a reply leans when its own head does not say.
    pub buffering: Buffering,
    /// Where `buffering` turns, in bytes; `None` is `off`.
    pub buffer_size: Option<usize>,
    /// Largest reply body handed back, in bytes; `None` is `off`.
    pub max_body_size: Option<usize>,
    /// Longest a reply write may wait for a client that is not reading.
    pub write_timeout: Option<Duration>,
    /// Longest gap between two pieces of a reply arriving from the backend.
    pub body_idle_timeout: Option<Duration>,
}

impl ResponsePolicy {
    /// Streams, and nothing bounded.
    pub const fn unlimited() -> Self {
        Self {
            buffering: Buffering::StreamDefault,
            buffer_size: None,
            max_body_size: None,
            write_timeout: None,
            body_idle_timeout: None,
        }
    }

    /// A listener's reply policy from what it declares; unwritten is `off`.
    pub fn for_listener(declared: &ResponsePolicyOverride) -> Self {
        Self {
            buffering: declared.buffering.unwrap_or_default(),
            buffer_size: declared.buffer_size.and_then(Limit::value),
            max_body_size: declared.max_body_size.and_then(Limit::value),
            write_timeout: declared.write_timeout.and_then(Limit::value),
            body_idle_timeout: declared
                .body_idle_timeout
                .and_then(Limit::value),
        }
    }

    /// This policy with a nearer layer's block applied: limits only shrink,
    /// `buffering` replaces.
    pub fn narrow(
        &self,
        nearer: &ResponsePolicyOverride,
    ) -> Self {
        Self {
            buffering: nearer.buffering.unwrap_or(self.buffering),
            buffer_size: narrowed(self.buffer_size, nearer.buffer_size),
            max_body_size: narrowed(self.max_body_size, nearer.max_body_size),
            write_timeout: narrowed(self.write_timeout, nearer.write_timeout),
            body_idle_timeout: narrowed(
                self.body_idle_timeout,
                nearer.body_idle_timeout,
            ),
        }
    }

    /// How one reply is handed back, decided when its head is known. The
    /// application's own head comes first: `X-Accel-Buffering` (nginx's
    /// header, kept as it is) — `no` streams, `yes` holds up to
    /// `buffer_size` — then `Content-Type: text/event-stream`, which always
    /// streams. Only then `buffering`: `stream_default` holds a reply
    /// declared within `buffer_size`, `buffer_default` holds up to it.
    pub fn delivery(
        &self,
        head: &http::HeaderMap,
        declared_length: Option<u64>,
    ) -> Delivery {
        match head.get(X_ACCEL_BUFFERING).map(|value| value.as_bytes()) {
            Some(value) if value.eq_ignore_ascii_case(b"no") => {
                return Delivery::Pieces;
            }
            Some(value) if value.eq_ignore_ascii_case(b"yes") => {
                return Delivery::Whole { up_to: self.buffer_size };
            }
            _ => {}
        }
        if head
            .get(http::header::CONTENT_TYPE)
            .is_some_and(|value| is_event_stream(value.as_bytes()))
        {
            return Delivery::Pieces;
        }
        match self.buffering {
            Buffering::StreamDefault => {
                match (declared_length, self.buffer_size) {
                    (Some(length), Some(size)) if length <= size as u64 => {
                        Delivery::Whole { up_to: None }
                    }
                    _ => Delivery::Pieces,
                }
            }
            Buffering::BufferDefault => {
                Delivery::Whole { up_to: self.buffer_size }
            }
        }
    }
}

/// nginx's header for an application to say how its reply is sent. A
/// message to the proxy, not the client: removed before the reply leaves.
pub const X_ACCEL_BUFFERING: &str = "x-accel-buffering";

/// Whether a `Content-Type` is a server-sent event stream: a reply that
/// never ends on its own.
pub fn is_event_stream(content_type: &[u8]) -> bool {
    let mime =
        content_type.split(|byte| *byte == b';').next().unwrap_or(content_type);
    mime.trim_ascii().eq_ignore_ascii_case(b"text/event-stream")
}

/// What a layer declares under `responses:`. Every field optional.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponsePolicyOverride {
    #[serde(default)]
    pub buffering: Option<Buffering>,
    /// Bytes, a size (`"256 KiB"`), or `off`.
    #[serde(default, deserialize_with = "optional_byte_limit")]
    pub buffer_size: Option<Limit<usize>>,
    /// Bytes, a size, or `off`.
    #[serde(default, deserialize_with = "optional_byte_limit")]
    pub max_body_size: Option<Limit<usize>>,
    /// A duration or `off`.
    #[serde(default, deserialize_with = "optional_time_limit")]
    pub write_timeout: Option<Limit<Duration>>,
    /// A duration or `off`.
    #[serde(default, deserialize_with = "optional_time_limit")]
    pub body_idle_timeout: Option<Limit<Duration>>,
}

impl ResponsePolicyOverride {
    /// `nearer` over `self`, each field `nearer` writes replacing this one.
    pub fn overlay(
        &self,
        nearer: &Self,
    ) -> Self {
        Self {
            buffering: nearer.buffering.or(self.buffering),
            buffer_size: nearer.buffer_size.or(self.buffer_size),
            max_body_size: nearer.max_body_size.or(self.max_body_size),
            write_timeout: nearer.write_timeout.or(self.write_timeout),
            body_idle_timeout: nearer
                .body_idle_timeout
                .or(self.body_idle_timeout),
        }
    }
}

/// Bytes, a size string, or `off`.
fn optional_byte_limit<'de, D>(
    deserializer: D
) -> Result<Option<Limit<usize>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Bytes(usize),
        Text(String),
    }
    let Some(raw) = Option::<Raw>::deserialize(deserializer)? else {
        return Ok(None);
    };
    match raw {
        Raw::Bytes(bytes) => Ok(Some(Limit::Value(bytes))),
        Raw::Text(text) if text.trim().eq_ignore_ascii_case("off") => {
            Ok(Some(Limit::Off))
        }
        Raw::Text(text) => text
            .parse::<byte_unit::Byte>()
            .map(|bytes| Some(Limit::Value(bytes.as_u64() as usize)))
            .map_err(|error| {
                serde::de::Error::custom(format!("invalid byte size: {error}"))
            }),
    }
}

/// A duration or `off`.
fn optional_time_limit<'de, D>(
    deserializer: D
) -> Result<Option<Limit<Duration>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(text) = Option::<String>::deserialize(deserializer)? else {
        return Ok(None);
    };
    if text.trim().eq_ignore_ascii_case("off") {
        return Ok(Some(Limit::Off));
    }
    claviron_helpers::de::parse_duration(&text)
        .map(|after| Some(Limit::Value(after)))
        .map_err(|error| {
            serde::de::Error::custom(format!("a duration, or off: {error}"))
        })
}
