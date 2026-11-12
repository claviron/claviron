//! What crosses the inbox: one exchange's control, and its body when the body
//! is whole and fits.
//!
//! ## One crossing per direction
//!
//! A body held whole — a buffered request, a reply whose size is known — rides
//! after the frame's head, so the frame that wakes the far side carries
//! everything it needs and nothing else crosses. Only a body that streams, or
//! one larger than the frame, goes on the exchange's stream, which is then a
//! second crossing and usually a second wake. The frame says which.
//!
//! ## Layout
//!
//! A fixed prologue and then length-prefixed bytes. No self-describing format,
//! no varints, no serde — decoding is a struct read and one walk forward, and
//! a frame that does not parse is a frame from another build rather than a
//! frame to guess at.
//!
//! ## Typed envelope, raw headers
//!
//! Method, scheme, version and the peer addresses cross as values. The edge
//! already parsed them off the wire and parsing them a second time buys
//! nothing.
//!
//! The header list crosses as byte pairs, untouched. Two reasons, and the
//! second is the stronger: the far side hands them to ASGI in exactly this
//! form, so anything typed would have to be turned back into bytes; and an
//! application may depend on what actually arrived — a repeated header, an
//! order, a signature computed over the bytes as sent. Normalising is
//! flattening, and flattening loses what nobody can put back.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use bytes::Bytes;
use orbitive::NetId64;
use orbitive::stream::{Side, StreamId, Ticket};

use super::{Error, Result};

/// The kind of payload carried by a Link inbox frame.
///
/// A lane only ever receives one of these — an edge's lane takes replies, an
/// application's takes requests — so this is not routing. It is the check that
/// catches a frame written to the wrong lane before its bytes are read as
/// something they are not.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum FrameKind {
    Request = 1,
    Reply = 2,
    Control = 3,
    ShutdownInitiated = 4,
    ApplicationState = 5,
}

impl FrameKind {
    fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            1 => Ok(Self::Request),
            2 => Ok(Self::Reply),
            3 => Ok(Self::Control),
            4 => Ok(Self::ShutdownInitiated),
            5 => Ok(Self::ApplicationState),
            _ => Err(Error::Malformed("unknown application frame kind")),
        }
    }
}

/// One row of a runner application snapshot. The application layer owns the
/// row codec; Link groups rows from one live lane incarnation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationStateNotice {
    pub application: String,
    pub lane: u16,
    pub incarnation: u64,
    pub batch: u64,
    pub row_count: u16,
    pub row_index: u16,
    pub payload: Vec<u8>,
}

impl ApplicationStateNotice {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let name = self.application.as_bytes();
        if name.is_empty() || name.len() > u8::MAX as usize {
            return Err(Error::Malformed("invalid application state identity"));
        }
        let payload_len = u16::try_from(self.payload.len())
            .map_err(|_| Error::FrameTooLarge)?;
        if self.row_count == 0 || self.row_index >= self.row_count {
            return Err(Error::Malformed(
                "invalid application state row index",
            ));
        }
        let mut frame =
            Vec::with_capacity(27 + name.len() + self.payload.len());
        frame.extend_from_slice(&[
            FrameKind::ApplicationState as u8,
            2,
            name.len() as u8,
        ]);
        frame.extend_from_slice(&self.lane.to_le_bytes());
        frame.extend_from_slice(&self.incarnation.to_le_bytes());
        frame.extend_from_slice(&self.batch.to_le_bytes());
        frame.extend_from_slice(&self.row_count.to_le_bytes());
        frame.extend_from_slice(&self.row_index.to_le_bytes());
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(name);
        frame.extend_from_slice(&self.payload);
        Ok(frame)
    }

    pub fn decode(frame: &[u8]) -> Result<Self> {
        let [5, 2, name_len, rest @ ..] = frame else {
            return Err(Error::Malformed("invalid application state header"));
        };
        let name_len = *name_len as usize;
        if name_len == 0 || rest.len() < 24 + name_len {
            return Err(Error::Malformed("invalid application state length"));
        }
        let lane =
            u16::from_le_bytes(rest[0..2].try_into().expect("checked length"));
        let incarnation =
            u64::from_le_bytes(rest[2..10].try_into().expect("checked length"));
        let batch = u64::from_le_bytes(
            rest[10..18].try_into().expect("checked length"),
        );
        let row_count = u16::from_le_bytes(
            rest[18..20].try_into().expect("checked length"),
        );
        let row_index = u16::from_le_bytes(
            rest[20..22].try_into().expect("checked length"),
        );
        let payload_len = u16::from_le_bytes(
            rest[22..24].try_into().expect("checked length"),
        ) as usize;
        if rest.len() != 24 + name_len + payload_len
            || row_count == 0
            || row_index >= row_count
        {
            return Err(Error::Malformed("invalid application state length"));
        }
        let application = String::from_utf8(rest[24..24 + name_len].to_vec())
            .map_err(|_| {
            Error::Malformed("invalid application state name")
        })?;
        Ok(Self {
            application,
            lane,
            incarnation,
            batch,
            row_count,
            row_index,
            payload: rest[24 + name_len..].to_vec(),
        })
    }
}

/// One application managed inside the Link destination's runner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlTarget {
    pub runtime: String,
    pub name: String,
}

/// An opaque application command carried by Link. The bridge never executes it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlCommand {
    /// The advertised Link name selecting the runner lane.
    pub application: String,
    pub name: String,
    pub payload: Vec<u8>,
    /// A hosted application identity, independent of the Link lane name.
    pub target: Option<ControlTarget>,
    /// The edge lane that asked, when the command expects an answer for that
    /// edge alone (`status`). Absent, an answer goes to every edge.
    pub origin: Option<u16>,
}

/// Control frame layout: 2 had no origin; 3 carries one.
const CONTROL_WITHOUT_ORIGIN: u8 = 2;
const CONTROL_WITH_ORIGIN: u8 = 3;

impl ControlCommand {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let application = self.application.as_bytes();
        let name = self.name.as_bytes();
        let (runtime, target) =
            self.target.as_ref().map_or((&[][..], &[][..]), |target| {
                (target.runtime.as_bytes(), target.name.as_bytes())
            });
        if application.is_empty()
            || application.len() > u8::MAX as usize
            || name.is_empty()
            || name.len() > u8::MAX as usize
            || runtime.len() > u8::MAX as usize
            || target.len() > u8::MAX as usize
            || (runtime.is_empty() != target.is_empty())
            || (self.target.is_some() && runtime.is_empty())
        {
            return Err(Error::Malformed(
                "invalid application control identity",
            ));
        }
        let payload_len = u16::try_from(self.payload.len())
            .map_err(|_| Error::FrameTooLarge)?;
        let mut frame = Vec::with_capacity(
            11 + application.len()
                + name.len()
                + runtime.len()
                + target.len()
                + self.payload.len(),
        );
        frame.extend_from_slice(&[
            FrameKind::Control as u8,
            CONTROL_WITH_ORIGIN,
            application.len() as u8,
            name.len() as u8,
            runtime.len() as u8,
            target.len() as u8,
        ]);
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.push(u8::from(self.origin.is_some()));
        frame.extend_from_slice(&self.origin.unwrap_or(0).to_le_bytes());
        frame.extend_from_slice(application);
        frame.extend_from_slice(name);
        frame.extend_from_slice(runtime);
        frame.extend_from_slice(target);
        frame.extend_from_slice(&self.payload);
        Ok(frame)
    }

    pub fn decode(frame: &[u8]) -> Result<Self> {
        let (
            origin,
            application_len,
            name_len,
            runtime_len,
            target_len,
            len_lo,
            len_hi,
            rest,
        ) = match frame {
            [
                3,
                CONTROL_WITH_ORIGIN,
                application_len,
                name_len,
                runtime_len,
                target_len,
                len_lo,
                len_hi,
                has_origin,
                origin_lo,
                origin_hi,
                rest @ ..,
            ] => {
                let origin = match has_origin {
                    0 => None,
                    1 => Some(u16::from_le_bytes([*origin_lo, *origin_hi])),
                    _ => {
                        return Err(Error::Malformed(
                            "invalid application control origin",
                        ));
                    }
                };
                (
                    origin,
                    application_len,
                    name_len,
                    runtime_len,
                    target_len,
                    len_lo,
                    len_hi,
                    rest,
                )
            }
            [
                3,
                CONTROL_WITHOUT_ORIGIN,
                application_len,
                name_len,
                runtime_len,
                target_len,
                len_lo,
                len_hi,
                rest @ ..,
            ] => (
                None,
                application_len,
                name_len,
                runtime_len,
                target_len,
                len_lo,
                len_hi,
                rest,
            ),
            _ => {
                return Err(Error::Malformed(
                    "invalid application control header",
                ));
            }
        };
        let payload_len = u16::from_le_bytes([*len_lo, *len_hi]) as usize;
        let application_len = *application_len as usize;
        let name_len = *name_len as usize;
        let runtime_len = *runtime_len as usize;
        let target_len = *target_len as usize;
        if application_len == 0
            || name_len == 0
            || (runtime_len == 0) != (target_len == 0)
            || rest.len()
                != application_len
                    + name_len
                    + runtime_len
                    + target_len
                    + payload_len
        {
            return Err(Error::Malformed("invalid application control length"));
        }
        let application = String::from_utf8(rest[..application_len].to_vec())
            .map_err(|_| {
            Error::Malformed("invalid application control application")
        })?;
        let name = String::from_utf8(
            rest[application_len..application_len + name_len].to_vec(),
        )
        .map_err(|_| Error::Malformed("invalid application control name"))?;
        let target_offset = application_len + name_len;
        let payload_offset = target_offset + runtime_len + target_len;
        let target = if runtime_len == 0 {
            None
        } else {
            Some(ControlTarget {
                runtime: String::from_utf8(
                    rest[target_offset..target_offset + runtime_len].to_vec(),
                )
                .map_err(|_| {
                    Error::Malformed("invalid control target runtime")
                })?,
                name: String::from_utf8(
                    rest[target_offset + runtime_len..payload_offset].to_vec(),
                )
                .map_err(|_| {
                    Error::Malformed("invalid control target application")
                })?,
            })
        };
        let payload = rest[payload_offset..].to_vec();
        Ok(Self { application, name, payload, target, origin })
    }
}

/// The edge master's own shutdown began. This is independent of lane loss.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShutdownNotice {
    pub edge: String,
}

impl ShutdownNotice {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let edge = self.edge.as_bytes();
        if edge.is_empty() || edge.len() > u8::MAX as usize {
            return Err(Error::Malformed("invalid shutdown notice identity"));
        }
        let mut frame = Vec::with_capacity(3 + edge.len());
        frame.extend_from_slice(&[
            FrameKind::ShutdownInitiated as u8,
            1,
            edge.len() as u8,
        ]);
        frame.extend_from_slice(edge);
        Ok(frame)
    }

    pub fn decode(frame: &[u8]) -> Result<Self> {
        if frame.len() < 4 || frame[0] != 4 || frame[1] != 1 {
            return Err(Error::Malformed("invalid shutdown notice header"));
        }
        let edge_len = frame[2] as usize;
        if edge_len == 0 || frame.len() != 3 + edge_len {
            return Err(Error::Malformed("invalid shutdown notice length"));
        }
        let edge = String::from_utf8(frame[3..].to_vec())
            .map_err(|_| Error::Malformed("invalid shutdown notice edge"))?;
        Ok(Self { edge })
    }
}

/// The HTTP version, as ASGI's `scope` wants to report it.
///
/// An enum rather than a string because there are five of them and the edge
/// already knows which; `as_str` gives the far side the spelling the scope
/// expects without a parse.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum HttpVersion {
    Http09 = 0,
    Http10 = 1,
    #[default]
    Http11 = 2,
    Http2 = 3,
    Http3 = 4,
}

impl HttpVersion {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http09 => "0.9",
            Self::Http10 => "1.0",
            Self::Http11 => "1.1",
            Self::Http2 => "2",
            Self::Http3 => "3",
        }
    }

    fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0 => Ok(Self::Http09),
            1 => Ok(Self::Http10),
            2 => Ok(Self::Http11),
            3 => Ok(Self::Http2),
            4 => Ok(Self::Http3),
            _ => Err(Error::Malformed("unknown HTTP version")),
        }
    }
}

impl From<http::Version> for HttpVersion {
    fn from(version: http::Version) -> Self {
        match version {
            http::Version::HTTP_09 => Self::Http09,
            http::Version::HTTP_10 => Self::Http10,
            http::Version::HTTP_2 => Self::Http2,
            http::Version::HTTP_3 => Self::Http3,
            _ => Self::Http11,
        }
    }
}

impl From<HttpVersion> for http::Version {
    fn from(version: HttpVersion) -> Self {
        match version {
            HttpVersion::Http09 => Self::HTTP_09,
            HttpVersion::Http10 => Self::HTTP_10,
            HttpVersion::Http11 => Self::HTTP_11,
            HttpVersion::Http2 => Self::HTTP_2,
            HttpVersion::Http3 => Self::HTTP_3,
        }
    }
}

/// A header block as it arrived, in order, with whatever casing and
/// repetition it had.
pub type Headers = Vec<(Vec<u8>, Vec<u8>)>;

/// Where one direction's body is.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum ExchangeBody {
    /// None: the direction ended before the frame was written.
    #[default]
    Empty,
    /// The whole body, carried in the frame after its head.
    Inline(Bytes),
    /// On the exchange's stream, as it comes.
    Stream,
}

impl ExchangeBody {
    fn flag(&self) -> u8 {
        match self {
            Self::Empty => 0,
            Self::Inline(_) => FLAG_INLINE,
            Self::Stream => FLAG_STREAMED,
        }
    }

    /// The inline bytes; empty for a body that is not inline.
    pub fn inline(&self) -> &[u8] {
        match self {
            Self::Inline(bytes) => bytes,
            Self::Empty | Self::Stream => &[],
        }
    }

    /// Read from the flags, the inline bytes being the rest of the frame.
    fn decode(
        flags: u8,
        frame: &Bytes,
        at: usize,
    ) -> Result<Self> {
        match (flags & FLAG_INLINE != 0, flags & FLAG_STREAMED != 0) {
            (false, false) if at == frame.len() => Ok(Self::Empty),
            (true, false) => Ok(Self::Inline(frame.slice(at..))),
            (false, true) if at == frame.len() => Ok(Self::Stream),
            (true, true) => {
                Err(Error::Malformed("a body is both inline and streamed"))
            }
            _ => Err(Error::Malformed("a frame has bytes past its head")),
        }
    }
}

/// One request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Exchange {
    /// Matches the reply to this request. The caller mints it; nobody else
    /// interprets it.
    pub correlation: u64,
    /// The node the reply must go to.
    ///
    /// A request may be taken by any worker serving its application, but a
    /// reply may not: only the edge worker holding the client's connection can
    /// use it.
    pub origin: u16,
    pub method: String,
    pub scheme: String,
    pub version: HttpVersion,
    /// Percent-decoded, as ASGI's `path` wants it.
    pub path: String,
    /// As it arrived, as ASGI's `raw_path` wants it.
    pub raw_path: Vec<u8>,
    pub query: Vec<u8>,
    pub headers: Headers,
    pub client: Option<SocketAddr>,
    pub server: Option<SocketAddr>,
    /// The exchange's stream: what a streamed body travels on in either
    /// direction, and how the application learns the edge has gone before it
    /// answers. Present on every exchange an edge sends.
    pub stream: Option<Ticket>,
    /// The request body: none, carried here, or on the stream.
    pub body: ExchangeBody,
    /// A message-session request: H1/H2 WebSocket or one reliable H3
    /// WebTransport bidi stream. The application answers 101 to accept or an
    /// error status to refuse. Edge translates that acknowledgment to the
    /// native HTTP handshake; 101 is never sent on an H2/H3 connection.
    /// Accepted directions carry [`super::message`] records, not HTTP bodies.
    pub upgrade: bool,
}

/// One reply. Its stream, when the body streams, is the exchange's own.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExchangeReply {
    pub correlation: u64,
    pub status: u16,
    pub headers: Headers,
    pub body: ExchangeBody,
}

/// The exchange carries a stream ticket.
const FLAG_STREAM: u8 = 1 << 0;
const FLAG_CLIENT: u8 = 1 << 1;
const FLAG_SERVER: u8 = 1 << 2;
/// A request asking to become an application message session.
const FLAG_UPGRADE: u8 = 1 << 3;
/// The body follows the head, to the end of the frame.
const FLAG_INLINE: u8 = 1 << 4;
/// The body travels on the stream.
const FLAG_STREAMED: u8 = 1 << 5;
/// Where the flags byte sits in every frame's prologue.
const FLAGS_AT: usize = 2;

impl Exchange {
    /// The whole frame in one buffer. [`Self::encode_head`] and
    /// [`Self::inline`] give the same bytes as two parts, for a vectored
    /// write that does not join them.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = self.encode_head()?;
        out.extend_from_slice(self.body.inline());
        Ok(out)
    }

    /// The inline body, written after [`Self::encode_head`]; empty otherwise.
    pub fn inline(&self) -> &[u8] {
        self.body.inline()
    }

    pub(super) fn fits_paired_response(
        &self,
        capacity: usize,
    ) -> Result<bool> {
        if self.upgrade || matches!(self.body, ExchangeBody::Stream) {
            return Ok(false);
        }
        if matches!(self.body, ExchangeBody::Empty) {
            return Ok(true);
        }
        // A stream ticket is 17 bytes. An already assigned ticket is
        // included in encode_head, so do not count it again on a retry.
        let ticket = if self.stream.is_none() { 17 } else { 0 };
        Ok(self.encode_head()?.len() + ticket + self.inline().len() <= capacity)
    }

    /// Everything but the inline body.
    pub fn encode_head(&self) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(512);
        let mut flags = self.body.flag();
        if self.stream.is_some() {
            flags |= FLAG_STREAM;
        }
        if self.client.is_some() {
            flags |= FLAG_CLIENT;
        }
        if self.server.is_some() {
            flags |= FLAG_SERVER;
        }
        if self.upgrade {
            flags |= FLAG_UPGRADE;
        }

        out.push(FrameKind::Request as u8);
        out.push(self.version as u8);
        out.push(flags);
        out.push(0); // reserved: keeps the prologue a round shape
        out.extend_from_slice(&self.origin.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // status, a reply's field
        out.extend_from_slice(&self.correlation.to_le_bytes());

        put_ticket(&mut out, self.stream.as_ref());
        put_addr(&mut out, self.client.as_ref());
        put_addr(&mut out, self.server.as_ref());

        put_bytes(&mut out, self.method.as_bytes())?;
        put_bytes(&mut out, self.scheme.as_bytes())?;
        put_bytes(&mut out, self.path.as_bytes())?;
        put_bytes(&mut out, &self.raw_path)?;
        put_bytes(&mut out, &self.query)?;
        put_headers(&mut out, &self.headers)?;
        Ok(out)
    }

    /// Taken by value: an inline body is a slice of the frame, not a copy.
    pub fn decode(frame: Bytes) -> Result<Self> {
        let mut cursor = Cursor::new(&frame);
        if FrameKind::from_byte(cursor.byte()?)? != FrameKind::Request {
            return Err(Error::Malformed(
                "a reply arrived where a request belongs",
            ));
        }
        let version = HttpVersion::from_byte(cursor.byte()?)?;
        let flags = cursor.byte()?;
        cursor.byte()?; // reserved
        let origin = cursor.u16()?;
        cursor.u16()?; // status
        let correlation = cursor.u64()?;

        let stream = cursor.ticket(flags & FLAG_STREAM != 0)?;
        let client = cursor.addr(flags & FLAG_CLIENT != 0)?;
        let server = cursor.addr(flags & FLAG_SERVER != 0)?;
        let method = cursor.string()?;
        let scheme = cursor.string()?;
        let path = cursor.string()?;
        let raw_path = cursor.bytes()?.to_vec();
        let query = cursor.bytes()?.to_vec();
        let headers = cursor.headers()?;
        let at = cursor.at;

        Ok(Self {
            correlation,
            origin,
            method,
            scheme,
            version,
            path,
            raw_path,
            query,
            headers,
            client,
            server,
            stream,
            body: ExchangeBody::decode(flags, &frame, at)?,
            upgrade: flags & FLAG_UPGRADE != 0,
        })
    }
}

/// Fit a whole body into a frame whose head is already encoded: inline when
/// head and body together are within `capacity`, otherwise marked as streamed
/// and handed back for the stream. The flag is changed in the encoded head, so
/// the head is not encoded twice.
pub fn fit_inline(
    head: &mut [u8],
    body: ExchangeBody,
    capacity: usize,
) -> (ExchangeBody, Option<Bytes>) {
    match body {
        ExchangeBody::Inline(bytes) if head.len() + bytes.len() > capacity => {
            head[FLAGS_AT] = head[FLAGS_AT] & !FLAG_INLINE | FLAG_STREAMED;
            (ExchangeBody::Stream, Some(bytes))
        }
        body => (body, None),
    }
}

impl ExchangeReply {
    /// The whole frame in one buffer; see [`Exchange::encode`].
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = self.encode_head()?;
        out.extend_from_slice(self.body.inline());
        Ok(out)
    }

    /// The inline body, written after [`Self::encode_head`]; empty otherwise.
    pub fn inline(&self) -> &[u8] {
        self.body.inline()
    }

    /// Everything but the inline body.
    pub fn encode_head(&self) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(256);
        let flags = self.body.flag();

        out.push(FrameKind::Reply as u8);
        out.push(HttpVersion::default() as u8);
        out.push(flags);
        out.push(0);
        out.extend_from_slice(&0u16.to_le_bytes()); // origin, a request's field
        out.extend_from_slice(&self.status.to_le_bytes());
        out.extend_from_slice(&self.correlation.to_le_bytes());

        put_headers(&mut out, &self.headers)?;
        Ok(out)
    }

    /// Taken by value: an inline body is a slice of the frame, not a copy.
    pub fn decode(frame: Bytes) -> Result<Self> {
        let mut cursor = Cursor::new(&frame);
        if FrameKind::from_byte(cursor.byte()?)? != FrameKind::Reply {
            return Err(Error::Malformed(
                "a request arrived where a reply belongs",
            ));
        }
        cursor.byte()?; // version
        let flags = cursor.byte()?;
        cursor.byte()?; // reserved
        cursor.u16()?; // origin
        let status = cursor.u16()?;
        let correlation = cursor.u64()?;
        let headers = cursor.headers()?;
        let at = cursor.at;

        Ok(Self {
            correlation,
            status,
            headers,
            body: ExchangeBody::decode(flags, &frame, at)?,
        })
    }
}

/// Which half a frame carries, without decoding the rest of it.
pub fn frame_kind(bytes: &[u8]) -> Result<FrameKind> {
    FrameKind::from_byte(*bytes.first().ok_or(Error::Malformed("empty frame"))?)
}

fn put_bytes(
    out: &mut Vec<u8>,
    value: &[u8],
) -> Result<()> {
    let len = u16::try_from(value.len())
        .map_err(|_| Error::Malformed("a frame field is longer than 64 KiB"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value);
    Ok(())
}

fn put_headers(
    out: &mut Vec<u8>,
    headers: &Headers,
) -> Result<()> {
    let count = u16::try_from(headers.len())
        .map_err(|_| Error::Malformed("more headers than a frame can name"))?;
    out.extend_from_slice(&count.to_le_bytes());
    for (name, value) in headers {
        put_bytes(out, name)?;
        put_bytes(out, value)?;
    }
    Ok(())
}

fn put_ticket(
    out: &mut Vec<u8>,
    ticket: Option<&Ticket>,
) {
    let Some(ticket) = ticket else { return };
    out.extend_from_slice(&ticket.id.net_id().raw().to_le_bytes());
    out.push(match ticket.side {
        Side::A => 0,
        Side::B => 1,
    });
    out.extend_from_slice(&ticket.epoch.to_le_bytes());
}

fn put_addr(
    out: &mut Vec<u8>,
    addr: Option<&SocketAddr>,
) {
    let Some(addr) = addr else { return };
    match addr.ip() {
        IpAddr::V4(ip) => {
            out.push(4);
            out.extend_from_slice(&ip.octets());
            out.extend_from_slice(&[0u8; 12]);
        }
        IpAddr::V6(ip) => {
            out.push(6);
            out.extend_from_slice(&ip.octets());
        }
    }
    out.extend_from_slice(&addr.port().to_le_bytes());
}

/// Walks a frame forward once. Every read is bounds-checked: the bytes come
/// from another process, and a frame that claims a length it does not have is
/// a frame to refuse, not to trust.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn take(
        &mut self,
        count: usize,
    ) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(count)
            .ok_or(Error::Malformed("frame length overflows"))?;
        let slice = self
            .bytes
            .get(self.at..end)
            .ok_or(Error::Malformed("frame ends mid-field"))?;
        self.at = end;
        Ok(slice)
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("two bytes")))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("eight bytes")))
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.u16()? as usize;
        self.take(len)
    }

    fn string(&mut self) -> Result<String> {
        let bytes = self.bytes()?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| Error::Malformed("a frame field is not valid UTF-8"))
    }

    fn headers(&mut self) -> Result<Headers> {
        let count = self.u16()? as usize;
        let mut headers = Vec::with_capacity(count);
        for _ in 0..count {
            let name = self.bytes()?.to_vec();
            let value = self.bytes()?.to_vec();
            headers.push((name, value));
        }
        Ok(headers)
    }

    fn ticket(
        &mut self,
        present: bool,
    ) -> Result<Option<Ticket>> {
        if !present {
            return Ok(None);
        }
        let id = StreamId::from_net_id(NetId64::from_raw(self.u64()?));
        let side = match self.byte()? {
            0 => Side::A,
            1 => Side::B,
            _ => return Err(Error::Malformed("a ticket names neither side")),
        };
        Ok(Some(Ticket { id, side, epoch: self.u64()? }))
    }

    fn addr(
        &mut self,
        present: bool,
    ) -> Result<Option<SocketAddr>> {
        if !present {
            return Ok(None);
        }
        let family = self.byte()?;
        let octets: [u8; 16] =
            self.take(16)?.try_into().expect("sixteen bytes");
        let port = self.u16()?;
        let ip = match family {
            4 => IpAddr::V4(Ipv4Addr::from([
                octets[0], octets[1], octets[2], octets[3],
            ])),
            6 => IpAddr::V6(Ipv6Addr::from(octets)),
            _ => return Err(Error::Malformed("an address names no family")),
        };
        Ok(Some(SocketAddr::new(ip, port)))
    }
}

#[cfg(test)]
#[path = "../../../tests/link/application/frame.rs"]
mod tests;
