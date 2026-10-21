//! What crosses the inbox: one exchange's control, never its body.
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
            _ => Err(Error::Malformed("unknown transport frame kind")),
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
}

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
            return Err(Error::Malformed("invalid transport control identity"));
        }
        let payload_len = u16::try_from(self.payload.len())
            .map_err(|_| Error::FrameTooLarge)?;
        let mut frame = Vec::with_capacity(
            8 + application.len()
                + name.len()
                + runtime.len()
                + target.len()
                + self.payload.len(),
        );
        frame.extend_from_slice(&[
            FrameKind::Control as u8,
            2,
            application.len() as u8,
            name.len() as u8,
            runtime.len() as u8,
            target.len() as u8,
        ]);
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(application);
        frame.extend_from_slice(name);
        frame.extend_from_slice(runtime);
        frame.extend_from_slice(target);
        frame.extend_from_slice(&self.payload);
        Ok(frame)
    }

    pub fn decode(frame: &[u8]) -> Result<Self> {
        let [
            3,
            2,
            application_len,
            name_len,
            runtime_len,
            target_len,
            len_lo,
            len_hi,
            rest @ ..,
        ] = frame
        else {
            return Err(Error::Malformed("invalid transport control header"));
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
            return Err(Error::Malformed("invalid transport control length"));
        }
        let application = String::from_utf8(rest[..application_len].to_vec())
            .map_err(|_| {
            Error::Malformed("invalid transport control application")
        })?;
        let name = String::from_utf8(
            rest[application_len..application_len + name_len].to_vec(),
        )
        .map_err(|_| Error::Malformed("invalid transport control name"))?;
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
        Ok(Self { application, name, payload, target })
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

/// One request, without its body.
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
    /// Where the body is, if there is one. Absent means no body at all, not
    /// an empty one that still needs reading.
    pub body: Option<Ticket>,
    /// A WebSocket upgrade. The application answers 101 to accept or any
    /// other status to refuse; once accepted, both directions of the slot
    /// carry [`super::message`] records instead of bytes of a body.
    pub upgrade: bool,
}

/// One reply, without its body.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExchangeReply {
    pub correlation: u64,
    pub status: u16,
    pub headers: Headers,
    pub body: Option<Ticket>,
}

const FLAG_BODY: u8 = 1 << 0;
const FLAG_CLIENT: u8 = 1 << 1;
const FLAG_SERVER: u8 = 1 << 2;
/// A request asking to become a WebSocket.
const FLAG_UPGRADE: u8 = 1 << 3;

impl Exchange {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(512);
        let mut flags = 0u8;
        if self.body.is_some() {
            flags |= FLAG_BODY;
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

        put_ticket(&mut out, self.body.as_ref());
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

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
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

        let body = cursor.ticket(flags & FLAG_BODY != 0)?;
        let client = cursor.addr(flags & FLAG_CLIENT != 0)?;
        let server = cursor.addr(flags & FLAG_SERVER != 0)?;

        Ok(Self {
            correlation,
            origin,
            method: cursor.string()?,
            scheme: cursor.string()?,
            version,
            path: cursor.string()?,
            raw_path: cursor.bytes()?.to_vec(),
            query: cursor.bytes()?.to_vec(),
            headers: cursor.headers()?,
            client,
            server,
            body,
            upgrade: flags & FLAG_UPGRADE != 0,
        })
    }
}

impl ExchangeReply {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(256);
        let flags = if self.body.is_some() { FLAG_BODY } else { 0 };

        out.push(FrameKind::Reply as u8);
        out.push(HttpVersion::default() as u8);
        out.push(flags);
        out.push(0);
        out.extend_from_slice(&0u16.to_le_bytes()); // origin, a request's field
        out.extend_from_slice(&self.status.to_le_bytes());
        out.extend_from_slice(&self.correlation.to_le_bytes());

        put_ticket(&mut out, self.body.as_ref());
        put_headers(&mut out, &self.headers)?;
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
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

        Ok(Self {
            correlation,
            status,
            body: cursor.ticket(flags & FLAG_BODY != 0)?,
            headers: cursor.headers()?,
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
#[path = "../../../tests/link/transport/frame.rs"]
mod tests;
