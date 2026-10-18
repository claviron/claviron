//! One log record, as a producer builds it and as it travels the ring.
//!
//! Encoded once by the producer, decoded once by the writer; formatting
//! happens in the writer. The encoding is little-endian and self-describing
//! enough to survive a writer that is newer than its producers:
//!
//! ```text
//! [version u8][flags u8][channel u16][level u8][time: unix nanos i64]
//! [target: u16 len, bytes][message: u32 len, bytes]
//! [field count u16] then per field: [key: u16 len, bytes][tag u8][value]
//! ```
//!
//! A record that does not fit one frame keeps its header, target and as much
//! of the message as fits; fields are kept in order while they fit after the
//! message. Anything left out sets [`Decoded::truncated`].

use std::time::SystemTime;

const VERSION: u8 = 1;
const FLAG_TRUNCATED: u8 = 1;

const TAG_STR: u8 = 0;
const TAG_U64: u8 = 1;
const TAG_I64: u8 = 2;
const TAG_F64: u8 = 3;
const TAG_BOOL: u8 = 4;

/// A declared channel, resolved from its name once.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct ChannelId(pub(crate) u16);

impl ChannelId {
    /// The number a channel travels as — to PHP, and back.
    pub fn raw(self) -> u16 {
        self.0
    }

    /// A number handed back by PHP. `emit` refuses one that names no
    /// declared channel.
    pub fn from_raw(raw: u16) -> Self {
        Self(raw)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum Level {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            1 => Self::Error,
            2 => Self::Warn,
            3 => Self::Info,
            4 => Self::Debug,
            5 => Self::Trace,
            _ => return None,
        })
    }
}

/// A field value, typed so a JSON line says `200`, not `"200"`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value<'a> {
    Str(&'a str),
    U64(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
}

/// What a producer hands over. Borrowed: nothing is copied until it is
/// encoded into the frame.
#[derive(Clone, Copy, Debug)]
pub struct Record<'a> {
    pub level: Level,
    pub target: &'a str,
    pub message: &'a str,
    pub fields: &'a [(&'a str, Value<'a>)],
}

/// A record as the writer reads it back.
#[derive(Clone, Debug, PartialEq)]
pub struct Decoded {
    pub channel: ChannelId,
    pub level: Level,
    /// Unix time in nanoseconds, taken by the producer.
    pub time: i64,
    pub target: String,
    pub message: String,
    pub fields: Vec<(String, OwnedValue)>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OwnedValue {
    Str(String),
    U64(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
}

/// Encode `record` for `channel` into at most `capacity` bytes.
pub(crate) fn encode(
    channel: ChannelId,
    record: &Record<'_>,
    time: SystemTime,
    capacity: usize,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(capacity.min(512));
    let mut truncated = false;
    out.push(VERSION);
    out.push(0);
    out.extend_from_slice(&channel.0.to_le_bytes());
    out.push(record.level as u8);
    out.extend_from_slice(&unix_nanos(time).to_le_bytes());

    let target = prefix(record.target, usize::from(u16::MAX));
    truncated |= target.len() < record.target.len();
    out.extend_from_slice(&(target.len() as u16).to_le_bytes());
    out.extend_from_slice(target.as_bytes());

    // Room for the message, its length and the field count.
    let room = capacity.saturating_sub(out.len() + 4 + 2);
    let message = prefix(record.message, room);
    truncated |= message.len() < record.message.len();
    out.extend_from_slice(&(message.len() as u32).to_le_bytes());
    out.extend_from_slice(message.as_bytes());

    let count_at = out.len();
    out.extend_from_slice(&0u16.to_le_bytes());
    let mut count: u16 = 0;
    for (key, value) in record.fields {
        let size = 2 + key.len() + 1 + value_size(value);
        if key.len() > usize::from(u16::MAX)
            || out.len() + size > capacity
            || count == u16::MAX
        {
            truncated = true;
            continue;
        }
        out.extend_from_slice(&(key.len() as u16).to_le_bytes());
        out.extend_from_slice(key.as_bytes());
        put_value(&mut out, value);
        count += 1;
    }
    out[count_at..count_at + 2].copy_from_slice(&count.to_le_bytes());
    if truncated {
        out[1] |= FLAG_TRUNCATED;
    }
    out
}

/// Decode one frame; `None` for a frame this writer does not understand.
pub(crate) fn decode(bytes: &[u8]) -> Option<Decoded> {
    let mut input = Reader(bytes);
    if input.u8()? != VERSION {
        return None;
    }
    let flags = input.u8()?;
    let channel = ChannelId(u16::from_le_bytes(input.array()?));
    let level = Level::from_u8(input.u8()?)?;
    let time = i64::from_le_bytes(input.array()?);
    let target_len = u16::from_le_bytes(input.array()?) as usize;
    let target = input.text(target_len)?;
    let message_len = u32::from_le_bytes(input.array()?) as usize;
    let message = input.text(message_len)?;
    let count = u16::from_le_bytes(input.array()?);
    let mut fields = Vec::with_capacity(usize::from(count));
    for _ in 0..count {
        let key_len = u16::from_le_bytes(input.array()?) as usize;
        let key = input.text(key_len)?;
        let value = match input.u8()? {
            TAG_STR => {
                let len = u32::from_le_bytes(input.array()?) as usize;
                OwnedValue::Str(input.text(len)?)
            }
            TAG_U64 => OwnedValue::U64(u64::from_le_bytes(input.array()?)),
            TAG_I64 => OwnedValue::I64(i64::from_le_bytes(input.array()?)),
            TAG_F64 => OwnedValue::F64(f64::from_le_bytes(input.array()?)),
            TAG_BOOL => OwnedValue::Bool(input.u8()? != 0),
            _ => return None,
        };
        fields.push((key, value));
    }
    Some(Decoded {
        channel,
        level,
        time,
        target,
        message,
        fields,
        truncated: flags & FLAG_TRUNCATED != 0,
    })
}

fn unix_nanos(time: SystemTime) -> i64 {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(since) => i64::try_from(since.as_nanos()).unwrap_or(i64::MAX),
        Err(before) => {
            i64::try_from(before.duration().as_nanos()).map_or(i64::MIN, |n| -n)
        }
    }
}

/// The longest prefix of `text` within `max` bytes, cut at a char boundary.
fn prefix(
    text: &str,
    max: usize,
) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn value_size(value: &Value<'_>) -> usize {
    match value {
        Value::Str(text) => 4 + text.len(),
        Value::U64(_) | Value::I64(_) | Value::F64(_) => 8,
        Value::Bool(_) => 1,
    }
}

fn put_value(
    out: &mut Vec<u8>,
    value: &Value<'_>,
) {
    match value {
        Value::Str(text) => {
            out.push(TAG_STR);
            out.extend_from_slice(&(text.len() as u32).to_le_bytes());
            out.extend_from_slice(text.as_bytes());
        }
        Value::U64(number) => {
            out.push(TAG_U64);
            out.extend_from_slice(&number.to_le_bytes());
        }
        Value::I64(number) => {
            out.push(TAG_I64);
            out.extend_from_slice(&number.to_le_bytes());
        }
        Value::F64(number) => {
            out.push(TAG_F64);
            out.extend_from_slice(&number.to_le_bytes());
        }
        Value::Bool(flag) => {
            out.push(TAG_BOOL);
            out.push(u8::from(*flag));
        }
    }
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn u8(&mut self) -> Option<u8> {
        let (&first, rest) = self.0.split_first()?;
        self.0 = rest;
        Some(first)
    }

    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_at_checked(N)?;
        self.0 = rest;
        head.try_into().ok()
    }

    fn text(
        &mut self,
        len: usize,
    ) -> Option<String> {
        let (head, rest) = self.0.split_at_checked(len)?;
        self.0 = rest;
        String::from_utf8(head.to_vec()).ok()
    }
}

#[cfg(test)]
#[path = "../../tests/pipeline/record.rs"]
mod tests;
