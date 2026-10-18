//! Lossless language-neutral values carried by application events.

const MAX_DEPTH: usize = 64;

const NULL: u8 = 0;
const FALSE: u8 = 1;
const TRUE: u8 = 2;
const INT: u8 = 3;
const FLOAT: u8 = 4;
const STRING: u8 = 5;
const ARRAY: u8 = 6;
const INT_KEY: u8 = 0;
const STRING_KEY: u8 = 1;

/// A language-neutral application-event value that owns every byte it contains.
///
/// Arrays deliberately remain ordered integer-or-string keyed maps. This keeps
/// PHP arrays lossless while JavaScript adapters may project ordinary sequential
/// arrays and string-keyed objects without making JSON the fleet wire format.
#[derive(Clone, Debug, PartialEq)]
pub enum EventValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(Vec<u8>),
    Array(Vec<(EventArrayKey, EventValue)>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventArrayKey {
    Int(i64),
    String(String),
}

impl EventValue {
    /// This value as JSON. Sequential integer keys from zero become an array,
    /// anything else an object; a string that is not UTF-8 is refused.
    pub fn to_json(&self) -> Result<serde_json::Value, String> {
        Ok(match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(value) => serde_json::Value::Bool(*value),
            Self::Int(value) => serde_json::Value::from(*value),
            Self::Float(value) => serde_json::Value::from(*value),
            Self::String(bytes) => serde_json::Value::String(
                String::from_utf8(bytes.clone())
                    .map_err(|_| "string is not UTF-8".to_owned())?,
            ),
            Self::Array(items) => {
                let sequential = items.iter().enumerate().all(
                    |(index, (key, _))| matches!(key, EventArrayKey::Int(i) if *i == index as i64),
                );
                if sequential {
                    serde_json::Value::Array(
                        items
                            .iter()
                            .map(|(_, value)| value.to_json())
                            .collect::<Result<_, _>>()?,
                    )
                } else {
                    let mut object =
                        serde_json::Map::with_capacity(items.len());
                    for (key, value) in items {
                        let key = match key {
                            EventArrayKey::Int(i) => i.to_string(),
                            EventArrayKey::String(s) => s.clone(),
                        };
                        object.insert(key, value.to_json()?);
                    }
                    serde_json::Value::Object(object)
                }
            }
        })
    }

    /// A JSON value as an event value: an array keeps sequential integer
    /// keys, an object its string keys, and a number stays an integer when it
    /// is one.
    pub fn from_json(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(value) => Self::Bool(*value),
            serde_json::Value::Number(number) => match number.as_i64() {
                Some(integer) => Self::Int(integer),
                None => Self::Float(number.as_f64().unwrap_or(f64::NAN)),
            },
            serde_json::Value::String(text) => {
                Self::String(text.as_bytes().to_vec())
            }
            serde_json::Value::Array(items) => Self::Array(
                items
                    .iter()
                    .enumerate()
                    .map(|(index, item)| {
                        (
                            EventArrayKey::Int(index as i64),
                            Self::from_json(item),
                        )
                    })
                    .collect(),
            ),
            serde_json::Value::Object(entries) => Self::Array(
                entries
                    .iter()
                    .map(|(key, item)| {
                        (
                            EventArrayKey::String(key.clone()),
                            Self::from_json(item),
                        )
                    })
                    .collect(),
            ),
        }
    }

    /// Encode this language-neutral value for another Claviron runtime
    /// boundary, after enforcing the same bounded nesting contract used by
    /// application events.
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        let capacity = self.encoded_len()?;
        let mut bytes = Vec::with_capacity(capacity);
        self.encode_into(&mut bytes);
        Ok(bytes)
    }

    /// Decode a language-neutral value produced by [`Self::to_bytes`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        Self::decode(bytes)
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        self.to_bytes().expect("validated application event value")
    }

    pub(crate) fn encoded_len(&self) -> Result<usize, String> {
        self.encoded_len_at(0)
    }

    fn encoded_len_at(
        &self,
        depth: usize,
    ) -> Result<usize, String> {
        if depth > MAX_DEPTH {
            return Err(format!(
                "application event payload exceeds maximum nesting depth {MAX_DEPTH}"
            ));
        }
        match self {
            Self::Null | Self::Bool(_) => Ok(1),
            Self::Int(_) | Self::Float(_) => Ok(9),
            Self::String(value) => {
                5usize.checked_add(value.len()).ok_or_else(|| {
                    "application event payload length overflow".to_owned()
                })
            }
            Self::Array(entries) => {
                let mut len = 5usize;
                for (key, value) in entries {
                    let key_len = match key {
                        EventArrayKey::Int(_) => 9,
                        EventArrayKey::String(key) => {
                            5usize.checked_add(key.len()).ok_or_else(|| {
                                "application event payload length overflow"
                                    .to_owned()
                            })?
                        }
                    };
                    let value_len = value.encoded_len_at(depth + 1)?;
                    len = len
                        .checked_add(key_len)
                        .and_then(|len| len.checked_add(value_len))
                        .ok_or_else(|| {
                            "application event payload length overflow"
                                .to_owned()
                        })?;
                }
                Ok(len)
            }
        }
    }

    fn encode_into(
        &self,
        bytes: &mut Vec<u8>,
    ) {
        match self {
            Self::Null => bytes.push(NULL),
            Self::Bool(false) => bytes.push(FALSE),
            Self::Bool(true) => bytes.push(TRUE),
            Self::Int(value) => {
                bytes.push(INT);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Self::Float(value) => {
                bytes.push(FLOAT);
                bytes.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            Self::String(value) => {
                bytes.push(STRING);
                put_len(bytes, value.len());
                bytes.extend_from_slice(value);
            }
            Self::Array(entries) => {
                bytes.push(ARRAY);
                put_len(bytes, entries.len());
                for (key, value) in entries {
                    match key {
                        EventArrayKey::Int(key) => {
                            bytes.push(INT_KEY);
                            bytes.extend_from_slice(&key.to_le_bytes());
                        }
                        EventArrayKey::String(key) => {
                            bytes.push(STRING_KEY);
                            put_len(bytes, key.len());
                            bytes.extend_from_slice(key.as_bytes());
                        }
                    }
                    value.encode_into(bytes);
                }
            }
        }
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut decoder = Decoder { bytes, offset: 0 };
        let value = decoder.value(0)?;
        if decoder.offset != bytes.len() {
            return Err(
                "application event payload has trailing bytes".to_owned()
            );
        }
        Ok(value)
    }
}

fn put_len(
    bytes: &mut Vec<u8>,
    len: usize,
) {
    bytes.extend_from_slice(&(len as u32).to_le_bytes());
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl Decoder<'_> {
    fn value(
        &mut self,
        depth: usize,
    ) -> Result<EventValue, String> {
        if depth > MAX_DEPTH {
            return Err(format!(
                "application event payload exceeds maximum nesting depth {MAX_DEPTH}"
            ));
        }
        match self.byte()? {
            NULL => Ok(EventValue::Null),
            FALSE => Ok(EventValue::Bool(false)),
            TRUE => Ok(EventValue::Bool(true)),
            INT => Ok(EventValue::Int(i64::from_le_bytes(self.fixed()?))),
            FLOAT => Ok(EventValue::Float(f64::from_bits(u64::from_le_bytes(
                self.fixed()?,
            )))),
            STRING => {
                let len = self.len()?;
                Ok(EventValue::String(self.take(len)?.to_vec()))
            }
            ARRAY => {
                let len = self.len()?;
                if len > self.bytes.len().saturating_sub(self.offset) {
                    return Err(
                        "application event array length exceeds its payload"
                            .to_owned(),
                    );
                }
                let mut entries = Vec::with_capacity(len);
                for _ in 0..len {
                    let key = match self.byte()? {
                        INT_KEY => EventArrayKey::Int(i64::from_le_bytes(
                            self.fixed()?,
                        )),
                        STRING_KEY => {
                            let len = self.len()?;
                            let key = std::str::from_utf8(self.take(len)?)
                                .map_err(|error| {
                                    format!("application event array key is not UTF-8: {error}")
                                })?
                                .to_owned();
                            EventArrayKey::String(key)
                        }
                        tag => {
                            return Err(format!(
                                "unknown application event array key tag {tag}"
                            ));
                        }
                    };
                    entries.push((key, self.value(depth + 1)?));
                }
                Ok(EventValue::Array(entries))
            }
            tag => Err(format!("unknown application event payload tag {tag}")),
        }
    }

    fn byte(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn len(&mut self) -> Result<usize, String> {
        Ok(u32::from_le_bytes(self.fixed()?) as usize)
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], String> {
        self.take(N)?
            .try_into()
            .map_err(|_| "application event payload is truncated".to_owned())
    }

    fn take(
        &mut self,
        len: usize,
    ) -> Result<&[u8], String> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            "application event payload length overflow".to_owned()
        })?;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| {
            "application event payload is truncated".to_owned()
        })?;
        self.offset = end;
        Ok(value)
    }
}

#[cfg(test)]
#[path = "../../tests/event/value.rs"]
mod tests;
