use crate::invoke::{CallError as Error, CallResult as Result, Failure};

use super::Peer;
use orbitive::NetId64;
use orbitive::stream::{Side, StreamId, Ticket};

// Claviron protocol tag, outside the Link-reserved F0..FF and existing HTTP
// tags 1..5. Version 1 fixes the endpoint stamps and single-frame length fields.
pub(super) const TAG: u8 = 0x49;
const VERSION: u8 = 1;
pub(super) const HEADER: usize = 59;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Frame {
    pub from: Peer,
    pub to: Peer,
    pub sequence: u64,
    pub reply_limit: u32,
    pub ticket: Option<Ticket>,
    pub body: Body,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Body {
    Request { operation: String, payload: Vec<u8> },
    Reply(std::result::Result<Vec<u8>, Failure>),
}

impl Frame {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let (kind, code, operation, payload): (u8, u8, &[u8], &[u8]) =
            match &self.body {
                Body::Request { operation, payload } => {
                    (1, 0, operation.as_bytes(), payload)
                }
                Body::Reply(Ok(payload)) => (2, 0, &[], payload),
                Body::Reply(Err(failure)) => match failure {
                    Failure::Busy => (2, 1, &[], &[]),
                    Failure::Closing => (2, 2, &[], &[]),
                    Failure::Unsupported => (2, 3, &[], &[]),
                    Failure::ResponseTooLarge => (2, 4, &[], &[]),
                    Failure::Rejected(payload) => (2, 5, &[], payload),
                },
            };
        let operation_len = u16::try_from(operation.len())
            .map_err(|_| Error::PayloadTooLarge)?;
        let payload_len =
            u32::try_from(payload.len()).map_err(|_| Error::PayloadTooLarge)?;
        if self.sequence == 0 || (kind == 1 && operation.is_empty()) {
            return Err(Error::Protocol("empty operation or zero sequence"));
        }
        let mut out =
            Vec::with_capacity(HEADER + operation.len() + payload.len());
        out.extend_from_slice(&[TAG, VERSION, kind]);
        out.extend_from_slice(&self.from.lane.to_le_bytes());
        out.extend_from_slice(&self.from.incarnation.to_le_bytes());
        out.extend_from_slice(&self.to.lane.to_le_bytes());
        out.extend_from_slice(&self.to.incarnation.to_le_bytes());
        out.extend_from_slice(&self.sequence.to_le_bytes());
        out.push(code);
        out.extend_from_slice(&self.reply_limit.to_le_bytes());
        out.extend_from_slice(&operation_len.to_le_bytes());
        out.extend_from_slice(&payload_len.to_le_bytes());
        if let Some(ticket) = self.ticket {
            out.extend_from_slice(&ticket.id.net_id().raw().to_le_bytes());
            out.push(match ticket.side {
                Side::A => 0,
                Side::B => 1,
            });
            out.extend_from_slice(&ticket.epoch.to_le_bytes());
        } else {
            out.extend_from_slice(&[0; 17]);
        }
        out.extend_from_slice(operation);
        out.extend_from_slice(payload);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER || bytes[0..2] != [TAG, VERSION] {
            return Err(Error::Protocol("header or version"));
        }
        let u16_at = |i| u16::from_le_bytes([bytes[i], bytes[i + 1]]);
        let u64_at = |i| {
            u64::from_le_bytes(
                bytes[i..i + 8].try_into().expect("checked header"),
            )
        };
        let operation_len = u16_at(36) as usize;
        let payload_len = u32::from_le_bytes(
            bytes[38..42].try_into().expect("checked header"),
        ) as usize;
        if bytes.len().checked_sub(HEADER + operation_len) != Some(payload_len)
        {
            return Err(Error::Protocol("frame length"));
        }
        let payload = &bytes[HEADER + operation_len..];
        let ticket = if bytes[42..59].iter().all(|&byte| byte == 0) {
            None
        } else {
            Some(Ticket {
                id: StreamId::from_net_id(NetId64::from_raw(u64_at(42))),
                side: match bytes[50] {
                    0 => Side::A,
                    1 => Side::B,
                    _ => return Err(Error::Protocol("ticket side")),
                },
                epoch: u64_at(51),
            })
        };
        let body = match (bytes[2], bytes[31]) {
            (1, 0) if operation_len > 0 => Body::Request {
                operation: std::str::from_utf8(
                    &bytes[HEADER..HEADER + operation_len],
                )
                .map_err(|_| Error::Protocol("operation encoding"))?
                .to_owned(),
                payload: payload.to_vec(),
            },
            (2, code) if operation_len == 0 => Body::Reply(match code {
                0 => Ok(payload.to_vec()),
                1 if payload.is_empty() => Err(Failure::Busy),
                2 if payload.is_empty() => Err(Failure::Closing),
                3 if payload.is_empty() => Err(Failure::Unsupported),
                4 if payload.is_empty() => Err(Failure::ResponseTooLarge),
                5 => Err(Failure::Rejected(payload.to_vec())),
                _ => return Err(Error::Protocol("result code")),
            }),
            _ => return Err(Error::Protocol("message kind")),
        };
        let sequence = u64_at(23);
        if sequence == 0 {
            return Err(Error::Protocol("zero sequence"));
        }
        Ok(Self {
            from: Peer { lane: u16_at(3), incarnation: u64_at(5) },
            to: Peer { lane: u16_at(13), incarnation: u64_at(15) },
            sequence,
            reply_limit: u32::from_le_bytes(
                bytes[32..36].try_into().expect("checked header"),
            ),
            ticket,
            body,
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/link/invoke/frame.rs"]
mod tests;
