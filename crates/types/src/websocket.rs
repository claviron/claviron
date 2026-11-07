//! What a WebSocket carries, whichever door or carrier it came through.

/// A WebSocket control payload's protocol maximum.
pub const CONTROL_BYTES: usize = 125;

/// One WebSocket message, as an application sends and receives it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    Text(String),
    Binary(Vec<u8>),
    /// The close handshake: a status code (1000 is a normal close) and a
    /// reason, empty when none was given.
    Close {
        code: u16,
        reason: String,
    },
}

impl Message {
    /// Whether a close with this code and reason may be sent. 1005 and 1006
    /// are observations an application receives, never codes on the wire.
    pub fn sendable_close(
        code: u16,
        reason: &str,
    ) -> bool {
        matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999)
            && reason.len() <= CONTROL_BYTES - 2
    }
}
