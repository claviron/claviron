//! Signed double-submit tokens bound to a caller-owned session scope.

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

/// The host supplies a dedicated, domain-separated key. This primitive never
/// reads configuration, resolves a session, or chooses cookie policy.
#[derive(Clone)]
pub struct Csrf {
    key: [u8; 32],
}

impl Csrf {
    pub const COOKIE: &'static str = "XSRF-TOKEN";
    pub const HEADER: &'static str = "x-xsrf-token";

    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    fn mac(
        &self,
        nonce: &[u8],
        scope: Option<&[u8]>,
    ) -> Hmac<Sha256> {
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&self.key)
            .expect("HMAC accepts a 32-byte key");
        // Versioned domain and explicit scope framing prevent token fields
        // from being reinterpreted under another scope or protocol.
        mac.update(b"claviron-csrf-v1\0");
        mac.update(nonce);
        match scope {
            None => mac.update(&[0]),
            Some(scope) => {
                mac.update(&[1]);
                mac.update(&(scope.len() as u64).to_be_bytes());
                mac.update(scope);
            }
        }
        mac
    }

    /// Issue a fresh token. `None` denotes the guest scope.
    pub fn issue(
        &self,
        scope: Option<&[u8]>,
    ) -> String {
        let nonce = uuid::Uuid::new_v4().into_bytes();
        let mac = self.mac(&nonce, scope).finalize().into_bytes();
        format!("v1.{}.{}", hex::encode(nonce), hex::encode(mac))
    }

    /// Verify the signature against exactly the supplied scope.
    pub fn verify(
        &self,
        token: &str,
        scope: Option<&[u8]>,
    ) -> bool {
        let Some((nonce, mac)) = token
            .trim()
            .strip_prefix("v1.")
            .and_then(|token| token.split_once('.'))
        else {
            return false;
        };
        // UUID nonces are exactly 16 bytes and SHA-256 MACs exactly 32.
        // Reject lengths before decoding; attacker input allocates nothing.
        if nonce.len() != 32 || mac.len() != 64 {
            return false;
        }
        let mut nonce_bytes = [0; 16];
        let mut mac_bytes = [0; 32];
        if hex::decode_to_slice(nonce, &mut nonce_bytes).is_err()
            || hex::decode_to_slice(mac, &mut mac_bytes).is_err()
        {
            return false;
        }
        self.mac(&nonce_bytes, scope).verify_slice(&mac_bytes).is_ok()
    }
}

#[cfg(test)]
#[path = "../tests/csrf.rs"]
mod tests;
