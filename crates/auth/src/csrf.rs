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
        mac.update(nonce);
        if let Some(scope) = scope {
            mac.update(scope);
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
        format!("{}.{}", hex::encode(nonce), hex::encode(mac))
    }

    /// Verify the signature against exactly the supplied scope.
    pub fn verify(
        &self,
        token: &str,
        scope: Option<&[u8]>,
    ) -> bool {
        let Some((nonce, mac)) = token.trim().split_once('.') else {
            return false;
        };
        let (Ok(nonce), Ok(mac)) = (hex::decode(nonce), hex::decode(mac))
        else {
            return false;
        };
        self.mac(&nonce, scope).verify_slice(&mac).is_ok()
    }
}
