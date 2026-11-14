use std::io;
use std::sync::Arc;

use super::{AlpnProfile, AlpnProfiles};

struct Profile(&'static [&'static [u8]]);

impl AlpnProfile for Profile {
    fn protocols(&self) -> &[&'static [u8]] {
        self.0
    }

    fn handshake(
        &self,
        _: &str,
        _: &rustls::server::ClientHello<'_>,
    ) -> io::Result<Option<Arc<rustls::ServerConfig>>> {
        Ok(None)
    }
}

#[test]
fn a_protocol_has_one_profile_and_a_refused_profile_takes_none() {
    let profiles = AlpnProfiles::default();
    profiles.register(Arc::new(Profile(&[b"acme-tls/1"]))).unwrap();

    let conflict = profiles
        .register(Arc::new(Profile(&[b"example/1", b"acme-tls/1"])))
        .unwrap_err();
    assert_eq!(conflict.protocol, b"acme-tls/1");

    // The refused profile's other id was not taken either.
    profiles.register(Arc::new(Profile(&[b"example/1"]))).unwrap();
}
