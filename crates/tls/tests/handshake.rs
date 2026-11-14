use std::io;
use std::sync::Arc;

use super::{AlpnProfile, AlpnProfiles};

struct Profile(Box<[Box<[u8]>]>);

impl Profile {
    fn answering(ids: &[&[u8]]) -> Arc<Self> {
        Arc::new(Self(ids.iter().map(|id| Box::from(*id)).collect()))
    }
}

impl AlpnProfile for Profile {
    fn protocols(&self) -> &[Box<[u8]>] {
        &self.0
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
    profiles.register(Profile::answering(&[b"acme-tls/1"])).unwrap();

    let conflict = profiles
        .register(Profile::answering(&[b"example/1", b"acme-tls/1"]))
        .unwrap_err();
    assert_eq!(&*conflict.protocol, b"acme-tls/1");

    // The refused profile's other id was not taken either.
    profiles.register(Profile::answering(&[b"example/1"])).unwrap();
}
