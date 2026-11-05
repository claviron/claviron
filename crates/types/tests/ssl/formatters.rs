use super::certificate_chain_to_pem;
use crate::ssl::generators::{generate_ca_cert, generate_ca_key};
use crate::ssl::parsers::concat_chain_bytes_from_pem;

#[test]
fn concatenated_der_round_trips_through_public_fullchain_pem() {
    let key = generate_ca_key().unwrap();
    let der = generate_ca_cert(
        &"test-ca".parse().unwrap(),
        &key,
        1,
        &Default::default(),
    )
    .unwrap();

    let fullchain = certificate_chain_to_pem(&der).unwrap();

    assert_eq!(
        concat_chain_bytes_from_pem(fullchain.as_bytes()).unwrap(),
        der.as_ref()
    );
}
