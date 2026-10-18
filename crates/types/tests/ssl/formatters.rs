use super::certificate_chain_to_pem;
use crate::ssl::generators::{generate_ca_cert, generate_ca_key};
use crate::ssl::parsers::concat_chain_bytes_from_pem;

#[test]
fn concatenated_der_round_trips_through_public_fullchain_pem() {
    let key = generate_ca_key().unwrap();
    let certificate = generate_ca_cert(
        "test-ca",
        key.serialize_pem().as_bytes(),
        1,
        None,
        None,
    )
    .unwrap();
    let der = concat_chain_bytes_from_pem(&certificate).unwrap();

    let fullchain = certificate_chain_to_pem(&der).unwrap();

    assert_eq!(concat_chain_bytes_from_pem(fullchain.as_bytes()).unwrap(), der);
}
