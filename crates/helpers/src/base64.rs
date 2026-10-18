use base64ct::{Base64, Base64UrlUnpadded, Encoding};

pub fn base64_encode(data: &[u8]) -> String {
    Base64UrlUnpadded::encode_string(data)
}

pub fn base64_standard_encode(data: &[u8]) -> String {
    Base64::encode_string(data)
}

pub fn base64_decode(data: &str) -> Result<Vec<u8>, String> {
    Base64UrlUnpadded::decode_vec(data).map_err(|e| e.to_string())
}
