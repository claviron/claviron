use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

// `new_from_slice` moved to `KeyInit` in the digest 0.11 generation; `Mac`
// alone no longer carries it.
use hmac::{Hmac, KeyInit, Mac};
use md5::{Digest, Md5};
use sha2::Sha256;

use crate::{PusherAppKey, PusherAppSecret};

const AUTH_WINDOW_SECONDS: i64 = 600;

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum AuthenticationError {
    #[error("missing authentication query parameter '{0}'")]
    Missing(&'static str),
    #[error("duplicate authentication query parameter '{0}'")]
    Duplicate(String),
    #[error("unsupported Pusher authentication version")]
    Version,
    #[error("invalid Pusher application key")]
    Key,
    #[error("invalid Pusher authentication timestamp")]
    Timestamp,
    #[error("Pusher authentication timestamp is outside the allowed window")]
    Expired,
    #[error("request body digest does not match body_md5")]
    BodyDigest,
    #[error("invalid Pusher authentication signature")]
    Signature,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum ChannelAuthorizationError {
    #[error("missing channel authorization")]
    Missing,
    #[error("invalid channel authorization format")]
    Format,
    #[error("channel authorization belongs to another Pusher application")]
    Key,
    #[error("presence channel authorization requires channel_data")]
    MissingChannelData,
    #[error("private channel authorization must not contain channel_data")]
    UnexpectedChannelData,
    #[error("invalid channel authorization signature")]
    Signature,
}

pub fn authorize_channel(
    socket_id: &str,
    channel: &str,
    channel_data: Option<&str>,
    key: &PusherAppKey,
    secret: &PusherAppSecret,
) -> String {
    let value = channel_authorization_value(socket_id, channel, channel_data);
    let mut signer = Hmac::<Sha256>::new_from_slice(secret.expose().as_bytes())
        .expect("HMAC accepts keys of any length");
    signer.update(value.as_bytes());
    format!("{}:{}", key.as_str(), hex::encode(signer.finalize().into_bytes()))
}

pub fn verify_channel_authorization(
    authorization: Option<&str>,
    socket_id: &str,
    channel: &str,
    channel_data: Option<&str>,
    key: &PusherAppKey,
    secret: &PusherAppSecret,
) -> Result<(), ChannelAuthorizationError> {
    let authorization =
        authorization.ok_or(ChannelAuthorizationError::Missing)?;
    let (authorization_key, signature) = authorization
        .split_once(':')
        .ok_or(ChannelAuthorizationError::Format)?;
    if authorization_key != key.as_str() {
        return Err(ChannelAuthorizationError::Key);
    }
    let channel_data = if channel.starts_with("presence-") {
        Some(
            channel_data
                .ok_or(ChannelAuthorizationError::MissingChannelData)?,
        )
    } else {
        if channel_data.is_some() {
            return Err(ChannelAuthorizationError::UnexpectedChannelData);
        }
        None
    };
    let signature = hex::decode(signature)
        .map_err(|_| ChannelAuthorizationError::Format)?;
    let value = channel_authorization_value(socket_id, channel, channel_data);
    let mut verifier =
        Hmac::<Sha256>::new_from_slice(secret.expose().as_bytes())
            .map_err(|_| ChannelAuthorizationError::Signature)?;
    verifier.update(value.as_bytes());
    verifier
        .verify_slice(&signature)
        .map_err(|_| ChannelAuthorizationError::Signature)
}

fn channel_authorization_value(
    socket_id: &str,
    channel: &str,
    channel_data: Option<&str>,
) -> String {
    match channel_data {
        Some(channel_data) => format!("{socket_id}:{channel}:{channel_data}"),
        None => format!("{socket_id}:{channel}"),
    }
}

pub fn authenticate(
    method: &str,
    path: &str,
    query: Option<&str>,
    body: &[u8],
    key: &PusherAppKey,
    secret: &PusherAppSecret,
) -> Result<(), AuthenticationError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AuthenticationError::Timestamp)?
        .as_secs() as i64;
    authenticate_at(method, path, query, body, key, secret, now)
}

pub fn authentication_query(
    method: &str,
    path: &str,
    body: &[u8],
    key: &PusherAppKey,
    secret: &PusherAppSecret,
) -> Result<String, AuthenticationError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AuthenticationError::Timestamp)?
        .as_secs() as i64;
    Ok(authentication_query_at(method, path, body, key, secret, now))
}

pub fn authentication_query_at(
    method: &str,
    path: &str,
    body: &[u8],
    key: &PusherAppKey,
    secret: &PusherAppSecret,
    timestamp: i64,
) -> String {
    let mut parameters = BTreeMap::from([
        ("auth_key".to_owned(), key.as_str().to_owned()),
        ("auth_timestamp".to_owned(), timestamp.to_string()),
        ("auth_version".to_owned(), "1.0".to_owned()),
    ]);
    if !body.is_empty() {
        parameters
            .insert("body_md5".to_owned(), hex::encode(Md5::digest(body)));
    }
    let canonical_query = canonical_query(&parameters);
    let string_to_sign =
        format!("{}\n{path}\n{canonical_query}", method.to_ascii_uppercase());
    let mut signer = Hmac::<Sha256>::new_from_slice(secret.expose().as_bytes())
        .expect("HMAC accepts keys of any length");
    signer.update(string_to_sign.as_bytes());
    let signature = hex::encode(signer.finalize().into_bytes());
    format!("{canonical_query}&auth_signature={signature}")
}

pub fn authenticate_at(
    method: &str,
    path: &str,
    query: Option<&str>,
    body: &[u8],
    key: &PusherAppKey,
    secret: &PusherAppSecret,
    now_unix_seconds: i64,
) -> Result<(), AuthenticationError> {
    let mut parameters = BTreeMap::<String, String>::new();
    for (key, value) in
        url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
    {
        let key = key.to_ascii_lowercase();
        if parameters.insert(key.clone(), value.into_owned()).is_some() {
            return Err(AuthenticationError::Duplicate(key));
        }
    }

    let signature = required(&parameters, "auth_signature")?;
    if required(&parameters, "auth_key")? != key.as_str() {
        return Err(AuthenticationError::Key);
    }
    if required(&parameters, "auth_version")? != "1.0" {
        return Err(AuthenticationError::Version);
    }
    let timestamp = required(&parameters, "auth_timestamp")?
        .parse::<i64>()
        .map_err(|_| AuthenticationError::Timestamp)?;
    if now_unix_seconds.abs_diff(timestamp) > AUTH_WINDOW_SECONDS as u64 {
        return Err(AuthenticationError::Expired);
    }

    if !body.is_empty() {
        let expected = required(&parameters, "body_md5")?;
        let actual = hex::encode(Md5::digest(body));
        if actual != expected {
            return Err(AuthenticationError::BodyDigest);
        }
    }

    let canonical_query = canonical_query(&parameters);
    let string_to_sign =
        format!("{}\n{path}\n{canonical_query}", method.to_ascii_uppercase());
    let signature =
        hex::decode(signature).map_err(|_| AuthenticationError::Signature)?;
    let mut verifier =
        Hmac::<Sha256>::new_from_slice(secret.expose().as_bytes())
            .map_err(|_| AuthenticationError::Signature)?;
    verifier.update(string_to_sign.as_bytes());
    verifier
        .verify_slice(&signature)
        .map_err(|_| AuthenticationError::Signature)
}

fn canonical_query(parameters: &BTreeMap<String, String>) -> String {
    parameters
        .iter()
        .filter(|(name, _)| name.as_str() != "auth_signature")
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn required<'a>(
    parameters: &'a BTreeMap<String, String>,
    name: &'static str,
) -> Result<&'a str, AuthenticationError> {
    parameters
        .get(name)
        .map(String::as_str)
        .ok_or(AuthenticationError::Missing(name))
}

#[cfg(test)]
#[path = "../tests/auth.rs"]
mod tests;
