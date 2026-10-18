use std::time::Duration;

use claviron_helpers::de::deserialize_duration;
use serde::Deserialize;

macro_rules! path_identifier {
    ($name:ident, $label:literal) => {
        #[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize)]
        #[serde(try_from = "String")]
        pub struct $name(String);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = String;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                let value = value.trim();
                if value.is_empty() {
                    return Err(concat!($label, " cannot be empty").to_owned());
                }
                if value.bytes().any(|byte| matches!(byte, b'/' | b'?' | b'#'))
                {
                    return Err(format!(
                        concat!(
                            $label,
                            " contains a reserved path character: '{}'"
                        ),
                        value
                    ));
                }
                Ok(Self(value.to_owned()))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(
                &self,
                formatter: &mut std::fmt::Formatter<'_>,
            ) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

path_identifier!(PusherAppId, "Pusher app id");
path_identifier!(PusherAppKey, "Pusher app key");

#[derive(Clone, Eq, PartialEq, Deserialize)]
#[serde(try_from = "String")]
pub struct PusherAppSecret(String);

impl PusherAppSecret {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for PusherAppSecret {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let value = value.trim();
        if value.is_empty() {
            return Err("Pusher app secret cannot be empty".to_owned());
        }
        Ok(Self(value.to_owned()))
    }
}

impl std::fmt::Debug for PusherAppSecret {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        formatter.write_str("PusherAppSecret([REDACTED])")
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PusherConfig {
    pub target: PusherTarget,
}

#[derive(Clone, Debug)]
pub enum PusherTarget {
    /// Route Pusher traffic to the Broadcast service in this Claviron fleet.
    Fleet,
    /// Proxy Pusher traffic to an external HTTP/1.1-compatible server.
    Http(HttpTargetConfig),
}

impl<'de> Deserialize<'de> for PusherTarget {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Representation {
            Name(String),
            Http(HttpRepresentation),
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct HttpRepresentation {
            http: HttpTargetConfig,
        }

        match Representation::deserialize(deserializer)? {
            Representation::Name(name) if name == "fleet" => Ok(Self::Fleet),
            Representation::Name(name) => {
                Err(serde::de::Error::custom(format!(
                    "unknown Pusher target '{name}', expected 'fleet' or an 'http' mapping"
                )))
            }
            Representation::Http(target) => Ok(Self::Http(target.http)),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpTargetConfig {
    pub address: String,
    #[serde(
        default = "default_request_timeout",
        deserialize_with = "deserialize_duration"
    )]
    request_timeout: Duration,
    #[serde(
        default = "default_connect_timeout",
        deserialize_with = "deserialize_duration"
    )]
    connect_timeout: Duration,
    #[serde(
        default = "default_idle_timeout",
        deserialize_with = "deserialize_duration"
    )]
    idle_timeout: Duration,
}

impl HttpTargetConfig {
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }
}

fn default_request_timeout() -> Duration {
    Duration::from_secs(1)
}

fn default_connect_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_idle_timeout() -> Duration {
    Duration::from_secs(120)
}
