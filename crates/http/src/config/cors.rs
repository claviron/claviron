use std::fmt;

use once_cell::sync::OnceCell;
use regex::RegexSet;
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CorsPolicy {
    Disabled,
    Permissive,
    Allowlist,
}

#[derive(Clone, Debug)]
pub struct CorsConfig {
    pub policy: CorsPolicy,
    pub credentials: bool,
    pub origins: Vec<String>,     // serde-friendly
    compiled: OnceCell<RegexSet>, // compiled at first use
}

impl CorsConfig {
    /// Compile allowlist once; cache for reuse.
    fn regex_set(&self) -> Result<&RegexSet, regex::Error> {
        self.compiled.get_or_try_init(|| RegexSet::new(&self.origins))
    }

    pub fn allows_origin(
        &self,
        origin: &str,
    ) -> bool {
        match self.policy {
            CorsPolicy::Disabled => false,
            CorsPolicy::Permissive => true,
            CorsPolicy::Allowlist => {
                // First check exact string matches
                if self.origins.iter().any(|allowed| allowed == origin) {
                    return true;
                }

                // Then try regex matching for patterns starting with ^
                match self.regex_set() {
                    Ok(set) => set.is_match(origin),
                    Err(_) => false,
                }
            }
        }
    }

    pub fn enabled(&self) -> bool {
        self.policy != CorsPolicy::Disabled
    }
}

impl<'de> Deserialize<'de> for CorsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CorsVisitor;

        impl<'de> Visitor<'de> for CorsVisitor {
            type Value = CorsConfig;

            fn expecting(
                &self,
                f: &mut fmt::Formatter,
            ) -> fmt::Result {
                f.write_str("a string (\"disabled\" | \"permissive\" | \"allowlist\") or a map {policy, credentials, origins}")
            }

            fn visit_str<E>(
                self,
                v: &str,
            ) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let policy = match v {
                    "disabled" => CorsPolicy::Disabled,
                    "permissive" => CorsPolicy::Permissive,
                    "allowlist" => CorsPolicy::Allowlist,
                    other => {
                        return Err(E::unknown_variant(
                            other,
                            &["disabled", "permissive", "allowlist"],
                        ));
                    }
                };
                Ok(CorsConfig {
                    policy,
                    credentials: false,
                    origins: Vec::new(),
                    compiled: OnceCell::new(),
                })
            }

            fn visit_map<M>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                #[derive(Deserialize)]
                struct CorsConfigInner {
                    policy: CorsPolicy,
                    #[serde(default)]
                    credentials: bool,
                    #[serde(default)]
                    origins: Vec<String>,
                }

                let inner = CorsConfigInner::deserialize(
                    de::value::MapAccessDeserializer::new(&mut map),
                )?;

                Ok(CorsConfig {
                    policy: inner.policy,
                    credentials: inner.credentials,
                    origins: inner.origins,
                    compiled: OnceCell::new(),
                })
            }
        }

        deserializer.deserialize_any(CorsVisitor)
    }
}
