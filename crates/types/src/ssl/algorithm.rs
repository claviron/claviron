use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// ACME Algorithm enum (matching acme::config::AcmeAlgorithm)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")] // serialize as "RS256"/"ES256"/"EdDSA"
pub enum Algorithm {
    RS256,
    ES256,
    ES384,
    EdDSA,
}

impl fmt::Display for Algorithm {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Algorithm::RS256 => f.write_str("RS256"),
            Algorithm::ES256 => f.write_str("ES256"),
            Algorithm::ES384 => f.write_str("ES384"),
            Algorithm::EdDSA => f.write_str("EdDSA"),
        }
    }
}

impl FromStr for Algorithm {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "RS256" => Ok(Algorithm::RS256),
            "ES256" => Ok(Algorithm::ES256),
            "ES384" => Ok(Algorithm::ES384),
            "EDDSA" => Ok(Algorithm::EdDSA),
            _ => Err("invalid algorithm"),
        }
    }
}
