use std::fmt;

use serde::de::{Error, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

/// Placement of a runnable on its caller's executor or its own.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Execution {
    #[default]
    Inherit,
    Dedicated,
}

impl Execution {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inherit => "inherit",
            Self::Dedicated => "dedicated",
        }
    }
}

impl<'de> Deserialize<'de> for Execution {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        struct Placement;

        impl Visitor<'_> for Placement {
            type Value = Execution;

            fn expecting(
                &self,
                f: &mut fmt::Formatter<'_>,
            ) -> fmt::Result {
                f.write_str("inherit or dedicated")
            }

            fn visit_str<E: Error>(
                self,
                value: &str,
            ) -> Result<Execution, E> {
                match value {
                    "inherit" => Ok(Execution::Inherit),
                    "dedicated" => Ok(Execution::Dedicated),
                    "shared" => Err(E::custom(
                        "execution 'shared' has been replaced by 'inherit'",
                    )),
                    _ => Err(E::unknown_variant(
                        value,
                        &["inherit", "dedicated"],
                    )),
                }
            }
        }

        deserializer.deserialize_str(Placement)
    }
}
