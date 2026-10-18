use serde::{Deserialize, Deserializer, Serializer};
use time::OffsetDateTime;

/// Serialize OffsetDateTime as ISO 8601 string for JavaScript compatibility
pub mod iso8601 {
    use time::format_description::well_known::Rfc3339;

    use super::*;

    pub fn serialize<S>(
        datetime: &OffsetDateTime,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = datetime.format(&Rfc3339).map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(
        deserializer: D
    ) -> Result<OffsetDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        OffsetDateTime::parse(&s, &Rfc3339).map_err(serde::de::Error::custom)
    }
}

/// Serialize `Option<OffsetDateTime>` as an ISO 8601 string or null.
pub mod iso8601_option {
    use time::format_description::well_known::Rfc3339;

    use super::*;

    pub fn serialize<S>(
        datetime: &Option<OffsetDateTime>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match datetime {
            Some(dt) => {
                let s =
                    dt.format(&Rfc3339).map_err(serde::ser::Error::custom)?;
                serializer.serialize_str(&s)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(
        deserializer: D
    ) -> Result<Option<OffsetDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt_s: Option<String> = Option::deserialize(deserializer)?;
        match opt_s {
            Some(s) => {
                let dt = OffsetDateTime::parse(&s, &Rfc3339)
                    .map_err(serde::de::Error::custom)?;
                Ok(Some(dt))
            }
            None => Ok(None),
        }
    }
}
