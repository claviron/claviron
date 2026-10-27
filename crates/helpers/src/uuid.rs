use serde::{Deserializer, Serializer};
use uuid::Uuid;

pub fn serialize<S>(
    uuid: &Uuid,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // Serialize as 16-byte binary (MessagePack bin format)
    serializer.serialize_bytes(uuid.as_bytes())
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
where
    D: Deserializer<'de>,
{
    // Deserialize from MessagePack bin format or array format
    struct UuidBytesVisitor;

    impl<'de> serde::de::Visitor<'de> for UuidBytesVisitor {
        type Value = Uuid;

        fn expecting(
            &self,
            formatter: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            formatter.write_str("a 16-byte UUID as binary or array")
        }

        fn visit_bytes<E>(
            self,
            v: &[u8],
        ) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if v.len() != 16 {
                return Err(E::custom(format!(
                    "Invalid UUID byte length: expected 16, got {}",
                    v.len()
                )));
            }
            let uuid_bytes: [u8; 16] = v
                .try_into()
                .map_err(|_| E::custom("Failed to convert bytes to UUID"))?;
            Ok(Uuid::from_bytes(uuid_bytes))
        }

        fn visit_byte_buf<E>(
            self,
            v: Vec<u8>,
        ) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.visit_bytes(&v)
        }

        fn visit_seq<A>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut bytes = [0u8; 16];
            for (index, byte) in bytes.iter_mut().enumerate() {
                *byte = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::invalid_length(index, &self)
                })?;
            }
            if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::invalid_length(17, &self));
            }
            Ok(Uuid::from_bytes(bytes))
        }
    }

    deserializer.deserialize_bytes(UuidBytesVisitor)
}

#[cfg(test)]
#[path = "../tests/uuid.rs"]
mod tests;
