#[cfg(feature = "axum")]
use headers::{Header, HeaderName, HeaderValue};

#[derive(Debug, Clone)]
pub struct ClavironApplication(pub String);

#[cfg(feature = "axum")]
impl Header for ClavironApplication {
    fn name() -> &'static HeaderName {
        static NAME: HeaderName = HeaderName::from_static("claviron-app");
        &NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
    where
        I: Iterator<Item = &'i HeaderValue>,
    {
        let value = values
            .next()
            .ok_or_else(headers::Error::invalid)?
            .to_str()
            .map_err(|_| headers::Error::invalid())?
            .trim();

        if !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(headers::Error::invalid());
        }

        Ok(ClavironApplication(value.to_string()))
    }

    fn encode<E: Extend<HeaderValue>>(
        &self,
        values: &mut E,
    ) {
        if let Ok(v) = HeaderValue::from_str(&self.0) {
            values.extend(std::iter::once(v));
        }
    }
}

use std::ops::Deref;

impl Deref for ClavironApplication {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0 // assuming .0 is a String
    }
}
