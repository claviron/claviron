use std::borrow::Cow;

pub trait KeyString {
    /// Borrow if possible, allocate only when needed.
    fn key_str(&self) -> Cow<'_, str>;

    /// Convenience: always-owned String.
    #[inline]
    fn key_string(&self) -> String {
        self.key_str().into_owned()
    }
}

impl<T: KeyString + ?Sized> KeyString for &T {
    #[inline]
    fn key_str(&self) -> Cow<'_, str> {
        (*self).key_str()
    }
}
