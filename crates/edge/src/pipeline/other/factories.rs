#[cfg(feature = "upstream")]
pub(crate) mod upstream;

#[cfg(any(feature = "php_fcgi", feature = "php_embed"))]
pub(crate) mod php;
