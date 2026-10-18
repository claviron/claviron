pub(crate) mod config;
pub mod fcgi;
pub mod fpm;
mod provider;

pub(crate) use provider::register;
