//! Owns the TCP or Unix transport target for an origin.

use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConnectTarget {
    Tcp(SocketAddr),
    Unix(PathBuf),
    Tls(SocketAddr),
}
