//! Errors of the application Link, over the generic `orbitive::link` ones.

use super::LINK_INBOX_PAYLOAD_CAPACITY;

/// Application link errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("the receiver work capacity is full")]
    CapacityFull,
    #[error("the selected receiver left")]
    ReceiverGone,
    #[error("the work reservation is no longer valid")]
    StaleWork,
    #[error("the target's inbox lane is full")]
    InboxFull,
    #[error("this node holds no free stream slot")]
    LaneFull,
    #[error("no live consumer serves application '{0}'")]
    NoConsumer(String),
    #[error("every lane in the destination link fleet is taken")]
    NoLane,
    #[error("every directory watcher slot in the link fleet is taken")]
    NoWatcher,
    #[error(
        "exchange frame does not fit in {LINK_INBOX_PAYLOAD_CAPACITY} bytes"
    )]
    FrameTooLarge,
    #[error("exchange frame is malformed: {0}")]
    Malformed(&'static str),
    #[error(
        "Link max_in_flight {max_in_flight} must be positive and below inbox_capacity \
         {inbox_capacity}: the inbox also carries control frames"
    )]
    AdmissionExceedsInbox { max_in_flight: usize, inbox_capacity: usize },
    #[error("invalid Orbit name '{application}': {reason}")]
    InvalidName { application: String, reason: String },
    #[error(
        "'{application}' is already served by {held_by} (lane {lane}, pid {pid}); a {runtime} \
         process cannot join it under the same name"
    )]
    RuntimeConflict {
        application: String,
        runtime: String,
        held_by: String,
        lane: usize,
        pid: u32,
    },
    #[error(transparent)]
    Stream(#[from] orbitive::stream::Error),
    #[error(transparent)]
    Orbit(#[from] orbitive::core::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn map_link_error(error: orbitive::link::Error) -> Error {
    match error {
        orbitive::link::Error::CapacityFull => Error::CapacityFull,
        orbitive::link::Error::NoConsumer => Error::ReceiverGone,
        orbitive::link::Error::StaleWork => Error::StaleWork,
        orbitive::link::Error::InboxFull => Error::InboxFull,
        orbitive::link::Error::NoLane => Error::NoLane,
        orbitive::link::Error::NoWatcher => Error::NoWatcher,
        orbitive::link::Error::FrameTooLarge => Error::FrameTooLarge,
        orbitive::link::Error::Malformed(message) => Error::Malformed(message),
        orbitive::link::Error::InvalidName { name, reason } => {
            Error::InvalidName { application: name, reason }
        }
        orbitive::link::Error::RoleConflict {
            name,
            role,
            held_by,
            lane,
            pid,
        } => Error::RuntimeConflict {
            application: name,
            runtime: role,
            held_by,
            lane,
            pid,
        },
        orbitive::link::Error::Stream(error) => Error::Stream(error),
        orbitive::link::Error::Core(error) => Error::Orbit(error),
        orbitive::link::Error::Io(error) => {
            Error::Orbit(orbitive::core::Error::Io(error))
        }
        _ => Error::Malformed("unrecognized Orbit link error"),
    }
}
