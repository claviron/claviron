//! Native integration primitives for Laravel applications hosted by Claviron.
//!
//! Authentication, broadcasting, PHP configuration evaluation and console
//! execution each have their own module and registration boundary.

pub mod artisan;
pub mod auth;
pub mod broadcast;
pub mod congif;
