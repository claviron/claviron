//! Owns the file-to-compiled transition and validation context.

use std::collections::BTreeMap;

use crate::app::CompiledUpstreamApp;
use crate::compile::profile::Profile;
#[derive(Clone, Debug)]
pub struct CompiledUpstreams {
    pub profiles: BTreeMap<String, std::sync::Arc<Profile>>,
    pub apps: BTreeMap<String, CompiledUpstreamApp>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Validation {
    pub healthy_worker_count: Option<usize>,
}

mod app;
pub(crate) mod forward;
pub(crate) mod l7;
pub(crate) mod origin;
pub(crate) mod profile;
pub(crate) mod retry;
pub(crate) mod tls;
