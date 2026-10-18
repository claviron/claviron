//! Immutable virtual-node ring, built once; each origin is yielded at most once.
use super::key::{self, Key};
use crate::config::error::ConfigError;
use crate::config::profile::balance::BalanceConfig;
use crate::origin::Origin;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Ring {
    pub(super) key: Key,
    points: Box<[Box<[u64]>]>,
}
impl Ring {
    pub(super) fn compile(
        name: &str,
        config: &BalanceConfig,
        origins: &[Origin],
    ) -> Result<Self, ConfigError> {
        let invalid = |field, detail| ConfigError::InvalidBalance {
            profile: name.into(),
            field,
            detail,
        };
        let key =
            config.key.as_deref().and_then(Key::compile).ok_or_else(|| {
                invalid(
                    "key",
                    "expected header:<name>, cookie:<name>, path or remote_ip",
                )
            })?;
        if config.replicas == 0 {
            return Err(invalid("replicas", "must be greater than zero"));
        }
        let mut labels = std::collections::HashSet::new();
        if origins.iter().any(|origin| !labels.insert(&origin.label)) {
            return Err(invalid(
                "origins",
                "consistent_hash requires unique origin labels",
            ));
        }
        let mut points = Vec::new();
        points
            .try_reserve_exact(origins.len())
            .map_err(|_| invalid("replicas", "ring cannot be allocated"))?;
        for origin in origins {
            let mut nodes = Vec::new();
            nodes
                .try_reserve_exact(config.replicas)
                .map_err(|_| invalid("replicas", "ring cannot be allocated"))?;
            let base = key::extend(key::hash(origin.label.as_bytes()), &[0]);
            for replica in 0..config.replicas {
                nodes.push(key::finish(key::extend(
                    base,
                    &(replica as u64).to_le_bytes(),
                )));
            }
            nodes.sort_unstable();
            points.push(nodes.into_boxed_slice());
        }
        Ok(Self { key, points: points.into_boxed_slice() })
    }
    pub(super) fn distance(
        &self,
        origin: usize,
        hash: u64,
    ) -> u64 {
        let nodes = &self.points[origin];
        let next = nodes.partition_point(|point| *point < hash);
        nodes[if next == nodes.len() { 0 } else { next }].wrapping_sub(hash)
    }
}
