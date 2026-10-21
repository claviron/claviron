use orbitive::link::{GroupAccess, LinkSegment, LinkSpec, Result};

/// Access to one Link, not to the host's Fleet or its other SHM resources.
/// Group writers are trusted peers; service/role names are not authentication.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Access {
    #[default]
    OwnerOnly,
    Group(GroupAccess),
}

impl Access {
    pub(crate) fn open(
        &self,
        fleet: &str,
        spec: LinkSpec,
    ) -> Result<LinkSegment> {
        match self {
            Self::OwnerOnly => LinkSegment::open(fleet, spec),
            Self::Group(access) => {
                LinkSegment::open_group(fleet, spec, access.clone())
            }
        }
    }

    /// Inspect an exact resource without creating directories, locks or SHM.
    pub(crate) fn exists(
        &self,
        fleet: &str,
        spec: LinkSpec,
    ) -> Result<bool> {
        use orbitive::shm::{ShmAccessPolicy, ShmRegion, ShmValidation};
        let validation = match self {
            Self::OwnerOnly => ShmRegion::validate_existing(
                &orbitive::shm::ring_segment_name(fleet, spec.inbox_kind),
                spec.inbox_segment_bytes(),
            ),
            Self::Group(access) => ShmRegion::validate_existing_with_policy(
                &access.segment_name(fleet, spec.inbox_kind),
                spec.inbox_segment_bytes(),
                access.owner_uid(),
                ShmAccessPolicy::GroupReadWrite { gid: access.gid() },
            ),
        }
        .map_err(orbitive::link::Error::Io)?;
        let present = matches!(validation, ShmValidation::Valid { .. });
        if present && let Self::Group(access) = self {
            // Group Links provision both resources before joining. Management
            // must not recreate a missing body table while observing an inbox.
            let stream = ShmRegion::validate_existing_with_policy(
                &access.segment_name(fleet, spec.streams.kind),
                orbitive::stream::segment_size_for(
                    spec.fleet_capacity,
                    spec.streams,
                ),
                access.owner_uid(),
                ShmAccessPolicy::GroupReadWrite { gid: access.gid() },
            )
            .map_err(orbitive::link::Error::Io)?;
            if matches!(stream, ShmValidation::Missing) {
                return Err(orbitive::link::Error::Malformed(
                    "group Link body table is missing",
                ));
            }
        }
        Ok(present)
    }

    pub(crate) fn recover(
        &self,
        fleet: &str,
        spec: LinkSpec,
        lane: usize,
        incarnation: u64,
    ) -> Result<()> {
        use orbitive::stream::{Incarnation, Streams};
        let node = orbitive::NodeId::new(lane as u16);
        let incarnation = Incarnation::new(incarnation);
        match self {
            Self::OwnerOnly => Streams::node_dead_in_shm(
                fleet,
                spec.fleet_capacity,
                spec.streams,
                node,
                incarnation,
            )
            .map(|_| ()),
            Self::Group(access) => {
                if !self.exists(fleet, spec)? {
                    return Ok(());
                }
                Streams::node_dead_in_group(
                    fleet,
                    spec.fleet_capacity,
                    spec.streams,
                    node,
                    incarnation,
                    access.clone(),
                )
            }
        }
        .map_err(orbitive::link::Error::Stream)
    }
}

#[cfg(test)]
#[path = "../../tests/link/access.rs"]
mod tests;
