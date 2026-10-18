//! Resolve live process membership without retaining a bootstrap count.

use std::collections::BTreeSet;

use claviron_runtime::AppState;

use super::{FleetMember, FleetMemberRole, FleetMembership};

/// Read the process owner's current, validated membership. This is reporting
/// membership, not a claim that every member is healthy or accepting work.
/// Supervised processes require an owner; unavailable observations never fall
/// back to the count captured when this process was spawned.
pub fn current_members(state: &AppState) -> Result<Vec<FleetMember>, String> {
    if state.is_standalone() {
        return Ok(vec![FleetMember {
            node_id: 0,
            role: FleetMemberRole::Standalone,
        }]);
    }
    let source = state
        .registry_ref()
        .resolve_dyn::<dyn FleetMembership>()
        .ok_or_else(|| "fleet membership provider unavailable".to_owned())?;
    let members = source.members(state)?;
    let mut nodes = BTreeSet::new();
    if members.is_empty()
        || members.iter().any(|member| {
            member.node_id > u16::from(state.worker_capacity())
                || !nodes.insert(member.node_id)
                || (member.node_id == 0
                    && member.role != FleetMemberRole::Master)
                || (member.node_id != 0
                    && member.role != FleetMemberRole::Worker)
        })
        || !nodes.contains(&0)
        || !nodes.contains(&state.fleet_node_id().raw())
    {
        return Err("invalid fleet membership: roles, duplicate/out-of-range nodes or missing current process".into());
    }
    Ok(members)
}

#[cfg(test)]
#[path = "../../tests/service/membership.rs"]
mod tests;
