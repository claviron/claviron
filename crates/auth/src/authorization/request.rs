use super::AssignmentKey;

/// Atomically replace a complete assignment set; an empty set clears it.
/// Every reference must be nonempty and unique. Do not silently omit hidden
/// assignments from a replaceable set: expose the complete editable set or
/// reject replacement. The provider checks every target and prevents privilege
/// escalation or changes to shared roles outside its management authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaceAssignments {
    pub key: AssignmentKey,
    pub revision: String,
    pub members: Vec<String>,
}
