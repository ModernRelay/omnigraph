//! Fault-injection hooks for the cluster apply protocol, mirroring the
//! engine's `omnigraph::failpoints` pattern. With the `failpoints` feature
//! off, every call site compiles to `Ok(())`.
//!
//! Only `maybe_fail` lives here — it returns the cluster's [`Diagnostic`]
//! error type. The test-side configuration guard is shared: use
//! [`omnigraph::failpoints::ScopedFailPoint`], which is registry-only
//! (error-type agnostic) and reachable because the cluster's `failpoints`
//! feature enables `omnigraph/failpoints`. One `ScopedFailPoint`, in the
//! lowest crate, avoids a drifting duplicate.

use crate::Diagnostic;

pub(crate) fn maybe_fail(_name: &str) -> Result<(), Diagnostic> {
    #[cfg(feature = "failpoints")]
    {
        let name = _name;
        fail::fail_point!(name, |_| {
            return Err(Diagnostic::error(
                "injected_failpoint",
                name,
                format!("injected failpoint triggered: {name}"),
            ));
        });
    }
    Ok(())
}

/// Compile-checked catalog of this crate's apply-protocol failpoint names.
/// Engine-scoped names referenced from cluster tests live in
/// [`omnigraph::failpoints::names`].
pub mod names {
    pub const CLUSTER_APPLY_AFTER_GRAPH_CREATE: &str = "cluster_apply.after_graph_create";
    pub const CLUSTER_APPLY_AFTER_GRAPH_DELETE: &str = "cluster_apply.after_graph_delete";
    pub const CLUSTER_APPLY_AFTER_PAYLOAD_PHASE: &str = "cluster_apply.after_payload_phase";
    pub const CLUSTER_APPLY_AFTER_SCHEMA_APPLY: &str = "cluster_apply.after_schema_apply";
    pub const CLUSTER_APPLY_BEFORE_GRAPH_CREATE: &str = "cluster_apply.before_graph_create";
    pub const CLUSTER_APPLY_BEFORE_GRAPH_DELETE: &str = "cluster_apply.before_graph_delete";
    pub const CLUSTER_APPLY_BEFORE_SCHEMA_APPLY: &str = "cluster_apply.before_schema_apply";
    pub const CLUSTER_APPLY_BEFORE_STATE_WRITE: &str = "cluster_apply.before_state_write";
}
