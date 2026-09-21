use thiserror::Error;

/// A planning failure. For a change-feed or merge operation the gate turns
/// it into an executor-route decision; a read query has no executor behind
/// it, so the engine returns it to the caller as a failed query.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PlanError {
    /// Resolution could not bind a name or a side against the plan source.
    #[error("the plan source could not resolve: {detail}")]
    Unresolved { detail: String },
    /// A pass met a plan it has no rule for. A registered shape never reaches
    /// this arm; the registry test pins that.
    #[error("planner internal error: {0}")]
    Internal(String),
}
