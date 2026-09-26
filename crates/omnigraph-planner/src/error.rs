use thiserror::Error;

/// A planning failure. For a change-feed or merge operation the gate turns
/// it into an executor-route decision; a read query has no executor behind
/// it, so the engine returns it to the caller as a failed query.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PlanError {
    /// Resolution could not bind a name or a side against the plan source.
    #[error("the plan source could not resolve: {detail}")]
    Unresolved { detail: String },
    /// A well-formed query shape the planner refuses by design; the caller's
    /// error, answered as a bad request, never as a planner defect.
    #[error("{detail}")]
    Unsupported { detail: String },
    /// A pass met a plan it has no rule for. A registered shape never reaches
    /// this arm; the registry test pins that.
    #[error("planner internal error: {0}")]
    Internal(String),
}
