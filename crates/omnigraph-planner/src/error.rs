use omnigraph_compiler::QueryDiagnostic;
use omnigraph_compiler::query::diagnostic::QueryCode;
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
    /// error, answered as a bad request carrying its diagnostic, never as a
    /// planner defect.
    #[error("{0}")]
    Unsupported(Box<QueryDiagnostic>),
    /// A pass met a plan it has no rule for. A registered shape never reaches
    /// this arm; the registry test pins that.
    #[error("planner internal error: {0}")]
    Internal(String),
    /// The plan source failed while answering a lookup (opening a pinned
    /// table, reading its index metadata): the source's own error class is
    /// the caller's answer, never a planner defect.
    #[error("the plan source failed: {detail}")]
    Source { detail: String },
}

impl PlanError {
    /// A refusal by design: the planner code, what was refused, and the one
    /// fix that answers it, or `None` when the message names the decision.
    pub fn refused(code: QueryCode, message: impl Into<String>, fix: Option<&str>) -> Self {
        let diagnostic = QueryDiagnostic::plan(code, message);
        Self::Unsupported(Box::new(match fix {
            Some(fix) => diagnostic.with_fix(fix),
            None => diagnostic,
        }))
    }
}

/// The fix of a traversal that needs a work limit (`P002`).
pub(crate) const SET_TRAVERSAL_WORK_LIMIT: &str = "set `traversal_work_limit` before the query, for example `set traversal_work_limit = 1000000;`";
