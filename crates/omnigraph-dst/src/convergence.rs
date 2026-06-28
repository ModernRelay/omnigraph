//! Convergence invariants for multi-writer (`Cohort`) scenarios — the registry
//! twin of `invariants::run_battery`, for properties that only exist ACROSS
//! independent writers: a linear lineage chain (no CAS fork under concurrent
//! disjoint writers), every commit landing (no lost write), and all
//! coordinators converging on the same committed head after quiesce.
//!
//! White-box (reads `Omnigraph::list_commits`), so the embedded tier. The CLI
//! tier asserts the same properties via `omnigraph commit list --json`
//! (black-box) — added with the cross-process test that needs it. Returns the
//! same `Finding` type as the single-writer battery, so a multi-writer test
//! reports findings in one vocabulary.

use std::collections::HashSet;

use omnigraph::db::{GraphCommit, Omnigraph};

use crate::backend::Embedded;
use crate::cohort::Cohort;
use crate::invariants::Finding;

/// The head (tip) commit id on a branch = the commit with the max manifest
/// version. The lineage is append-only and a linear chain has a unique tip; a
/// fork is caught separately by [`lineage_is_linear_chain`].
fn tip_id(commits: &[GraphCommit]) -> Option<String> {
    commits
        .iter()
        .max_by_key(|c| c.manifest_version)
        .map(|c| c.graph_commit_id.clone())
}

/// No CAS fork: concurrent disjoint writers on one branch must serialize into a
/// LINEAR chain. A fork shows as two commits sharing one `parent_commit_id`; a
/// merge parent means a real merge DAG, which a disjoint-write race must never
/// produce. (The §7.1 `graph_head:<branch>` contention row is what forces this:
/// one writer wins the CAS, the other retries and re-parents.)
pub async fn lineage_is_linear_chain(db: &Omnigraph, branch: &str) -> Result<(), Finding> {
    let commits = db.list_commits(Some(branch)).await.map_err(Finding::Engine)?;
    let mut parents: HashSet<String> = HashSet::new();
    for c in &commits {
        if c.merged_parent_commit_id.is_some() {
            return Err(Finding::Logical(format!(
                "non-linear lineage on {branch}: commit {} carries a merge parent",
                c.graph_commit_id
            )));
        }
        if let Some(p) = &c.parent_commit_id {
            if !parents.insert(p.clone()) {
                return Err(Finding::Logical(format!(
                    "forked lineage on {branch}: parent {p} has >1 child (a CAS fork)"
                )));
            }
        }
    }
    Ok(())
}

/// The chain has exactly `expected` commits — every concurrent writer's commit
/// landed, no lost or duplicated write.
pub async fn chain_len_is(db: &Omnigraph, branch: &str, expected: usize) -> Result<(), Finding> {
    let n = db
        .list_commits(Some(branch))
        .await
        .map_err(Finding::Engine)?
        .len();
    if n != expected {
        return Err(Finding::Logical(format!(
            "lineage on {branch} has {n} commits, expected {expected} (lost or extra write)"
        )));
    }
    Ok(())
}

/// Every writer's coordinator converges on the same committed head after
/// quiesce: same chain length AND same tip commit id. A handle whose warm state
/// failed to observe a foreign commit (a read that did not re-resolve latest)
/// diverges here.
pub async fn all_writers_converge(cohort: &Cohort<Embedded>, branch: &str) -> Result<(), Finding> {
    let mut reference: Option<(usize, Option<String>)> = None;
    for (i, w) in cohort.writers().iter().enumerate() {
        let commits = w.db().list_commits(Some(branch)).await.map_err(Finding::Engine)?;
        let observed = (commits.len(), tip_id(&commits));
        match &reference {
            None => reference = Some(observed),
            Some(r) => {
                if *r != observed {
                    return Err(Finding::Logical(format!(
                        "writer {i} sees {observed:?} on {branch}, writer 0 saw {r:?} — coordinators diverge"
                    )));
                }
            }
        }
    }
    Ok(())
}

/// The convergence battery as a registry (the [`crate::invariants::run_battery`]
/// twin): `(name, result)` per property. Adding a convergence property later is
/// one line here; the multi-writer test iterates and classifies.
pub async fn run_convergence_battery(
    cohort: &Cohort<Embedded>,
    branch: &str,
    expected_chain_len: usize,
) -> Vec<(&'static str, Result<(), Finding>)> {
    let lead = cohort.writer(0).db();
    vec![
        ("lineage-linear-chain", lineage_is_linear_chain(lead, branch).await),
        ("chain-len", chain_len_is(lead, branch, expected_chain_len).await),
        ("all-writers-converge", all_writers_converge(cohort, branch).await),
    ]
}
