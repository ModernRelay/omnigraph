//! Exact per-commit entity-change enumeration.
//!
//! For one first-parent edge `P -> C` this module derives the logical entity
//! changes by an ordered-by-`id` merge of every changed table lifetime's two
//! pinned endpoints. Logical operation is defined only by the two
//! graph-visible states: absent/present classifies inserts and deletes, and a
//! typed structural image comparison classifies updates while suppressing
//! physical no-ops. Blob columns compare payload-free by physical descriptor
//! identity, with an exact payload byte-compare only on a descriptor tie, so
//! compaction cannot surface phantom updates.
//!
//! Emission order within a block is frozen as
//! `(entity kind: nodes first, opaque type identity, id, operation rank)`;
//! the continuation key inside a page token names a position in that order.

use std::collections::BTreeSet;

use lance::Dataset;

use super::candidate_scan::{CandidatePlan, EmitSource};
use super::model::{
    COMMIT_CHANGES_MAX_BYTES, ChangeEntityKind, ChangeFeedScope, ChangeOpKind, EntityEndpoints,
    EntityImage, GraphEntityChange, GraphTypeRef,
};
use super::row_compare::{OrderedRows, RawRow, ScanTargets, rows_equal, user_schema_fingerprint};
use super::token::{cursor_rejected, opaque_type_id};
use super::{changed_table_intervals, parse_table_key};
use crate::db::DatasetEntry;
use crate::db::logical_row_image;
use crate::db::manifest::Snapshot;
use crate::error::{OmniError, Result};
use crate::table_store::TableStore;

/// Validate caller-supplied page limits against the server-owned ceilings.
/// Zero is a malformed request; above-ceiling values are typed resource
/// limits so transports map them distinctly.
pub(crate) fn validate_change_page_limits(max_changes: usize, max_bytes: u64) -> Result<()> {
    if max_changes == 0 {
        return Err(OmniError::manifest(
            "change page limit must be greater than zero",
        ));
    }
    if max_changes > super::model::COMMIT_CHANGES_MAX_CHANGES {
        return Err(OmniError::resource_limit(
            "commit_changes_page_changes",
            super::model::COMMIT_CHANGES_MAX_CHANGES as u64,
            max_changes as u64,
        ));
    }
    if max_bytes == 0 {
        return Err(OmniError::manifest(
            "change page max_bytes must be greater than zero",
        ));
    }
    if max_bytes > COMMIT_CHANGES_MAX_BYTES {
        return Err(OmniError::resource_limit(
            "commit_changes_page_bytes",
            COMMIT_CHANGES_MAX_BYTES,
            max_bytes,
        ));
    }
    Ok(())
}

/// Shared row/byte budget for one bounded page. The feed threads one budget
/// through every commit of a poll; the finite commit diff uses a fresh one.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PageBudget {
    pub(crate) remaining_rows: usize,
    pub(crate) remaining_bytes: u64,
    /// Page-wide, not commit-local. A feed threads one budget through multiple
    /// commit enumerations, and the solo-oversized forward-progress exception
    /// is legal only for the first change of the whole page.
    emitted_changes: usize,
}

impl PageBudget {
    pub(crate) fn new(max_changes: usize, max_bytes: u64) -> Self {
        Self {
            remaining_rows: max_changes,
            remaining_bytes: max_bytes,
            emitted_changes: 0,
        }
    }

    fn has_emitted(&self) -> bool {
        self.emitted_changes != 0
    }

    fn record_emitted(&mut self) {
        self.emitted_changes = self.emitted_changes.saturating_add(1);
    }
}

/// Continuation position of the last emitted change, carried opaquely inside
/// page tokens. The type is named by its PUBLISHED opaque identity — the same
/// key the emission order uses — so token payloads never carry numeric table
/// or incarnation components. `change_index` is internal bookkeeping that
/// keeps a resumed enumeration's positions monotonic across pages; it is
/// never a public field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContinuationKey {
    pub(crate) type_id: String,
    pub(crate) position: super::token::IdPositionV1,
    pub(crate) operation_rank: u8,
    pub(crate) change_index: usize,
}

/// How one `enumerate_commit_changes` call ended.
pub(crate) enum CommitEnumeration {
    /// Every change of this commit (within scope) was emitted.
    Complete,
    /// The budget closed the page after at least one change emitted by this
    /// call; resume from the key.
    Truncated(ContinuationKey),
    /// The budget could not admit this call's first change. The caller ends
    /// the page at the previous block boundary, or reports the typed
    /// resource-limit error when the page is empty.
    Exhausted { required_bytes: u64 },
}

/// Materialize the exact logical image for one row entering the page: the
/// commit-era flat row image with the logical `id` hoisted out and, for
/// edges, `src`/`dst` hoisted into public `{from, to}` endpoints. This is the
/// only place Blob payloads are read for emitted changes.
async fn emitted_image(
    dataset: &Dataset,
    raw: &RawRow,
    kind: ChangeEntityKind,
) -> Result<EntityImage> {
    crate::instrumentation::record_change_image_materialized();
    let mut properties = logical_row_image(dataset, &raw.slice, 0).await?;
    properties.remove("id");
    let endpoints = if kind == ChangeEntityKind::Edge {
        let from = properties
            .remove("src")
            .and_then(|value| value.as_str().map(str::to_string))
            .ok_or_else(|| OmniError::manifest_internal("edge image is missing src"))?;
        let to = properties
            .remove("dst")
            .and_then(|value| value.as_str().map(str::to_string))
            .ok_or_else(|| OmniError::manifest_internal("edge image is missing dst"))?;
        Some(EntityEndpoints { from, to })
    } else {
        None
    };
    Ok(EntityImage {
        properties,
        endpoints,
    })
}

pub(crate) enum Emit {
    Delete(RawRow),
    Insert(RawRow),
    Update { before: RawRow, after: RawRow },
}

impl Emit {
    pub(crate) fn op(&self) -> ChangeOpKind {
        match self {
            Self::Insert(_) => ChangeOpKind::Insert,
            Self::Update { .. } => ChangeOpKind::Update,
            Self::Delete(_) => ChangeOpKind::Delete,
        }
    }
}

/// The next in-scope logical change in the ordered id merge, or `None` when
/// both sides are exhausted. Equal rows and out-of-scope operations are
/// consumed without image or payload work.
pub(crate) async fn next_emit(
    from: &mut OrderedRows,
    to: &mut OrderedRows,
    scope: &ChangeFeedScope,
) -> Result<Option<Emit>> {
    loop {
        let left_id = from.peek().await?.map(|row| row.id.clone());
        let right_id = to.peek().await?.map(|row| row.id.clone());
        let emit = match (left_id, right_id) {
            (None, None) => return Ok(None),
            (Some(_), None) => Emit::Delete(from.pop().await?.expect("peeked row present")),
            (None, Some(_)) => Emit::Insert(to.pop().await?.expect("peeked row present")),
            (Some(left), Some(right)) if left < right => {
                Emit::Delete(from.pop().await?.expect("peeked row present"))
            }
            (Some(left), Some(right)) if left > right => {
                Emit::Insert(to.pop().await?.expect("peeked row present"))
            }
            (Some(_), Some(_)) => {
                let left = from.pop().await?.expect("peeked row present");
                let right = to.pop().await?.expect("peeked row present");
                if rows_equal(from.dataset(), &left, to.dataset(), &right).await? {
                    continue;
                }
                Emit::Update {
                    before: left,
                    after: right,
                }
            }
        };
        if !scope.wants_op(emit.op()) {
            continue;
        }
        return Ok(Some(emit));
    }
}

/// One paired table lifetime that survived the schema gate, with both pinned
/// datasets already open (the same handles the scans consume, so a changed
/// interval costs at most two opens per page).
pub(crate) struct IntervalPlan {
    /// The published opaque type identity: the block ordering key, the
    /// continuation key, and `GraphTypeRef.id`, all one value.
    opaque_id: String,
    kind: ChangeEntityKind,
    type_name: String,
    /// The paired manifest entries (begin/end version, branch, identity).
    pub(crate) from_entry: DatasetEntry,
    pub(crate) to_entry: DatasetEntry,
    pub(crate) from_dataset: Dataset,
    pub(crate) to_dataset: Dataset,
    /// The candidate-pruning decision, computed by `plan_intervals` BEFORE the
    /// final post-open head witness. The adjacent transaction is referenced by
    /// the already-pinned child manifest, and `Some` stores the complete
    /// transaction-touched parent/child fragment plan so emission performs no
    /// later history lookup. `None` means the exact ordered merge.
    pub(crate) candidate_plan: Option<CandidatePlan>,
}

/// Resolve the exceptional bounded digest position to its exact logical ID.
///
/// Normal (<=256-byte) IDs still use one `id > exact` scan. A longer ID begins
/// at its fixed-size prefix and scans only the equal-prefix change range. We do
/// this proof before constructing any response change so a missing or
/// ambiguous digest fails the whole page rather than publishing a guessed
/// continuation. Once the unique row is known, the ordinary ordered merge is
/// reopened after that exact ID.
async fn resolve_digest_position(
    plan: &IntervalPlan,
    scope: &ChangeFeedScope,
    key: &ContinuationKey,
) -> Result<String> {
    debug_assert!(key.position.is_digest());
    let mut left =
        OrderedRows::open(plan.from_dataset.clone(), Some(key.position.scan_after())).await?;
    let mut right =
        OrderedRows::open(plan.to_dataset.clone(), Some(key.position.scan_after())).await?;
    let mut resolved: Option<String> = None;
    while let Some(emit) = next_emit(&mut left, &mut right, scope).await? {
        let (id, operation_rank) = match &emit {
            Emit::Insert(row) | Emit::Delete(row) => (row.id.as_str(), emit.op().rank()),
            Emit::Update { after, .. } => (after.id.as_str(), emit.op().rank()),
        };
        if !key.position.prefix_contains(id) {
            break;
        }
        if !key.position.matches_digest(id) {
            continue;
        }
        if operation_rank != key.operation_rank {
            return Err(cursor_rejected(
                "change continuation digest names the wrong operation",
            ));
        }
        if resolved.is_some() {
            return Err(cursor_rejected(
                "change continuation digest is ambiguous within its prefix range",
            ));
        }
        resolved = Some(id.to_string());
    }
    resolved.ok_or_else(|| {
        cursor_rejected("change continuation digest no longer names a change in this commit")
    })
}

fn schema_boundary(graph_commit_id: &str, table_key: &str) -> OmniError {
    let (_, type_name) = parse_table_key(table_key);
    OmniError::ChangeSchemaBoundary {
        graph_commit_id: graph_commit_id.to_string(),
        type_name: type_name.to_string(),
    }
}

/// Prove the P→C pair compatible for entity diff and open every surviving
/// paired lifetime, in the frozen `(kind rank, published type id)` order —
/// the same opaque identity the emitted changes and continuation keys carry,
/// so a caller can reproduce the order from the response alone.
///
/// The gate runs over ALL changed intervals before anything is emitted, so a
/// schema boundary refuses the whole commit deterministically on every page.
/// Non-empty added or removed lifetimes are schema evolution with data
/// present — refused, never synthesized into entity inserts/deletes; empty
/// ones emit nothing. The gate deliberately ignores the request scope: a
/// boundary is a property of the commit pair, not of one filtered view.
async fn plan_intervals(
    store: &TableStore,
    parent: &Snapshot,
    child: &Snapshot,
    schema_identity_domain: &str,
    graph_commit_id: &str,
    scope: &ChangeFeedScope,
) -> Result<Vec<IntervalPlan>> {
    let intervals = changed_table_intervals(parent, child);

    // The manifest snapshots are captured and re-proven, but the per-table
    // datasets are opened next by (branch path, numeric version). A branch
    // delete/recreate in this window would retarget those opens. Two witnesses
    // close it: `open_at_entry_verified`'s per-table e_tag comparison
    // (defense-in-depth; unavailable on an e_tag-less store) and the logical
    // post-open `reprove_named_branch_heads` below (load-bearing on every
    // store). Tests park here to exercise the window.
    crate::failpoints::maybe_fail(crate::failpoints::names::CHANGE_FEED_PRE_TABLE_OPEN)?;

    let mut plans = Vec::with_capacity(intervals.len());
    let mut parent_branches: BTreeSet<String> = BTreeSet::new();
    let mut child_branches: BTreeSet<String> = BTreeSet::new();
    for interval in intervals {
        let table_key = interval.type_key();
        match (interval.from, interval.to) {
            (None, Some(added)) => {
                if added.entity_count > 0 {
                    return Err(schema_boundary(graph_commit_id, table_key));
                }
            }
            (Some(removed), None) => {
                if removed.entity_count > 0 {
                    return Err(schema_boundary(graph_commit_id, table_key));
                }
            }
            (Some(from), Some(to)) => {
                let from_dataset = store.open_at_entry_verified(from).await?;
                let to_dataset = store.open_at_entry_verified(to).await?;
                if let Some(branch) = from.native_dataset_branch.as_deref() {
                    parent_branches.insert(branch.to_string());
                }
                if let Some(branch) = to.native_dataset_branch.as_deref() {
                    child_branches.insert(branch.to_string());
                }
                if user_schema_fingerprint(&from_dataset) != user_schema_fingerprint(&to_dataset) {
                    return Err(schema_boundary(graph_commit_id, table_key));
                }
                let (kind, type_name) = parse_table_key(table_key);
                let kind: ChangeEntityKind = kind.into();
                // Classify the interval HERE — before the head witness below —
                // and retain the complete physical plan. The one transaction is
                // referenced by the already-pinned child manifest; no history
                // lookup is permitted later during emission. Scope-filtered
                // intervals are never emitted, so their stored decision is
                // irrelevant.
                let candidate_plan = if scope.wants_kind(kind) && scope.wants_type_name(type_name) {
                    super::candidate_scan::interval_candidate_plan(
                        from,
                        to,
                        &from_dataset,
                        &to_dataset,
                    )
                    .await?
                } else {
                    None
                };
                plans.push(IntervalPlan {
                    opaque_id: opaque_type_id(schema_identity_domain, interval.identity),
                    kind,
                    type_name: type_name.to_string(),
                    from_entry: from.clone(),
                    to_entry: to.clone(),
                    from_dataset,
                    to_dataset,
                    candidate_plan,
                });
            }
            (None, None) => unreachable!("changed intervals have at least one endpoint"),
        }
    }
    // The load-bearing second-window witness. The per-table opens above happen
    // AFTER the commit's manifest-head proof and are keyed only by (branch
    // path, numeric version), so a named branch deleted and recreated in that
    // window retargets them to the replacement branch's rows. Re-prove each
    // opened named branch LOGICALLY after all opens: a fresh, cache-bypassing
    // manifest snapshot at the same pinned version must still report the same
    // `graph_head` this enumeration's snapshot captured. A recreated fork's
    // manifest at that version carries different lineage commit ids (or lacks
    // the version entirely), so this fails closed on every store — including
    // one that persists no table e_tags; the per-open e_tag comparison is
    // defense-in-depth, not the witness. Ordering makes this sound: a
    // recreation before any table open is detected here, and a recreation
    // after this proof cannot have affected the already-opened handles. Main
    // cannot undergo branch-name ABA and pays no extra manifest resolution.
    reprove_named_branch_heads(store, parent, &parent_branches).await?;
    reprove_named_branch_heads(store, child, &child_branches).await?;
    // Nothing after this witness may read the branch's numeric-path history
    // live: version manifests sit at replaceable numeric paths (unlike
    // UUID-named data and transaction files), so a later live read would see a
    // recreated branch's history under this commit's label. Tests park here
    // and recreate the branch to pin that contract.
    crate::failpoints::maybe_fail(crate::failpoints::names::CHANGE_FEED_POST_HEAD_WITNESS)?;
    // The opaque ids are domain-scoped SHA-256 projections of distinct
    // immutable identities, so this order is total and deterministic.
    plans.sort_by(|left, right| {
        left.kind
            .rank()
            .cmp(&right.kind.rank())
            .then_with(|| left.opaque_id.cmp(&right.opaque_id))
    });
    Ok(plans)
}

/// Fail closed unless every named branch whose tables this enumeration just
/// opened still resolves — via a fresh manifest open at the snapshot's pinned
/// version — to the same `graph_head` the captured snapshot carries. See the
/// call site in [`plan_intervals`] for the window this closes.
async fn reprove_named_branch_heads(
    store: &TableStore,
    snapshot: &Snapshot,
    branches: &BTreeSet<String>,
) -> Result<()> {
    for branch in branches {
        let fresh = crate::db::manifest::ManifestCoordinator::snapshot_at(
            store.root_uri(),
            Some(branch),
            snapshot.graph_manifest_version(),
        )
        .await?;
        if fresh.graph_head(Some(branch)) != snapshot.graph_head(Some(branch)) {
            return Err(OmniError::manifest(format!(
                "change feed branch '{branch}' has no persisted native-branch \
                 incarnation witness after the per-table opens; the branch was \
                 deleted and recreated during the poll"
            )));
        }
    }
    Ok(())
}

/// Enumerate one commit's entity changes into `out`, charging `budget` per
/// emitted change (serialized size of the complete change, both images
/// included). Resumes deterministically from a continuation key over the same
/// two immutable snapshots.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn enumerate_commit_changes(
    store: &TableStore,
    parent: &Snapshot,
    child: &Snapshot,
    schema_identity_domain: &str,
    graph_commit_id: &str,
    scope: &ChangeFeedScope,
    resume: Option<&ContinuationKey>,
    budget: &mut PageBudget,
    out: &mut Vec<GraphEntityChange>,
) -> Result<CommitEnumeration> {
    let plans = plan_intervals(
        store,
        parent,
        child,
        schema_identity_domain,
        graph_commit_id,
        scope,
    )
    .await?;

    let mut resume_seen = resume.is_none();
    // `change_index` is client-controlled (it round-trips through the page
    // token, whose checksum is integrity not authenticity) and is pure
    // monotonic bookkeeping — the authoritative resume position is `(type_id,
    // id)`. Saturate so a crafted `usize::MAX` cannot panic under overflow
    // checks or wrap in release; a bounded wrong counter has no observable effect.
    let mut next_change_index = resume.map_or(0, |key| key.change_index.saturating_add(1));
    let mut last_emitted: Option<ContinuationKey> = None;
    let mut emitted_this_call = false;

    for plan in plans {
        // Skip lifetimes wholly outside the scope; the schema gate above
        // already covered them.
        if !scope.wants_kind(plan.kind) || !scope.wants_type_name(&plan.type_name) {
            continue;
        }
        let after_id = match resume {
            Some(key) if !resume_seen => {
                if plan.opaque_id == key.type_id {
                    let exact = if key.position.is_digest() {
                        resolve_digest_position(&plan, scope, key).await?
                    } else {
                        key.position.scan_after().to_string()
                    };
                    resume_seen = true;
                    Some(exact)
                } else {
                    // Sorted before the continuation position: consumed by an
                    // earlier page.
                    continue;
                }
            }
            _ => None,
        };

        let type_ref = GraphTypeRef {
            id: plan.opaque_id.clone(),
            name: plan.type_name.clone(),
        };
        // Per-interval emitter: the touched-fragment candidate path when the
        // commit's effect is a proven adjacent row-set-preserving shape, else
        // the exact full ordered merge. Both yield the same id-ordered `Emit`
        // stream, so the budgeting/continuation loop below is identical.
        // Before-images come from the parent handle, after-images from the
        // child handle. The pruning decision itself was made by
        // `plan_intervals` before, and covered by, the final head witness — no
        // live history read happens here.
        let mut source = EmitSource::plan(
            &plan.from_entry,
            &plan.to_entry,
            plan.from_dataset,
            plan.to_dataset,
            plan.candidate_plan,
            after_id.as_deref(),
            scope,
            ScanTargets::for_page(budget.remaining_rows, budget.remaining_bytes),
        )
        .await?;

        while let Some(emit) = source.next().await? {
            // The source yields one look-ahead change so we can distinguish a
            // complete block from a truncated one. If the page's row budget (or
            // an already-used byte budget) is exhausted, that sentinel must not
            // materialize JSON or Blob payloads merely to prove continuation.
            if budget.remaining_rows == 0 || (budget.remaining_bytes == 0 && budget.has_emitted()) {
                return Ok(match last_emitted {
                    Some(key) if emitted_this_call => CommitEnumeration::Truncated(key),
                    // A feed can carry an exhausted page-wide budget into the
                    // next commit. Its caller ends at the previous block
                    // boundary; no image-size value is needed in that case.
                    _ => CommitEnumeration::Exhausted { required_bytes: 0 },
                });
            }
            let op = emit.op();
            let (id, before, after) = match emit {
                Emit::Insert(raw) => {
                    let image = emitted_image(source.child_dataset(), &raw, plan.kind).await?;
                    (raw.id, None, Some(image))
                }
                Emit::Delete(raw) => {
                    let image = emitted_image(source.parent_dataset(), &raw, plan.kind).await?;
                    (raw.id, Some(image), None)
                }
                Emit::Update { before, after } => {
                    let before_image =
                        emitted_image(source.parent_dataset(), &before, plan.kind).await?;
                    let after_image =
                        emitted_image(source.child_dataset(), &after, plan.kind).await?;
                    (after.id, Some(before_image), Some(after_image))
                }
            };
            let change = GraphEntityChange {
                kind: plan.kind,
                entity_type: type_ref.clone(),
                id,
                op,
                before,
                after,
            };
            let encoded_bytes = u64::try_from(
                serde_json::to_vec(&change)
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?
                    .len(),
            )
            .map_err(|_| OmniError::manifest_internal("change image size exceeds u64"))?;

            // Forward progress: the byte budget is a packing target, never a
            // wall. A change that exceeds the remaining bytes ends the page at
            // the previous boundary ONLY if the page already carries a change;
            // if it is the FIRST change of the page it is emitted SOLO (even
            // over budget), so a legal committed change — whose two images can
            // exceed the write-path-derived ceiling once managed Blobs inline as
            // base64 — is always deliverable, one per page if needed. The row
            // budget still bounds packing. `Exhausted` is retained for a feed
            // carrying a page-wide budget already consumed by a prior block;
            // a standalone request's validated budget starts nonzero.
            let over_bytes = encoded_bytes > budget.remaining_bytes;
            if over_bytes && budget.has_emitted() {
                return Ok(match last_emitted {
                    Some(key) if emitted_this_call => CommitEnumeration::Truncated(key),
                    _ => CommitEnumeration::Exhausted {
                        required_bytes: encoded_bytes,
                    },
                });
            }
            budget.remaining_rows -= 1;
            // Saturating: a solo over-budget change drives remaining_bytes to 0,
            // so the next change (if any) ends the page here rather than
            // overflowing it further.
            budget.remaining_bytes = budget.remaining_bytes.saturating_sub(encoded_bytes);
            budget.record_emitted();
            last_emitted = Some(ContinuationKey {
                type_id: plan.opaque_id.clone(),
                position: super::token::IdPositionV1::for_id(&change.id),
                operation_rank: op.rank(),
                change_index: next_change_index,
            });
            emitted_this_call = true;
            next_change_index = next_change_index.saturating_add(1);
            out.push(change);
        }
    }

    if !resume_seen {
        return Err(cursor_rejected(
            "change continuation no longer names a changed graph type in this commit",
        ));
    }
    Ok(CommitEnumeration::Complete)
}
