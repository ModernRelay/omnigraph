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

use lance::Dataset;

use super::model::{
    COMMIT_CHANGES_MAX_BYTES, ChangeEntityKind, ChangeFeedScope, ChangeOpKind, EntityEndpoints,
    EntityImage, GraphEntityChange, GraphTypeRef,
};
use super::row_compare::{
    BlobComparisonScope, OrderedRows, RawRow, rows_equal, user_schema_fingerprint,
};
use super::token::{cursor_rejected, opaque_type_id};
use super::{changed_table_intervals, parse_table_key};
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
    if max_changes > super::model::COMMIT_CHANGES_MAX_ROWS {
        return Err(OmniError::resource_limit(
            "commit_changes_page_rows",
            super::model::COMMIT_CHANGES_MAX_ROWS as u64,
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
}

impl PageBudget {
    pub(crate) fn new(max_changes: usize, max_bytes: u64) -> Self {
        Self {
            remaining_rows: max_changes,
            remaining_bytes: max_bytes,
        }
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
    pub(crate) id: String,
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
    let mut properties = logical_row_image(dataset, &raw.slice, 0).await?;
    properties.remove("id");
    let endpoints = if kind == ChangeEntityKind::Edge {
        let from = properties
            .remove("src")
            .and_then(|value| value.as_str().map(str::to_string))
            .ok_or_else(|| OmniError::Lance("edge image is missing src".to_string()))?;
        let to = properties
            .remove("dst")
            .and_then(|value| value.as_str().map(str::to_string))
            .ok_or_else(|| OmniError::Lance("edge image is missing dst".to_string()))?;
        Some(EntityEndpoints { from, to })
    } else {
        None
    };
    Ok(EntityImage {
        properties,
        endpoints,
    })
}

enum Emit {
    Delete(RawRow),
    Insert(RawRow),
    Update { before: RawRow, after: RawRow },
}

impl Emit {
    fn op(&self) -> ChangeOpKind {
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
async fn next_emit(
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
                // Parent→child within one branch lifetime: managed descriptor
                // identity is source-qualified and authoritative.
                if rows_equal(
                    from.dataset(),
                    &left,
                    to.dataset(),
                    &right,
                    BlobComparisonScope::SameLineage,
                )
                .await?
                {
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
struct IntervalPlan {
    /// The published opaque type identity: the block ordering key, the
    /// continuation key, and `GraphTypeRef.id`, all one value.
    opaque_id: String,
    kind: ChangeEntityKind,
    type_name: String,
    from_dataset: Dataset,
    to_dataset: Dataset,
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
) -> Result<Vec<IntervalPlan>> {
    let intervals = changed_table_intervals(parent, child);

    let mut plans = Vec::with_capacity(intervals.len());
    for interval in intervals {
        let table_key = interval.table_key();
        match (interval.from, interval.to) {
            (None, Some(added)) => {
                if added.row_count > 0 {
                    return Err(schema_boundary(graph_commit_id, table_key));
                }
            }
            (Some(removed), None) => {
                if removed.row_count > 0 {
                    return Err(schema_boundary(graph_commit_id, table_key));
                }
            }
            (Some(from), Some(to)) => {
                let from_dataset = store.open_at_entry(from).await?;
                let to_dataset = store.open_at_entry(to).await?;
                if user_schema_fingerprint(&from_dataset) != user_schema_fingerprint(&to_dataset) {
                    return Err(schema_boundary(graph_commit_id, table_key));
                }
                let (kind, type_name) = parse_table_key(table_key);
                plans.push(IntervalPlan {
                    opaque_id: opaque_type_id(schema_identity_domain, interval.identity),
                    kind: kind.into(),
                    type_name: type_name.to_string(),
                    from_dataset,
                    to_dataset,
                });
            }
            (None, None) => unreachable!("changed intervals have at least one endpoint"),
        }
    }
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
    )
    .await?;

    let mut resume_seen = resume.is_none();
    let mut next_change_index = resume.map_or(0, |key| key.change_index + 1);
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
                    resume_seen = true;
                    Some(key.id.as_str())
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
        let mut left = OrderedRows::open(plan.from_dataset, after_id).await?;
        let mut right = OrderedRows::open(plan.to_dataset, after_id).await?;

        while let Some(emit) = next_emit(&mut left, &mut right, scope).await? {
            let op = emit.op();
            let (id, before, after) = match emit {
                Emit::Insert(raw) => {
                    let image = emitted_image(right.dataset(), &raw, plan.kind).await?;
                    (raw.id, None, Some(image))
                }
                Emit::Delete(raw) => {
                    let image = emitted_image(left.dataset(), &raw, plan.kind).await?;
                    (raw.id, Some(image), None)
                }
                Emit::Update { before, after } => {
                    let before_image = emitted_image(left.dataset(), &before, plan.kind).await?;
                    let after_image = emitted_image(right.dataset(), &after, plan.kind).await?;
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

            if budget.remaining_rows == 0 || encoded_bytes > budget.remaining_bytes {
                return Ok(match last_emitted {
                    Some(key) if emitted_this_call => CommitEnumeration::Truncated(key),
                    _ => CommitEnumeration::Exhausted {
                        required_bytes: encoded_bytes,
                    },
                });
            }
            budget.remaining_rows -= 1;
            budget.remaining_bytes -= encoded_bytes;
            last_emitted = Some(ContinuationKey {
                type_id: plan.opaque_id.clone(),
                id: change.id.clone(),
                operation_rank: op.rank(),
                change_index: next_change_index,
            });
            emitted_this_call = true;
            next_change_index += 1;
            out.push(change);
        }
    }

    if !resume_seen {
        return Err(cursor_rejected(
            "change continuation no longer names a changed table of this commit",
        ));
    }
    Ok(CommitEnumeration::Complete)
}
