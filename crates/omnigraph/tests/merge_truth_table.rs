//! Merge-pair truth table (MR-786).
//!
//! Enumerates every `(left_op, right_op)` cell from the graph operation
//! vocabulary `{noop, addNode, removeNode, addEdge, removeEdge,
//! setProperty, dropProperty, addLabel, removeLabel}` and asserts the
//! deterministic outcome of `Omnigraph::branch_merge`.
//!
//! The vocabulary is the one named in the ticket. Today's mutation grammar
//! only exposes `insert | update set | delete`, so `dropProperty`,
//! `addLabel`, and `removeLabel` are dispositioned as
//! [`Expected::Unsupported`] cells — they live in the matrix so adding the
//! ops later is a compile-time, fail-on-omission task. Adding a new op to
//! [`OpVariant`] forces a non-exhaustive match in [`build_case`] and so
//! refuses to compile until every new pair has been considered.
//!
//! For each executable cell the matrix runs the pair through the real
//! `branch_merge` path and asserts:
//!
//! * the [`MergeOutcome`] (or [`MergeConflictKind`]-bearing
//!   [`OmniError::MergeConflicts`] error), and
//! * the affected graph state on `main` after a successful merge.
//!
//! `branch_merge(source, target)` is directional: target acts as the merge
//! base anchor while source's diff is replayed. The matrix already
//! enumerates both `(L, R)` and `(R, L)` as independent cells (e.g.
//! `(Noop, AddNode)` expects `AlreadyUpToDate` because source is unchanged,
//! while `(AddNode, Noop)` expects `FastForward` because target is
//! unchanged). The op-pair symmetry of the matrix definition serves as the
//! commutativity oracle without doubling the runtime.
//!
//! See the addendum on the ticket for the *Lance-level* second axis
//! (Rebasable/Retryable/Incompatible). That axis describes concurrent
//! commits on a single branch and is therefore not applicable to pure
//! three-way `branch_merge` cells. Each cell records
//! [`LanceOutcome::NotApplicable`] for now; the placeholder column keeps
//! the data shape ready for the DST harness in MR-784 to populate.

mod helpers;

use std::time::Instant;

use helpers::{count_rows, mixed_params, mutate_branch, params, query_main};
use omnigraph::db::{MergeOutcome, Omnigraph};
use omnigraph::error::{MergeConflictKind, OmniError};
use omnigraph::loader::{LoadMode, load_jsonl};

// ─── Fixture ────────────────────────────────────────────────────────────────

const TRUTH_SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32?
}

edge Knows: Person -> Person
"#;

/// Base graph: four people; the only edge is `Bob -> Carol`.
///
/// `Alice` and `Dan` start isolated so [`Apply::DeleteAlice`] never produces
/// orphan-edge fallout by itself. `Bob -> Carol` exists so
/// [`Apply::DeleteKnowsFromBob`] always has a row to remove. `Eve` does
/// not exist, so [`Apply::InsertEve`] is always a fresh insert.
const TRUTH_DATA: &str = r#"{"type":"Person","data":{"name":"Alice","age":30}}
{"type":"Person","data":{"name":"Bob","age":25}}
{"type":"Person","data":{"name":"Carol","age":40}}
{"type":"Person","data":{"name":"Dan","age":50}}
{"edge":"Knows","from":"Bob","to":"Carol"}"#;

const TRUTH_MUTATIONS: &str = r#"
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}

query delete_person($name: String) {
    delete Person where name = $name
}

query insert_knows($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}

query delete_knows_from($from: String) {
    delete Knows where from = $from
}

query set_person_age($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}

query get_person($name: String) {
    match {
        $p: Person { name: $name }
    }
    return { $p.name, $p.age }
}
"#;

async fn bootstrap(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TRUTH_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TRUTH_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db
}

// ─── Op vocabulary ──────────────────────────────────────────────────────────

/// The graph operation vocabulary named in MR-786.
///
/// Exhaustiveness on this enum is what makes adding a new op a compile-time
/// failure in [`build_case`]. The ticket asks for nine variants even though
/// only six are representable in today's mutation grammar; the rest live
/// here so the surface stays honest as the language grows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // variants are constructed via OpVariant::ALL
enum OpVariant {
    Noop,
    AddNode,
    RemoveNode,
    AddEdge,
    RemoveEdge,
    SetProperty,
    /// Not in today's mutation grammar; see `crates/omnigraph/docs/query-language.md`
    /// (`.gq` exposes only `insert | update set | delete`).
    DropProperty,
    /// Schema has no first-class label concept; labels are encoded as
    /// node types today.
    AddLabel,
    /// Same shape as [`OpVariant::AddLabel`].
    RemoveLabel,
}

impl OpVariant {
    const ALL: [OpVariant; 9] = [
        OpVariant::Noop,
        OpVariant::AddNode,
        OpVariant::RemoveNode,
        OpVariant::AddEdge,
        OpVariant::RemoveEdge,
        OpVariant::SetProperty,
        OpVariant::DropProperty,
        OpVariant::AddLabel,
        OpVariant::RemoveLabel,
    ];

    fn label(self) -> &'static str {
        match self {
            OpVariant::Noop => "noop",
            OpVariant::AddNode => "addNode",
            OpVariant::RemoveNode => "removeNode",
            OpVariant::AddEdge => "addEdge",
            OpVariant::RemoveEdge => "removeEdge",
            OpVariant::SetProperty => "setProperty",
            OpVariant::DropProperty => "dropProperty",
            OpVariant::AddLabel => "addLabel",
            OpVariant::RemoveLabel => "removeLabel",
        }
    }
}

/// A concrete branch action — what a single `mutate_branch` invocation does
/// for one side of the matrix. Each cell picks the variant + values that
/// produce the documented outcome (e.g. `(SetProperty, SetProperty)` picks
/// two different ages so the cell collapses to `DivergentUpdate`).
#[derive(Debug, Clone, Copy)]
enum Apply {
    /// Do nothing on this side. Distinct from `OpVariant::Noop` so the two
    /// enums can coexist under `use Apply::*; use OpVariant::*` in the
    /// matrix builder.
    Skip,
    InsertEve {
        age: i32,
    },
    DeleteAlice,
    InsertAliceCarol,
    DeleteKnowsFromBob,
    SetAliceAge {
        age: i32,
    },
}

async fn apply(db: &mut Omnigraph, branch: &str, action: Apply) {
    match action {
        Apply::Skip => {}
        Apply::InsertEve { age } => {
            mutate_branch(
                db,
                branch,
                TRUTH_MUTATIONS,
                "insert_person",
                &mixed_params(&[("$name", "Eve")], &[("$age", age as i64)]),
            )
            .await
            .unwrap();
        }
        Apply::DeleteAlice => {
            mutate_branch(
                db,
                branch,
                TRUTH_MUTATIONS,
                "delete_person",
                &params(&[("$name", "Alice")]),
            )
            .await
            .unwrap();
        }
        Apply::InsertAliceCarol => {
            mutate_branch(
                db,
                branch,
                TRUTH_MUTATIONS,
                "insert_knows",
                &params(&[("$from", "Alice"), ("$to", "Carol")]),
            )
            .await
            .unwrap();
        }
        Apply::DeleteKnowsFromBob => {
            mutate_branch(
                db,
                branch,
                TRUTH_MUTATIONS,
                "delete_knows_from",
                &params(&[("$from", "Bob")]),
            )
            .await
            .unwrap();
        }
        Apply::SetAliceAge { age } => {
            mutate_branch(
                db,
                branch,
                TRUTH_MUTATIONS,
                "set_person_age",
                &mixed_params(&[("$name", "Alice")], &[("$age", age as i64)]),
            )
            .await
            .unwrap();
        }
    }
}

// ─── Expected outcome ───────────────────────────────────────────────────────

/// Lance-level conflict outcome (per the addendum's second axis). Pure
/// graph `branch_merge` is three-way and does not race per-table commits,
/// so every cell here records `NotApplicable`. The variants exist so the
/// DST harness in MR-784 can extend the same table shape with cells that
/// *do* cross Lance commit semantics (e.g. `Merge × Rewrite`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum LanceOutcome {
    NotApplicable,
    Rebasable,
    Retryable,
    Incompatible,
}

/// A single conflict expectation — matches `MergeConflict` fields with
/// `row_id` left optional because generated edge ULIDs aren't predictable.
#[derive(Debug, Clone, Copy)]
struct ConflictMatch {
    table_key: &'static str,
    kind: MergeConflictKind,
    row_id: Option<&'static str>,
}

#[derive(Debug, Clone)]
enum Expected {
    AlreadyUpToDate,
    FastForward(GraphAssert),
    Merged(GraphAssert),
    Conflicts(Vec<ConflictMatch>),
    /// Cell involves an op variant not representable in today's mutation
    /// grammar. Recorded with a `note` for follow-up work; the runner
    /// skips execution but counts the cell as covered by the matrix.
    Unsupported {
        #[allow(dead_code)]
        note: &'static str,
    },
}

/// Post-merge assertions on `main`. The truth table cells touch a small
/// set of entities; rather than hashing entire tables (which is brittle
/// across Lance compaction strategy changes) we assert the affected
/// entity shape directly.
#[derive(Debug, Clone)]
struct GraphAssert {
    /// Expected `node:Person` row count.
    persons: usize,
    /// Expected `edge:Knows` row count.
    knows_edges: usize,
    /// Expected age of `Alice`, or `None` if Alice is absent.
    alice_age: Option<i32>,
    /// Whether `Eve` is present in `node:Person`.
    eve_present: bool,
}

impl GraphAssert {
    const fn base() -> Self {
        Self {
            persons: 4,
            knows_edges: 1,
            alice_age: Some(30),
            eve_present: false,
        }
    }

    const fn with_alice_age(mut self, age: i32) -> Self {
        self.alice_age = Some(age);
        self
    }

    const fn without_alice(mut self) -> Self {
        self.alice_age = None;
        self.persons -= 1;
        self
    }

    const fn with_eve(mut self) -> Self {
        self.eve_present = true;
        self.persons += 1;
        self
    }

    const fn with_extra_knows_edge(mut self) -> Self {
        self.knows_edges += 1;
        self
    }

    const fn without_bob_carol(mut self) -> Self {
        self.knows_edges -= 1;
        self
    }
}

// ─── Case + matrix ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct MergeCase {
    left: OpVariant,
    right: OpVariant,
    apply_left: Apply,
    apply_right: Apply,
    expected: Expected,
    /// Lance-level second axis (see [`LanceOutcome`]). All pure-merge
    /// cells record `NotApplicable`; the field exists so the column shape
    /// is in place for MR-784's DST harness, which extends the matrix
    /// with cells that *do* cross Lance commit semantics.
    #[allow(dead_code)]
    lance: LanceOutcome,
    /// Free-form note shown in matrix-debugging output. Read via `{:?}`
    /// rendering only; not asserted.
    #[allow(dead_code)]
    note: &'static str,
}

impl MergeCase {
    fn unsupported(left: OpVariant, right: OpVariant, note: &'static str) -> Self {
        Self {
            left,
            right,
            apply_left: Apply::Skip,
            apply_right: Apply::Skip,
            expected: Expected::Unsupported { note },
            lance: LanceOutcome::NotApplicable,
            note,
        }
    }
}

/// Build the cell for a single `(left, right)` pair.
///
/// **The match is exhaustive on purpose.** Adding a variant to
/// [`OpVariant`] forces a compile error here, which is the acceptance
/// criterion "adding a new graph op forces a compile error in the truth
/// table".
fn build_case(left: OpVariant, right: OpVariant) -> MergeCase {
    use Apply::*;
    use OpVariant::*;

    let mk = |al: Apply, ar: Apply, exp: Expected, note: &'static str| MergeCase {
        left,
        right,
        apply_left: al,
        apply_right: ar,
        expected: exp,
        lance: LanceOutcome::NotApplicable,
        note,
    };

    let conflict = |kind: MergeConflictKind, table: &'static str, row: Option<&'static str>| {
        Expected::Conflicts(vec![ConflictMatch {
            table_key: table,
            kind,
            row_id: row,
        }])
    };

    match (left, right) {
        // ─── Row: Noop ──────────────────────────────────────────────────
        (Noop, Noop) => mk(
            Skip,
            Skip,
            Expected::AlreadyUpToDate,
            "no change either side",
        ),
        (Noop, AddNode) => mk(
            Skip,
            InsertEve { age: 22 },
            Expected::AlreadyUpToDate,
            "source unchanged → up to date",
        ),
        (Noop, RemoveNode) => mk(
            Skip,
            DeleteAlice,
            Expected::AlreadyUpToDate,
            "source unchanged → up to date",
        ),
        (Noop, AddEdge) => mk(
            Skip,
            InsertAliceCarol,
            Expected::AlreadyUpToDate,
            "source unchanged → up to date",
        ),
        (Noop, RemoveEdge) => mk(
            Skip,
            DeleteKnowsFromBob,
            Expected::AlreadyUpToDate,
            "source unchanged → up to date",
        ),
        (Noop, SetProperty) => mk(
            Skip,
            SetAliceAge { age: 31 },
            Expected::AlreadyUpToDate,
            "source unchanged → up to date",
        ),

        // ─── Row: AddNode ───────────────────────────────────────────────
        (AddNode, Noop) => mk(
            InsertEve { age: 22 },
            Skip,
            Expected::FastForward(GraphAssert::base().with_eve()),
            "target unchanged → fast-forward",
        ),
        (AddNode, AddNode) => mk(
            InsertEve { age: 21 },
            InsertEve { age: 22 },
            conflict(
                MergeConflictKind::DivergentInsert,
                "node:Person",
                Some("Eve"),
            ),
            "both sides insert Eve with different ages",
        ),
        (AddNode, RemoveNode) => mk(
            InsertEve { age: 22 },
            DeleteAlice,
            Expected::Merged(GraphAssert::base().with_eve().without_alice()),
            "disjoint: insert + delete different nodes",
        ),
        (AddNode, AddEdge) => mk(
            InsertEve { age: 22 },
            InsertAliceCarol,
            Expected::Merged(GraphAssert::base().with_eve().with_extra_knows_edge()),
            "disjoint: insert node + insert unrelated edge",
        ),
        (AddNode, RemoveEdge) => mk(
            InsertEve { age: 22 },
            DeleteKnowsFromBob,
            Expected::Merged(GraphAssert::base().with_eve().without_bob_carol()),
            "disjoint: insert node + delete edge",
        ),
        (AddNode, SetProperty) => mk(
            InsertEve { age: 22 },
            SetAliceAge { age: 31 },
            Expected::Merged(GraphAssert::base().with_eve().with_alice_age(31)),
            "disjoint: insert node + update other node",
        ),

        // ─── Row: RemoveNode ────────────────────────────────────────────
        (RemoveNode, Noop) => mk(
            DeleteAlice,
            Skip,
            Expected::FastForward(GraphAssert::base().without_alice()),
            "target unchanged → fast-forward",
        ),
        (RemoveNode, AddNode) => mk(
            DeleteAlice,
            InsertEve { age: 22 },
            Expected::Merged(GraphAssert::base().without_alice().with_eve()),
            "disjoint: delete + insert different nodes",
        ),
        (RemoveNode, RemoveNode) => mk(
            DeleteAlice,
            DeleteAlice,
            Expected::Merged(GraphAssert::base().without_alice()),
            "both sides delete Alice — idempotent",
        ),
        (RemoveNode, AddEdge) => mk(
            DeleteAlice,
            InsertAliceCarol,
            conflict(MergeConflictKind::OrphanEdge, "edge:Knows", None),
            "delete Alice on left races edge that needs Alice as src",
        ),
        (RemoveNode, RemoveEdge) => mk(
            DeleteAlice,
            DeleteKnowsFromBob,
            Expected::Merged(GraphAssert::base().without_alice().without_bob_carol()),
            "disjoint: delete node + delete unrelated edge",
        ),
        (RemoveNode, SetProperty) => mk(
            DeleteAlice,
            SetAliceAge { age: 31 },
            conflict(
                MergeConflictKind::DeleteVsUpdate,
                "node:Person",
                Some("Alice"),
            ),
            "delete vs update on same row",
        ),

        // ─── Row: AddEdge ───────────────────────────────────────────────
        (AddEdge, Noop) => mk(
            InsertAliceCarol,
            Skip,
            Expected::FastForward(GraphAssert::base().with_extra_knows_edge()),
            "target unchanged → fast-forward",
        ),
        (AddEdge, AddNode) => mk(
            InsertAliceCarol,
            InsertEve { age: 22 },
            Expected::Merged(GraphAssert::base().with_extra_knows_edge().with_eve()),
            "disjoint: insert edge + insert unrelated node",
        ),
        (AddEdge, RemoveNode) => mk(
            InsertAliceCarol,
            DeleteAlice,
            conflict(MergeConflictKind::OrphanEdge, "edge:Knows", None),
            "edge needs Alice as src; target deleted Alice",
        ),
        (AddEdge, AddEdge) => mk(
            InsertAliceCarol,
            InsertAliceCarol,
            // Both sides insert the same logical edge but each generates a
            // fresh ULID id, so the merge sees two distinct rows with the
            // same (src, dst) pair and keeps both. This is the current
            // behavior; whether duplicate Knows edges should be deduplicated
            // by endpoints is tracked separately — the cell pins the
            // current contract.
            Expected::Merged(GraphAssert {
                persons: 4,
                knows_edges: 3,
                alice_age: Some(30),
                eve_present: false,
            }),
            "both insert Alice→Carol; current edge model preserves duplicates",
        ),
        (AddEdge, RemoveEdge) => mk(
            InsertAliceCarol,
            DeleteKnowsFromBob,
            // Merged result: insert Alice→Carol applied; Bob→Carol removed.
            // Net edges: base 1 - 1 deleted + 1 inserted = 1 (matches base).
            Expected::Merged(GraphAssert::base()),
            "disjoint: insert one edge + delete different edge",
        ),
        (AddEdge, SetProperty) => mk(
            InsertAliceCarol,
            SetAliceAge { age: 31 },
            Expected::Merged(
                GraphAssert::base()
                    .with_extra_knows_edge()
                    .with_alice_age(31),
            ),
            "disjoint: insert edge involving Alice + update Alice.age",
        ),

        // ─── Row: RemoveEdge ────────────────────────────────────────────
        (RemoveEdge, Noop) => mk(
            DeleteKnowsFromBob,
            Skip,
            Expected::FastForward(GraphAssert::base().without_bob_carol()),
            "target unchanged → fast-forward",
        ),
        (RemoveEdge, AddNode) => mk(
            DeleteKnowsFromBob,
            InsertEve { age: 22 },
            Expected::Merged(GraphAssert::base().without_bob_carol().with_eve()),
            "disjoint: delete edge + insert node",
        ),
        (RemoveEdge, RemoveNode) => mk(
            DeleteKnowsFromBob,
            DeleteAlice,
            Expected::Merged(GraphAssert::base().without_bob_carol().without_alice()),
            "disjoint: delete edge + delete unrelated node",
        ),
        (RemoveEdge, AddEdge) => mk(
            DeleteKnowsFromBob,
            InsertAliceCarol,
            // Bob→Carol removed; Alice→Carol added. Net edges = 1.
            Expected::Merged(GraphAssert::base()),
            "disjoint: delete one edge + insert another",
        ),
        (RemoveEdge, RemoveEdge) => mk(
            DeleteKnowsFromBob,
            DeleteKnowsFromBob,
            Expected::Merged(GraphAssert::base().without_bob_carol()),
            "both sides delete Bob→Carol — idempotent",
        ),
        (RemoveEdge, SetProperty) => mk(
            DeleteKnowsFromBob,
            SetAliceAge { age: 31 },
            Expected::Merged(GraphAssert::base().without_bob_carol().with_alice_age(31)),
            "disjoint: delete edge + update unrelated node",
        ),

        // ─── Row: SetProperty ───────────────────────────────────────────
        (SetProperty, Noop) => mk(
            SetAliceAge { age: 31 },
            Skip,
            Expected::FastForward(GraphAssert::base().with_alice_age(31)),
            "target unchanged → fast-forward",
        ),
        (SetProperty, AddNode) => mk(
            SetAliceAge { age: 31 },
            InsertEve { age: 22 },
            Expected::Merged(GraphAssert::base().with_alice_age(31).with_eve()),
            "disjoint: update + insert",
        ),
        (SetProperty, RemoveNode) => mk(
            SetAliceAge { age: 31 },
            DeleteAlice,
            conflict(
                MergeConflictKind::DeleteVsUpdate,
                "node:Person",
                Some("Alice"),
            ),
            "update vs delete on same row",
        ),
        (SetProperty, AddEdge) => mk(
            SetAliceAge { age: 31 },
            InsertAliceCarol,
            Expected::Merged(
                GraphAssert::base()
                    .with_alice_age(31)
                    .with_extra_knows_edge(),
            ),
            "disjoint: update Alice + insert edge involving Alice",
        ),
        (SetProperty, RemoveEdge) => mk(
            SetAliceAge { age: 31 },
            DeleteKnowsFromBob,
            Expected::Merged(GraphAssert::base().with_alice_age(31).without_bob_carol()),
            "disjoint: update node + delete edge",
        ),
        (SetProperty, SetProperty) => mk(
            SetAliceAge { age: 31 },
            SetAliceAge { age: 32 },
            conflict(
                MergeConflictKind::DivergentUpdate,
                "node:Person",
                Some("Alice"),
            ),
            "both sides set Alice.age to different values",
        ),

        // ─── Unsupported (DropProperty / AddLabel / RemoveLabel) ────────
        //
        // Any cell that involves one of these op variants is dispositioned
        // as `Unsupported`. The mutation grammar exposes `insert | update
        // set | delete` only (see `docs/query-language.md`); there is no
        // first-class `drop nullable property` or `add/remove label`
        // operation today. When those land, fill in the cells below and
        // delete the catch-all.
        (DropProperty, _) | (_, DropProperty) => {
            MergeCase::unsupported(left, right, "dropProperty: not in mutation grammar today")
        }
        (AddLabel, _) | (_, AddLabel) => {
            MergeCase::unsupported(left, right, "addLabel: schema has no first-class label op")
        }
        (RemoveLabel, _) | (_, RemoveLabel) => MergeCase::unsupported(
            left,
            right,
            "removeLabel: schema has no first-class label op",
        ),
    }
}

// ─── Runner ─────────────────────────────────────────────────────────────────

/// Result of executing one direction of a single cell.
#[derive(Debug)]
struct DirectionResult {
    /// Cell label, e.g. `"addNode×removeNode"`. Read via `{:?}` rendering;
    /// not asserted directly.
    #[allow(dead_code)]
    cell: String,
    outcome: ActualOutcome,
}

#[derive(Debug, PartialEq)]
enum ActualOutcome {
    AlreadyUpToDate,
    FastForward,
    Merged,
    Conflicts(Vec<(String, MergeConflictKind, Option<String>)>),
    Skipped,
}

async fn run_direction(
    label: &str,
    left_op: Apply,
    right_op: Apply,
    expected: &Expected,
) -> DirectionResult {
    let dir = tempfile::tempdir().unwrap();
    let mut db = bootstrap(&dir).await;
    db.branch_create("feature").await.unwrap();

    // One handle, two branches — matches the pattern in
    // `tests/branching.rs`. Using two `Omnigraph::open` handles for one
    // dataset is unnecessary here and only opens room for cache-coherency
    // surprises that are out of scope for this test.
    apply(&mut db, "feature", left_op).await;
    apply(&mut db, "main", right_op).await;

    let merge_result = db.branch_merge("feature", "main").await;
    let outcome = match merge_result {
        Ok(MergeOutcome::AlreadyUpToDate) => ActualOutcome::AlreadyUpToDate,
        Ok(MergeOutcome::FastForward) => ActualOutcome::FastForward,
        Ok(MergeOutcome::Merged) => ActualOutcome::Merged,
        Err(OmniError::MergeConflicts(conflicts)) => ActualOutcome::Conflicts(
            conflicts
                .into_iter()
                .map(|c| (c.table_key, c.kind, c.row_id))
                .collect(),
        ),
        Err(other) => panic!("[{label}] unexpected merge error: {other:?}"),
    };

    if let Expected::Merged(assert) | Expected::FastForward(assert) = expected
        && matches!(
            outcome,
            ActualOutcome::Merged | ActualOutcome::FastForward | ActualOutcome::AlreadyUpToDate
        )
    {
        assert_state(&mut db, assert, label).await;
    }

    // Post-conflict invariant: `branch_merge` is atomic, so a failed
    // merge must leave target exactly as `right_op` alone produced it —
    // none of `left_op`'s mutations leak in. This generalizes the
    // assertion that lived in `branching.rs::branch_merge_reports_
    // divergent_update_conflict` (Alice.age stays 31 after the conflict)
    // to every conflict cell.
    if matches!(outcome, ActualOutcome::Conflicts(_)) {
        let expected_target = state_after_apply_only(right_op);
        assert_state(&mut db, &expected_target, label).await;
    }

    DirectionResult {
        cell: label.to_string(),
        outcome,
    }
}

/// Expected state on `main` after `action` alone runs on the base fixture.
///
/// Used by the conflict-cell invariant in [`run_direction`] to assert
/// `branch_merge` left target unchanged on the failure path.
fn state_after_apply_only(action: Apply) -> GraphAssert {
    match action {
        Apply::Skip => GraphAssert::base(),
        Apply::InsertEve { .. } => GraphAssert::base().with_eve(),
        Apply::DeleteAlice => GraphAssert::base().without_alice(),
        Apply::InsertAliceCarol => GraphAssert::base().with_extra_knows_edge(),
        Apply::DeleteKnowsFromBob => GraphAssert::base().without_bob_carol(),
        Apply::SetAliceAge { age } => GraphAssert::base().with_alice_age(age),
    }
}

async fn assert_state(db: &mut Omnigraph, expected: &GraphAssert, label: &str) {
    let person_count = count_rows(db, "node:Person").await;
    assert_eq!(
        person_count, expected.persons,
        "[{label}] node:Person count"
    );

    let knows_count = count_rows(db, "edge:Knows").await;
    assert_eq!(
        knows_count, expected.knows_edges,
        "[{label}] edge:Knows count"
    );

    // Alice presence + age.
    let alice = query_main(
        db,
        TRUTH_MUTATIONS,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    match expected.alice_age {
        None => assert_eq!(alice.num_rows(), 0, "[{label}] Alice should be absent"),
        Some(age) => {
            assert_eq!(alice.num_rows(), 1, "[{label}] Alice should be present");
            let batch = alice.concat_batches().unwrap();
            // get_person returns `{ $p.name, $p.age }` — name is col 0, age is col 1.
            let ages = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap();
            assert_eq!(ages.value(0), age, "[{label}] Alice.age");
        }
    }

    // Eve presence.
    let eve = query_main(
        db,
        TRUTH_MUTATIONS,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(
        eve.num_rows(),
        usize::from(expected.eve_present),
        "[{label}] Eve presence mismatch"
    );
}

fn check_outcome(label: &str, expected: &Expected, actual: &ActualOutcome) {
    match (expected, actual) {
        (Expected::AlreadyUpToDate, ActualOutcome::AlreadyUpToDate)
        | (Expected::FastForward(_), ActualOutcome::FastForward)
        | (Expected::Merged(_), ActualOutcome::Merged) => {}
        (Expected::Conflicts(want), ActualOutcome::Conflicts(got)) => {
            // The truth table's whole point is a deterministic oracle, so
            // a regression that produces extra spurious conflicts (e.g.
            // emitting both `OrphanEdge` and a stray `DivergentInsert`)
            // must fail this assertion. Hence exact-count equality, not
            // subset inclusion.
            assert_eq!(
                got.len(),
                want.len(),
                "[{label}] expected {} conflict(s) but got {}: {got:?}",
                want.len(),
                got.len()
            );
            for w in want {
                let hit = got.iter().any(|(table, kind, row)| {
                    table == w.table_key
                        && *kind == w.kind
                        && match w.row_id {
                            None => true,
                            Some(expected_row) => row.as_deref() == Some(expected_row),
                        }
                });
                assert!(
                    hit,
                    "[{label}] expected conflict {{table={}, kind={:?}, row={:?}}} not found in {:?}",
                    w.table_key, w.kind, w.row_id, got
                );
            }
        }
        (Expected::Unsupported { .. }, ActualOutcome::Skipped) => {}
        _ => panic!("[{label}] expected {expected:?}, got {actual:?}"),
    }
}

/// Run a single `(left, right)` cell and assert the documented outcome.
///
/// The matrix already enumerates both `(L, R)` and `(R, L)` as separate
/// cells, so a per-cell swap pass would double the runtime without
/// covering new combinations — `branch_merge(source, target)` is
/// directional, and the `(Noop, AddNode)` vs `(AddNode, Noop)` split is
/// exactly the kind of pair that lives in two distinct cells (one expects
/// `AlreadyUpToDate`, the other expects `FastForward`).
async fn run_cell(case: &MergeCase) -> DirectionResult {
    let label = format!("{}×{}", case.left.label(), case.right.label());

    if matches!(case.expected, Expected::Unsupported { .. }) {
        return DirectionResult {
            cell: label,
            outcome: ActualOutcome::Skipped,
        };
    }

    let result = run_direction(&label, case.apply_left, case.apply_right, &case.expected).await;
    check_outcome(&label, &case.expected, &result.outcome);
    result
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_pair_truth_table() {
    let start = Instant::now();
    let mut total_cells = 0_usize;
    let mut executable_cells = 0_usize;
    let mut unsupported_cells = 0_usize;
    let mut directions_run = 0_usize;

    for left in OpVariant::ALL {
        for right in OpVariant::ALL {
            total_cells += 1;
            let case = build_case(left, right);
            if matches!(case.expected, Expected::Unsupported { .. }) {
                unsupported_cells += 1;
            } else {
                executable_cells += 1;
            }
            let result = run_cell(&case).await;
            if !matches!(result.outcome, ActualOutcome::Skipped) {
                directions_run += 1;
            }
        }
    }

    let elapsed = start.elapsed();
    println!(
        "merge truth table: {} cells total ({} executable, {} unsupported), {} executions in {:.2}s",
        total_cells,
        executable_cells,
        unsupported_cells,
        directions_run,
        elapsed.as_secs_f64(),
    );

    assert_eq!(total_cells, 81, "truth table must enumerate all 81 cells");
    assert_eq!(
        executable_cells, 36,
        "expected 6×6 executable cells under the current mutation grammar"
    );
    assert_eq!(
        unsupported_cells, 45,
        "expected 45 cells involving dropProperty/addLabel/removeLabel"
    );
    assert!(
        elapsed.as_secs() < 30,
        "merge truth table exceeded 30s budget: {elapsed:?}"
    );
}
