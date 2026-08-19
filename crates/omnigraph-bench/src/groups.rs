//! Plain-language grouping of the 14 `MergeTimingPhase` buckets for the
//! reader-facing views (`show` and `report`). Grouping adds a layer, never
//! hides data: every view renders the raw phases (exact names, total/max µs)
//! beneath their group.
//!
//! The mapping and every annotation are verified against the engine source
//! (`crates/omnigraph/src/exec/merge.rs`, the `record_merge_timing` call
//! sites):
//!
//! - `TableWalk` is the general route's per-table three-way ordered walk
//!   plus merged-row staging (`stage_streaming_table_merge`), one interval
//!   per table, disjoint from the keyed publish loop by construction. The
//!   proven insert-only route bypasses the walk, so it reads zero there.
//!   Records written before the phase existed carry no `TableWalk` entry —
//!   their walk time stays in the unattributed remainder, honestly.
//! - `KeyedStage`/`KeyedCommit` are per-chunk sub-intervals recorded inside
//!   `commit_keyed_stream_chunks`, which BOTH publish routes share. On the
//!   proven-insert route that loop runs inside the span recorded as
//!   `PhysicalPublish` (`publish_proven_pure_insert_adopt` — the only
//!   `PhysicalPublish` record site), so summing all three would double-count.
//!   On the general rewrite route no `PhysicalPublish` interval is recorded
//!   and the two sub-buckets are the only physical-write timings. The write
//!   group therefore totals `max(PhysicalPublish, KeyedStage + KeyedCommit)`:
//!   the parent when it exists, the bare sub-buckets when it does not, never
//!   both. (A per-table mix of routes would undercount here and surface as
//!   unattributed time — the honest direction.)
//! - Shares are computed against the wall-clock median, not the phase sum, so
//!   the "biggest cost" headline and the unattributed remainder agree.

use std::collections::BTreeSet;

use crate::record::PhaseStats;

/// One plain-language group over raw phase names.
struct PhaseGroup {
    name: &'static str,
    annotation: &'static str,
    members: &'static [&'static str],
}

const WRITE_GROUP_NAME: &str = "write merged data";

/// The seven groups, in phase-model order (rendering sorts by time).
const GROUPS: [PhaseGroup; 7] = [
    PhaseGroup {
        name: "setup and refresh",
        annotation: "resolve refs and the base snapshot, take the merge lock, point the \
                     engine at the target; restore and refresh the handle afterwards",
        members: &["OuterPrepare", "OuterRestoreRefresh"],
    },
    PhaseGroup {
        name: "discover changes",
        annotation: "prove the insert-only fast route from the source table's history \
                     since the fork and plan its write chunks; zero when the merge takes \
                     the general three-way route",
        members: &["ProvenInsertHistory", "ProvenInsertPlanScan"],
    },
    PhaseGroup {
        name: "table walk",
        annotation: "the three-way row walk over base, source, and target plus \
                     merged-row staging — grows with full table size, not delta size; \
                     zero on the insert-only fast route",
        members: &["TableWalk"],
    },
    PhaseGroup {
        name: "validate changes",
        annotation: "declared constraints evaluated over the merge delta against the \
                     committed target through its indexes — scales with the delta, not \
                     the table",
        members: &["CandidateValidation", "FinalRevalidation"],
    },
    PhaseGroup {
        name: WRITE_GROUP_NAME,
        annotation: "stage and commit the bounded keyed Lance transactions that write \
                     merged rows onto the target; KeyedStage/KeyedCommit are the \
                     per-chunk sub-steps (inside PhysicalPublish on the insert-only \
                     route, bare on the general route — counted once, never twice)",
        members: &["PhysicalPublish", "KeyedStage", "KeyedCommit"],
    },
    PhaseGroup {
        name: "publish new state",
        annotation: "the one __manifest compare-and-swap that makes every merged table \
                     version and the lineage commit visible together",
        members: &["ManifestPublish"],
    },
    PhaseGroup {
        name: "crash-safety bookkeeping",
        annotation: "arm the recovery sidecar before the first durable effect, confirm \
                     it after every effect lands, delete it after publish",
        members: &["RecoveryArm", "RecoveryConfirm", "RecoveryCleanup"],
    },
];

/// The name and annotation of the explicit remainder bar. Rendered only when
/// the remainder share clears [`UNATTRIBUTED_MIN_SHARE_PCT`] — and never
/// silently normalized away below it.
pub const UNATTRIBUTED_NAME: &str = "not phase-attributed";
pub const UNATTRIBUTED_ANNOTATION: &str = "wall-clock outside all 14 phases (on records written before the TableWalk phase \
     existed, this includes the three-way row walk, which those binaries did not \
     instrument)";
pub const UNATTRIBUTED_MIN_SHARE_PCT: f64 = 5.0;

/// Footnote rendered when the group totals sum past the wall-clock median
/// (shares then exceed 100%): the math prints as computed, never silently
/// capped, and this line explains why it can happen.
pub const OVER_ATTRIBUTION_NOTE: &str =
    "phase p50s are per-phase medians and need not sum under the wall-clock median";

/// One raw phase as shown inside its group's unfolded detail.
pub struct MemberReading {
    pub name: String,
    pub total_us_p50: u64,
    pub total_us_max: u64,
    pub max_single_us: u64,
}

pub struct GroupReading {
    pub name: &'static str,
    pub annotation: &'static str,
    /// Group total in µs (p50 phase totals; the write group's containment
    /// rule is documented on the module).
    pub total_us: u64,
    /// Share of the wall-clock median (percent). 0 when the wall is 0.
    pub share_pct: f64,
    pub members: Vec<MemberReading>,
}

pub struct GroupedPhases {
    /// The seven groups (plus a catch-all for phases outside the known
    /// mapping, so a future 14th phase can never vanish), sorted descending
    /// by total.
    pub groups: Vec<GroupReading>,
    pub unattributed_us: u64,
    pub unattributed_share_pct: f64,
    /// Whether the group totals sum past the wall-clock median — possible
    /// because each phase's p50 is taken independently across reps. Rendering
    /// prints the shares as computed plus [`OVER_ATTRIBUTION_NOTE`].
    pub over_attributed: bool,
}

impl GroupedPhases {
    /// Whether the remainder is material enough to render its explicit bar.
    pub fn shows_unattributed(&self) -> bool {
        self.unattributed_share_pct > UNATTRIBUTED_MIN_SHARE_PCT
    }

    /// The headline "biggest cost": the largest group, or the unattributed
    /// remainder when it is shown and larger — the two never disagree with
    /// the bars below them.
    pub fn biggest_cost(&self) -> (&'static str, f64) {
        let top = self
            .groups
            .first()
            .map(|g| (g.name, g.share_pct))
            .unwrap_or((UNATTRIBUTED_NAME, 0.0));
        if self.shows_unattributed() && self.unattributed_share_pct > top.1 {
            (UNATTRIBUTED_NAME, self.unattributed_share_pct)
        } else {
            top
        }
    }
}

fn member(p: &PhaseStats) -> MemberReading {
    MemberReading {
        name: p.phase.clone(),
        total_us_p50: p.total_us_p50,
        total_us_max: p.total_us_max,
        max_single_us: p.max_single_us,
    }
}

/// Group a record's phases against its wall-clock median (ms).
pub fn group_phases(phases: &[PhaseStats], wall_p50_ms: f64) -> GroupedPhases {
    let wall_us = wall_p50_ms * 1000.0;
    let share = |us: u64| {
        if wall_us > 0.0 {
            us as f64 / wall_us * 100.0
        } else {
            0.0
        }
    };
    let find = |name: &str| phases.iter().find(|p| p.phase == name);
    let mut grouped_names: BTreeSet<&str> = BTreeSet::new();
    let mut groups: Vec<GroupReading> = Vec::with_capacity(GROUPS.len() + 1);
    for group in &GROUPS {
        let members: Vec<MemberReading> = group
            .members
            .iter()
            .inspect(|name| {
                grouped_names.insert(name);
            })
            .filter_map(|name| find(name))
            .map(member)
            .collect();
        let total_us = if group.name == WRITE_GROUP_NAME {
            // Containment rule: parent when recorded, bare sub-buckets when
            // not — see the module doc.
            let physical = find("PhysicalPublish").map_or(0, |p| p.total_us_p50);
            let keyed = find("KeyedStage").map_or(0, |p| p.total_us_p50)
                + find("KeyedCommit").map_or(0, |p| p.total_us_p50);
            physical.max(keyed)
        } else {
            members.iter().map(|m| m.total_us_p50).sum()
        };
        groups.push(GroupReading {
            name: group.name,
            annotation: group.annotation,
            total_us,
            share_pct: share(total_us),
            members,
        });
    }
    let leftovers: Vec<MemberReading> = phases
        .iter()
        .filter(|p| !grouped_names.contains(p.phase.as_str()))
        .map(member)
        .collect();
    if !leftovers.is_empty() {
        let total_us = leftovers.iter().map(|m| m.total_us_p50).sum();
        groups.push(GroupReading {
            name: "other instrumented phases",
            annotation: "phases outside this view's grouping — the grouping is stale; \
                         update it (groups.rs) for the new phase list",
            total_us,
            share_pct: share(total_us),
            members: leftovers,
        });
    }
    groups.sort_by_key(|g| std::cmp::Reverse(g.total_us));
    let attributed_us: u64 = groups.iter().map(|g| g.total_us).sum();
    let unattributed_us = (wall_us - attributed_us as f64).max(0.0) as u64;
    let over_attributed = wall_us > 0.0 && attributed_us as f64 > wall_us;
    GroupedPhases {
        groups,
        over_attributed,
        unattributed_us,
        unattributed_share_pct: share(unattributed_us),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn phase(name: &str, p50: u64) -> PhaseStats {
        PhaseStats {
            phase: name.to_string(),
            total_us_p50: p50,
            total_us_max: p50 + 1,
            max_single_us: p50,
            per_rep_total_us: vec![p50],
        }
    }

    fn group<'a>(grouped: &'a GroupedPhases, name: &str) -> &'a GroupReading {
        grouped
            .groups
            .iter()
            .find(|g| g.name == name)
            .unwrap_or_else(|| panic!("no group {name}"))
    }

    #[test]
    fn write_group_never_double_counts_the_keyed_sub_buckets() {
        // Proven route: PhysicalPublish wraps the sub-buckets — parent wins.
        let phases = [
            phase("PhysicalPublish", 50_000),
            phase("KeyedStage", 30_000),
            phase("KeyedCommit", 10_000),
        ];
        let grouped = group_phases(&phases, 100.0);
        let write = group(&grouped, "write merged data");
        assert_eq!(write.total_us, 50_000, "parent covers its sub-buckets");
        assert_eq!(write.members.len(), 3, "the fold still lists all three");

        // General route: no PhysicalPublish interval — the bare sub-buckets
        // are the write time and must not fall into unattributed.
        let phases = [
            phase("PhysicalPublish", 0),
            phase("KeyedStage", 30_000),
            phase("KeyedCommit", 10_000),
        ];
        let grouped = group_phases(&phases, 100.0);
        assert_eq!(group(&grouped, "write merged data").total_us, 40_000);
    }

    #[test]
    fn shares_are_against_wall_clock_and_remainder_is_explicit() {
        // 100 ms wall, 20 ms in phases: 78% unattributed, never normalized.
        let phases = [
            phase("OuterPrepare", 15_000),
            phase("ManifestPublish", 5_000),
        ];
        let grouped = group_phases(&phases, 100.0);
        assert!((group(&grouped, "setup and refresh").share_pct - 15.0).abs() < 1e-9);
        assert_eq!(grouped.unattributed_us, 80_000);
        assert!((grouped.unattributed_share_pct - 80.0).abs() < 1e-9);
        assert!(grouped.shows_unattributed());
        assert_eq!(grouped.biggest_cost().0, UNATTRIBUTED_NAME);

        // Phases covering the wall: no unattributed bar, biggest cost is the
        // top group.
        let phases = [
            phase("OuterPrepare", 97_000),
            phase("ManifestPublish", 3_000),
        ];
        let grouped = group_phases(&phases, 100.0);
        assert!(!grouped.shows_unattributed());
        assert_eq!(grouped.biggest_cost().0, "setup and refresh");
        // Groups sorted descending by time.
        assert_eq!(grouped.groups[0].name, "setup and refresh");
    }

    #[test]
    fn table_walk_is_its_own_group_and_collapses_the_remainder() {
        // A general-route record from a TableWalk-aware binary: the walk is
        // the named majority and the phases now cover the wall.
        let phases = [
            phase("TableWalk", 82_000),
            phase("OuterPrepare", 10_000),
            phase("KeyedStage", 8_000),
        ];
        let grouped = group_phases(&phases, 100.0);
        assert_eq!(group(&grouped, "table walk").total_us, 82_000);
        assert_eq!(grouped.groups[0].name, "table walk");
        assert!(!grouped.shows_unattributed());
        assert_eq!(grouped.biggest_cost().0, "table walk");
        // A pre-TableWalk record (no such phase) keeps its walk time in the
        // explicit remainder instead.
        let grouped = group_phases(&phases[1..], 100.0);
        assert_eq!(group(&grouped, "table walk").total_us, 0);
        assert!(grouped.shows_unattributed());
    }

    #[test]
    fn unknown_phases_never_vanish() {
        let phases = [phase("SomeFuturePhase", 9_000)];
        let grouped = group_phases(&phases, 10.0);
        let other = group(&grouped, "other instrumented phases");
        assert_eq!(other.total_us, 9_000);
        assert_eq!(other.members[0].name, "SomeFuturePhase");
    }

    #[test]
    fn zero_wall_clock_never_divides() {
        let grouped = group_phases(&[phase("OuterPrepare", 100)], 0.0);
        assert_eq!(grouped.unattributed_us, 0);
        assert_eq!(group(&grouped, "setup and refresh").share_pct, 0.0);
        assert!(!grouped.over_attributed);
    }

    #[test]
    fn over_attribution_prints_as_computed_and_is_flagged() {
        // Per-phase medians summing past the wall median: shares stay as
        // computed (here 120%), never silently capped; the flag drives the
        // footnote.
        let grouped = group_phases(&[phase("OuterPrepare", 120_000)], 100.0);
        assert!(grouped.over_attributed);
        assert!((group(&grouped, "setup and refresh").share_pct - 120.0).abs() < 1e-9);
        assert_eq!(grouped.unattributed_us, 0);
        assert!(!grouped.shows_unattributed());
        // Fully-attributed records never carry the flag.
        let grouped = group_phases(&[phase("OuterPrepare", 90_000)], 100.0);
        assert!(!grouped.over_attributed);
    }
}
