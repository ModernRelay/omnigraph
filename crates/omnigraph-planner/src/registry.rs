//! The routing registry for change-feed and merge operation shapes. An
//! entry requires integrated engine execution and checked-in differential
//! evidence for rows, order and errors. Read queries bypass this registry.

use serde::Serialize;

use crate::logical::{Census, JoinKind, LogicalKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Route {
    Executor,
    Planner,
    PlannerBehindFlag,
}

/// Whether a logical node kind may appear in a routed plan at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coverage {
    Routable,
    RefusedByName,
}

/// An exact shape: the sorted multiset of node kinds plus the sorted kinds
/// of every `Join`. No wildcard, no range, no field predicate; widening an
/// entry means editing this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shape {
    pub kinds: &'static [LogicalKind],
    pub joins: &'static [JoinKind],
}

impl Shape {
    pub fn matches(&self, census: &Census) -> bool {
        census.kinds == self.kinds && census.joins == self.joins
    }

    pub fn census(&self) -> Census {
        Census {
            kinds: self.kinds.to_vec(),
            joins: self.joins.to_vec(),
        }
    }
}

/// One registry entry. `evidence` names tests in the form
/// `<crate>::<test file>::<fn>`; every one must exist, run in the `test` CI
/// job and carry no `#[ignore]`. `negative_case` is one shape outside the
/// pattern that must resolve to the executor.
#[derive(Debug, Clone, Copy)]
pub struct Entry {
    pub name: &'static str,
    pub shape: Shape,
    pub route: Route,
    pub evidence: &'static [&'static str],
    pub negative_case: Shape,
    pub since: &'static str,
}

/// No change-feed or merge execution integration is registered. Their
/// plans remain available to the optimizer; routing keeps the executor
/// until an integration and its differential evidence are added together.
pub static REGISTRY: &[Entry] = &[];

/// Every logical node kind, marked routable or refused by name. Not a
/// registry key space: a kind is not a shape.
pub static COVERAGE: &[(LogicalKind, Coverage)] = &[
    (LogicalKind::Aggregate, Coverage::RefusedByName),
    (LogicalKind::AntiJoin, Coverage::RefusedByName),
    (LogicalKind::Expand, Coverage::RefusedByName),
    (LogicalKind::Filter, Coverage::Routable),
    (LogicalKind::Join, Coverage::Routable),
    (LogicalKind::Limit, Coverage::Routable),
    (LogicalKind::MergeClassify, Coverage::Routable),
    (LogicalKind::MetadataCount, Coverage::RefusedByName),
    (LogicalKind::Nearest, Coverage::RefusedByName),
    (LogicalKind::OuterReference, Coverage::RefusedByName),
    (LogicalKind::Projection, Coverage::Routable),
    (LogicalKind::RankFuse, Coverage::RefusedByName),
    (LogicalKind::RowDiff, Coverage::Routable),
    (LogicalKind::Sort, Coverage::Routable),
    (LogicalKind::TableScan, Coverage::Routable),
    (LogicalKind::TextSearch, Coverage::RefusedByName),
];

pub fn coverage(kind: LogicalKind) -> Coverage {
    COVERAGE
        .iter()
        .find(|(candidate, _)| *candidate == kind)
        .map_or(Coverage::RefusedByName, |(_, coverage)| *coverage)
}

/// The entry whose shape equals `census`, if any. Entries are pairwise
/// disjoint, so at most one matches; the registry test pins that.
pub fn lookup(census: &Census) -> Option<&'static Entry> {
    REGISTRY.iter().find(|entry| entry.shape.matches(census))
}
