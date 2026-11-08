//! The routing registry for change-feed and merge operation shapes. An
//! entry requires integrated engine execution and checked-in differential
//! evidence for rows, order and errors. Read queries bypass this registry.

use serde::Serialize;

pub use crate::logical::Coverage;
use crate::logical::{Census, JoinKind, LogicalKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Route {
    Executor,
    Planner,
    PlannerBehindFlag,
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

/// The coverage a kind declares beside its name (`logical_kinds!`); not a
/// registry key space: a kind is not a shape.
pub fn coverage(kind: LogicalKind) -> Coverage {
    kind.coverage()
}

/// The entry whose shape equals `census`, if any. Entries are pairwise
/// disjoint, so at most one matches; the registry test pins that.
pub fn lookup(census: &Census) -> Option<&'static Entry> {
    REGISTRY.iter().find(|entry| entry.shape.matches(census))
}
