//! The plan budget's meter (the GQ 3 RFC, section 2, `Call` and the plan
//! budget; invariant 11).
//!
//! A declaration's plan budget, `@budget(plan = n)` or
//! [`DEFAULT_PLAN_BUDGET`](crate::ir::core::DEFAULT_PLAN_BUDGET), bounds the
//! work of turning it into a plan, counted in one unit and charged to one
//! counter as the work happens:
//!
//! - every core-term node the type checker builds, an expanded `Call` body
//!   at each call site, an `optional`'s shared pattern once
//!   ([`WorkKind::TermNodes`]);
//! - every expression node those term nodes hold, and every one the shaper
//!   copies into the read's shape ([`WorkKind::ExprNodes`]);
//! - every expression the type checker types, once per time it is typed
//!   ([`WorkKind::Typing`]);
//! - every term node the shaper visits ([`WorkKind::Shaping`]);
//! - every shape step the planner resolves, once per pass it runs, and every
//!   state its plan search explores ([`WorkKind::Planning`]).
//!
//! The counter stops the work the moment it passes the budget, so a refused
//! declaration has done at most its budget's worth of work, plus the one
//! charge that passed it. Every charge is a function of the declaration's
//! text and the schema alone, never of a clock, a machine, a session setting
//! or a snapshot's statistics, so a declaration charges the same amount on
//! every machine and every run, and a replay of its plan agrees.

use std::cell::{Cell, RefCell};
use std::fmt;

/// What a unit of plan-budget work is spent on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WorkKind {
    /// A core-term node built.
    TermNodes,
    /// An expression node built: held by a core-term node or copied into
    /// the read's shape.
    ExprNodes,
    /// An expression typed.
    Typing,
    /// A core-term node the shaper visited.
    Shaping,
    /// A shape step resolved or rewritten by one planner pass, or a state
    /// the plan search explored.
    Planning,
}

impl WorkKind {
    pub const ALL: [WorkKind; 5] = [
        WorkKind::TermNodes,
        WorkKind::ExprNodes,
        WorkKind::Typing,
        WorkKind::Shaping,
        WorkKind::Planning,
    ];

    /// The name a refusal and the explain document print.
    pub const fn name(self) -> &'static str {
        match self {
            Self::TermNodes => "term nodes",
            Self::ExprNodes => "expression nodes",
            Self::Typing => "typing",
            Self::Shaping => "shaping",
            Self::Planning => "planning",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::TermNodes => 0,
            Self::ExprNodes => 1,
            Self::Typing => 2,
            Self::Shaping => 3,
            Self::Planning => 4,
        }
    }
}

/// The work charged so far, by kind.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct WorkSpent {
    pub term_nodes: u64,
    pub expr_nodes: u64,
    pub typing: u64,
    pub shaping: u64,
    pub planning: u64,
}

impl WorkSpent {
    /// Every kind together: what the budget compares.
    pub fn total(&self) -> u64 {
        self.term_nodes
            .saturating_add(self.expr_nodes)
            .saturating_add(self.typing)
            .saturating_add(self.shaping)
            .saturating_add(self.planning)
    }

    pub fn of(&self, kind: WorkKind) -> u64 {
        [
            self.term_nodes,
            self.expr_nodes,
            self.typing,
            self.shaping,
            self.planning,
        ][kind.index()]
    }

    fn add(&mut self, kind: WorkKind, n: u64) {
        let slot = match kind {
            WorkKind::TermNodes => &mut self.term_nodes,
            WorkKind::ExprNodes => &mut self.expr_nodes,
            WorkKind::Typing => &mut self.typing,
            WorkKind::Shaping => &mut self.shaping,
            WorkKind::Planning => &mut self.planning,
        };
        *slot = slot.saturating_add(n);
    }
}

/// A declaration's plan budget and the work charged to it so far: what a
/// later phase resumes from ([`PlanWork::resume`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct WorkRecord {
    pub limit: u64,
    pub spent: WorkSpent,
}

impl WorkRecord {
    /// Nothing charged yet against `limit`.
    pub fn fresh(limit: u64) -> Self {
        Self {
            limit,
            spent: WorkSpent::default(),
        }
    }
}

impl Default for WorkRecord {
    fn default() -> Self {
        Self::fresh(crate::ir::core::DEFAULT_PLAN_BUDGET as u64)
    }
}

/// The work charged to a checked declaration so far, carried beside it for
/// the phases after the type checker. It is not part of the declaration's
/// meaning: two declarations that type to the same term are equal whatever
/// typing them cost.
#[derive(Debug, Clone, Default)]
pub struct WorkSoFar(pub Box<WorkRecord>);

impl WorkSoFar {
    pub fn new(record: WorkRecord) -> Self {
        Self(Box::new(record))
    }
}

impl PartialEq for WorkSoFar {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

/// The work that passed a declaration's plan budget: the construct that
/// charged the most and how much, and the work by kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Overrun {
    pub limit: u64,
    /// Every charge up to and including the one that passed the budget.
    pub spent: WorkSpent,
    /// The construct the most work was charged to, and that work.
    pub construct: String,
    pub construct_charged: u64,
    /// The kind of the charge that passed the budget.
    pub kind: WorkKind,
}

impl fmt::Display for Overrun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kinds: Vec<String> = WorkKind::ALL
            .iter()
            .filter(|k| self.spent.of(**k) > 0)
            .map(|k| format!("{} {}", k.name(), self.spent.of(*k)))
            .collect();
        write!(
            f,
            "compiling it passed its plan budget of {} work units at {} ({}); {} grew the most, charging {}",
            self.limit,
            self.spent.total(),
            kinds.join(", "),
            self.construct,
            self.construct_charged,
        )
    }
}

impl Overrun {
    /// `T70` for the declaration `name` whose compile passed its budget,
    /// expanding the calls `chain` when it did. It prints no fix: the
    /// budget bounds the work a declaration may cost, and a declaration
    /// asking for more is refused, not advised.
    pub fn refusal(&self, name: &str, chain: &[&str]) -> crate::query::diagnostic::QueryDiagnostic {
        crate::query::diagnostic::QueryDiagnostic::typecheck(
            crate::query::codes::T70,
            format!(
                "`{name}` {self}{}",
                if chain.is_empty() {
                    String::new()
                } else {
                    format!(
                        "; the expansion runs through {name} → {}",
                        chain.join(" → ")
                    )
                }
            ),
        )
    }
}

/// The meter one declaration's work is charged to. Interior mutability
/// lets every phase charge through a shared reference.
#[derive(Debug)]
pub struct PlanWork {
    limit: u64,
    spent: Cell<WorkSpent>,
    /// Each construct work was charged to, in first-charged order, with its
    /// work.
    constructs: RefCell<Vec<(String, u64)>>,
    /// The construct charged now: the outermost one entered
    /// ([`PlanWork::enter`]), else the phase's ([`PlanWork::phase`]).
    current: Cell<Option<usize>>,
    phase: Cell<Option<usize>>,
    /// The first charge past the budget.
    overrun: RefCell<Option<Overrun>>,
}

impl PlanWork {
    /// A meter for `limit` work units with nothing charged.
    pub fn new(limit: u64) -> Self {
        Self::resume(WorkRecord::fresh(limit))
    }

    /// A meter continuing from `record`: a later phase charges the same
    /// budget the earlier ones did.
    pub fn resume(record: WorkRecord) -> Self {
        Self {
            limit: record.limit,
            spent: Cell::new(record.spent),
            constructs: RefCell::new(Vec::new()),
            current: Cell::new(None),
            phase: Cell::new(None),
            overrun: RefCell::new(None),
        }
    }

    /// The first charge that passed the budget, if one did.
    pub fn overrun(&self) -> Option<Overrun> {
        self.overrun.borrow().clone()
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    pub fn spent(&self) -> WorkSpent {
        self.spent.get()
    }

    /// The work left before the budget is passed.
    pub fn remaining(&self) -> u64 {
        self.limit.saturating_sub(self.spent.get().total())
    }

    /// The budget and the work charged so far.
    pub fn record(&self) -> WorkRecord {
        WorkRecord {
            limit: self.limit,
            spent: self.spent.get(),
        }
    }

    fn slot(&self, label: String) -> usize {
        let mut constructs = self.constructs.borrow_mut();
        match constructs.iter().position(|(l, _)| *l == label) {
            Some(i) => i,
            None => {
                constructs.push((label, 0));
                constructs.len() - 1
            }
        }
    }

    /// Charge work to `label` while no outer construct is being charged;
    /// the returned token restores the meter ([`PlanWork::leave`]). The
    /// outermost construct is the one a refusal names: the written element
    /// whose work grew.
    pub fn enter(&self, label: impl FnOnce() -> String) -> bool {
        if self.current.get().is_some() {
            return false;
        }
        let at = self.slot(label());
        self.current.set(Some(at));
        true
    }

    /// Undo an [`PlanWork::enter`] that returned `entered`.
    pub fn leave(&self, entered: bool) {
        if entered {
            self.current.set(None);
        }
    }

    /// Charge work no written construct holds to `label` from now on.
    pub fn phase(&self, label: impl Into<String>) {
        let at = self.slot(label.into());
        self.phase.set(Some(at));
    }

    /// Charge `n` units of `kind`; past the budget, the overrun, and the
    /// caller stops.
    pub fn charge(&self, kind: WorkKind, n: usize) -> Result<(), Box<Overrun>> {
        if n == 0 {
            return Ok(());
        }
        let n = n as u64;
        let mut spent = self.spent.get();
        spent.add(kind, n);
        self.spent.set(spent);
        let at = match self.current.get().or(self.phase.get()) {
            Some(at) => at,
            None => {
                let at = self.slot("the declaration".to_string());
                self.phase.set(Some(at));
                at
            }
        };
        {
            let mut constructs = self.constructs.borrow_mut();
            constructs[at].1 = constructs[at].1.saturating_add(n);
        }
        if spent.total() <= self.limit {
            return Ok(());
        }
        let constructs = self.constructs.borrow();
        // The first construct with the most work: a deterministic choice.
        let (construct, construct_charged) = constructs
            .iter()
            .fold(None::<&(String, u64)>, |best, c| match best {
                Some(b) if b.1 >= c.1 => Some(b),
                _ => Some(c),
            })
            .map(|(l, w)| (l.clone(), *w))
            .expect("a charge names its construct");
        let overrun = Overrun {
            limit: self.limit,
            spent,
            construct,
            construct_charged,
            kind,
        };
        self.overrun
            .borrow_mut()
            .get_or_insert_with(|| overrun.clone());
        Err(Box::new(overrun))
    }

    /// Charge `states` states a plan search explored ([`WorkKind::Planning`]).
    /// Each candidate a planner rule considers is one state, so a search
    /// stops at the budget rather than after enumerating every candidate.
    pub fn explore(&self, states: usize) -> Result<(), Box<Overrun>> {
        self.charge(WorkKind::Planning, states)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_charge_past_the_limit_is_an_overrun_naming_the_outermost_construct() {
        let work = PlanWork::new(10);
        work.phase("the match");
        work.charge(WorkKind::TermNodes, 2).unwrap();
        let outer = work.enter(|| "the `optional` at 3:5".to_string());
        let inner = work.enter(|| "the inner binding".to_string());
        assert!(outer && !inner);
        work.charge(WorkKind::Typing, 6).unwrap();
        work.leave(inner);
        let overrun = work.charge(WorkKind::ExprNodes, 3).unwrap_err();
        work.leave(outer);
        assert_eq!(overrun.spent.total(), 11);
        assert_eq!(overrun.construct, "the `optional` at 3:5");
        assert_eq!(overrun.construct_charged, 9);
        assert_eq!(overrun.kind, WorkKind::ExprNodes);
        assert!(
            overrun
                .to_string()
                .contains("term nodes 2, expression nodes 3, typing 6")
        );
    }

    #[test]
    fn a_resumed_meter_charges_the_same_budget() {
        let first = PlanWork::new(5);
        first.charge(WorkKind::Shaping, 4).unwrap();
        let later = PlanWork::resume(first.record());
        later.explore(1).unwrap();
        assert!(later.explore(1).is_err());
        assert_eq!(later.spent().planning, 2);
    }
}
