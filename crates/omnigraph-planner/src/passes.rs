//! The pass registration: every planner pass, in the order explain reports
//! them. The rewrite stage, the physical stage and the asynchronous scan
//! finalization each iterate their passes from this list and match on the
//! id to run one, so a pass cannot run, report or be catalogued under
//! another name or position; `@Pass` reads the same list.

/// The stage a pass belongs to, which decides which loop dispatches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassStage {
    /// Stage 1, over the logical plan.
    Rewrite,
    /// Stages 2 and 3, physical selection and reporting.
    Physical,
    /// The awaited scan-access finalization after physical selection.
    Finalize,
}

macro_rules! passes {
    ( $( $variant:ident => $name:literal, $stage:ident; )* ) => {
        /// Every planner pass, in canonical reporting order.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum PassId {
            $( $variant, )*
        }

        impl PassId {
            /// Every pass, in the order explain reports them; a run's
            /// `passes` list is a subsequence of this.
            pub const ALL: &'static [PassId] = &[ $( PassId::$variant, )* ];

            /// The name explain and `@Pass` print.
            pub const fn name(self) -> &'static str {
                match self { $( Self::$variant => $name, )* }
            }

            pub const fn stage(self) -> PassStage {
                match self { $( Self::$variant => PassStage::$stage, )* }
            }
        }
    };
}

passes! {
    Resolve => "resolve", Rewrite;
    PredicatePushdown => "predicate_pushdown", Rewrite;
    AggregatePushdown => "aggregate_pushdown", Rewrite;
    ProjectionPushdown => "projection_pushdown", Rewrite;
    FragmentScope => "fragment_scope", Physical;
    AddressShortCircuit => "address_short_circuit", Physical;
    LateMaterialization => "late_materialization", Physical;
    JoinAlgorithm => "join_algorithm", Physical;
    ExpandMode => "expand_mode", Physical;
    AccessPath => "access_path", Physical;
    AntiJoinStrategy => "anti_join_strategy", Physical;
    KeyToId => "key_to_id", Finalize;
    ScanAccess => "scan_access", Finalize;
}

impl PassId {
    /// The zero-based canonical position, the `@Pass.position` column.
    pub fn position(self) -> usize {
        PassId::ALL
            .iter()
            .position(|pass| *pass == self)
            .expect("every pass is in ALL")
    }

    /// The passes of one stage, in canonical order.
    pub fn of_stage(stage: PassStage) -> impl Iterator<Item = PassId> {
        PassId::ALL
            .iter()
            .copied()
            .filter(move |pass| pass.stage() == stage)
    }

    /// The pass a reported name denotes.
    pub fn parse(name: &str) -> Option<PassId> {
        PassId::ALL.iter().copied().find(|pass| pass.name() == name)
    }
}

/// One `@Pass` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassRow {
    pub name: &'static str,
    pub position: usize,
}

/// The `@Pass` rows, one per registered pass in canonical order.
pub fn pass_rows() -> Vec<PassRow> {
    PassId::ALL
        .iter()
        .map(|pass| PassRow {
            name: pass.name(),
            position: pass.position(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_distinct_and_positions_are_indices() {
        for (index, pass) in PassId::ALL.iter().enumerate() {
            assert_eq!(pass.position(), index);
            assert_eq!(PassId::parse(pass.name()), Some(*pass));
        }
        let mut names: Vec<_> = PassId::ALL.iter().map(|pass| pass.name()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), PassId::ALL.len());
    }

    #[test]
    fn stages_are_contiguous_in_reporting_order() {
        let stages: Vec<_> = PassId::ALL.iter().map(|pass| pass.stage()).collect();
        let mut seen = Vec::new();
        for stage in stages {
            if seen.last() != Some(&stage) {
                assert!(!seen.contains(&stage), "stage {stage:?} is split");
                seen.push(stage);
            }
        }
        assert_eq!(
            seen,
            [PassStage::Rewrite, PassStage::Physical, PassStage::Finalize]
        );
    }
}
