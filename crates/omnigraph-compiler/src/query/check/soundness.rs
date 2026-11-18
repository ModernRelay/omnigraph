//! The refusals the type checker justifies by a claim about every graph
//! that conforms to the schema, and a test-only switch that lets each one
//! through, so a law can check the claim (`omnigraph-gqt`
//! `tests/integration/law_refusal_soundness.rs`).
//!
//! With a refusal let through, the checker types the term as the claim
//! reads it, never as an edition-2 reading would: a comparison with `null`
//! compares with a null of the other operand's type; a literal outside its
//! property's domain is compared as written; a reversed triple traverses in
//! its written order; a hop range past one hop over an edge between two
//! types keeps its bounds; and two references of different types are
//! compared.
//!
//! The switch exists only in test builds (`cfg(test)` or the `test-util`
//! feature, which only dev-dependencies enable); in a release build every
//! refusal applies and the check folds to a constant.

/// One refusal whose rule rests on a claim about every conforming graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Claim {
    /// `T7`: "a comparison with `null` is null on every row".
    NullComparison,
    /// `T3`: a literal outside its property's domain. The RFC's reason is
    /// misspelling, refused under every operator; the claim this law checks,
    /// that the comparison returns no rows, holds under `=` and `in` only.
    LiteralOutsideDomain,
    /// `T82`: a triple written against its edge's declared direction, or a
    /// selection none of whose members points along the written order; the
    /// written order is the direction, so it matches no edge.
    ReversedTriple,
    /// `T5`: a hop range starting past one hop over an edge between two
    /// node types: "no path reaches hop n, so this pattern matches nothing".
    HopPastDistinctEndpoints,
    /// `T7`: "`A` and `B` references are never equal".
    ReferencesOfTwoTypes,
}

impl Claim {
    pub const ALL: [Claim; 5] = [
        Claim::NullComparison,
        Claim::LiteralOutsideDomain,
        Claim::ReversedTriple,
        Claim::HopPastDistinctEndpoints,
        Claim::ReferencesOfTwoTypes,
    ];
}

#[cfg(any(test, feature = "test-util"))]
thread_local! {
    static LET_THROUGH: std::cell::Cell<Option<Claim>> = const { std::cell::Cell::new(None) };
}

/// Whether the refusal resting on `claim` applies.
#[inline]
pub(crate) fn refuses(claim: Claim) -> bool {
    #[cfg(any(test, feature = "test-util"))]
    {
        LET_THROUGH.with(|c| c.get()) != Some(claim)
    }
    #[cfg(not(any(test, feature = "test-util")))]
    {
        let _ = claim;
        true
    }
}

/// Runs `f` with the refusal resting on `claim` let through on this
/// thread; every other refusal still applies.
#[cfg(any(test, feature = "test-util"))]
pub fn with_refusal_let_through<R>(claim: Claim, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<Claim>);
    impl Drop for Restore {
        fn drop(&mut self) {
            LET_THROUGH.with(|c| c.set(self.0));
        }
    }
    let _restore = Restore(LET_THROUGH.with(|c| c.replace(Some(claim))));
    f()
}
