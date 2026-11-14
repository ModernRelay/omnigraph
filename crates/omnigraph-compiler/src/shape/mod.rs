//! A checked declaration as the planner lowers it (docs/rfcs/2026-11-07-gq-3.md,
//! section 5: "A read the type checker accepts, the planner lowers").
//!
//! [`read::ReadShape::of`] decomposes a core read into the steps the logical
//! plan is built from, and [`write::WriteShape::of`] a core write into its
//! statements and the selection reads they run over. These two functions are
//! the one statement of what this release lowers: the type checker runs them
//! on every declaration it accepts and refuses a term they do not lower with
//! `T86` at the construct ([`Unlowered`]), and the planner calls the same
//! functions to build its plans, so a term the type checker accepted that
//! arrives unlowered is a defect (`P006`), never a refusal a query's author
//! sees.

pub mod read;
pub mod write;

/// The stage of the programme (docs/rfcs/2026-11-07-gq-3.md, section 11,
/// "Sequence") that adds the lowering of a term this release's planner
/// refuses. A relation parameter (stage RP) and a path (stage Path) start at
/// tokens this release's parser refuses, so no type-checked term names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgrammeStage {
    /// The break: edition 3 as sections 1 to 7 state it. This release ships
    /// it in part, so a construct of the break that the planner does not
    /// lower yet names it.
    B,
    /// Recursion: the `Fix` evaluator.
    Rec,
}

impl ProgrammeStage {
    /// The stage's name in the RFC's sequence table.
    pub fn code(self) -> &'static str {
        match self {
            Self::B => "B",
            Self::Rec => "Rec",
        }
    }

    /// What the stage is, as a refusal names it.
    pub fn title(self) -> &'static str {
        match self {
            Self::B => "the break",
            Self::Rec => "recursion, the `Fix` evaluator",
        }
    }
}

/// The kind of a `match` element a refusal points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElementKind {
    Let,
    From,
    Optional,
    Binding,
}

/// The written construct a term the planner does not lower came from. The
/// core term carries no source positions; the type checker resolves the
/// construct against the declaration's syntax to place the refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Construct {
    /// The read as a whole.
    Read,
    /// The read's `limit`.
    Limit,
    /// The order key at this position of `order`.
    OrderKey(usize),
    /// The return item at this position of `return`.
    ReturnItem(usize),
    /// The first element of one of these kinds, in written order, and the
    /// name it binds when the term names one.
    Element(&'static [ElementKind], Option<String>),
    /// A block in a `where`.
    Block,
    /// A write group's `match`, by its position.
    Group(usize),
    /// A statement of a write group: the group's and the statement's
    /// positions.
    Statement(usize, usize),
    /// An assignment of a statement: the group's and the statement's
    /// positions, and the property.
    Assignment(usize, usize, String),
}

/// A rewrite with the same rows that this release lowers, where one exists;
/// the type checker writes it against the declaration's syntax.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Repair {
    /// A computed order key: return it under an alias, which the sort reads.
    ReturnTheOrderKey,
}

/// A term this release's planner does not lower: the declaration, what the
/// term is, the construct it was written as, the stage that adds it and the
/// repair, where one exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unlowered {
    pub name: String,
    pub what: String,
    pub construct: Construct,
    /// The write group whose selection read holds the construct; `None` in
    /// a read.
    pub group: Option<usize>,
    pub stage: ProgrammeStage,
    pub repair: Option<Repair>,
}

impl Unlowered {
    /// A term of the break, written as `construct`.
    pub fn new(name: &str, what: impl std::fmt::Display, construct: Construct) -> Self {
        Self {
            name: name.to_string(),
            what: what.to_string(),
            construct,
            group: None,
            stage: ProgrammeStage::B,
            repair: None,
        }
    }

    pub fn stage(mut self, stage: ProgrammeStage) -> Self {
        self.stage = stage;
        self
    }

    pub fn repair(mut self, repair: Repair) -> Self {
        self.repair = Some(repair);
        self
    }

    /// The refusal's message: the declaration, the term and the stage.
    pub fn message(&self) -> String {
        format!(
            "`{}`: this release's planner does not lower {}; stage {} ({}) adds it",
            self.name,
            self.what,
            self.stage.code(),
            self.stage.title()
        )
    }
}

/// Why a checked declaration has no shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShapeError {
    /// A term this release does not lower.
    Unlowered(Box<Unlowered>),
    /// The term is not one the type checker produces: a defect.
    Internal(String),
}

impl ShapeError {
    pub(crate) fn unlowered(u: Unlowered) -> Self {
        Self::Unlowered(Box::new(u))
    }

    /// The refusal with its construct set where the shaper left it at the
    /// whole read.
    pub(crate) fn at(self, construct: Construct) -> Self {
        match self {
            Self::Unlowered(mut u) if u.construct == Construct::Read => {
                u.construct = construct;
                Self::Unlowered(u)
            }
            other => other,
        }
    }
}

impl std::fmt::Display for ShapeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unlowered(u) => f.write_str(&u.message()),
            Self::Internal(detail) => write!(f, "shaping a checked declaration failed: {detail}"),
        }
    }
}

impl std::error::Error for ShapeError {}
