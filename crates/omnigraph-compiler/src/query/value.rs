//! The values and operators the surface tree and the bound expression
//! share: literals, comparison and binary operators, aggregates, and the
//! reserved names of the parameter `now()` reads and of the score columns.

use std::fmt::Write as _;

use crate::types::{ExprType, ScalarType};

pub const NOW_PARAM_NAME: &str = "__nanograph_now";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    /// The parser emits `Contains` for the `contains` keyword; typecheck
    /// accepts a list left operand (membership) or a scalar String left
    /// operand (substring), and lowering resolves the String form to
    /// `StringContains` so downstream consumers never re-derive types.
    Contains,
    /// Exact, case-sensitive prefix match on a scalar String property.
    StartsWith,
    /// Exact, case-sensitive substring match on a scalar String property.
    /// Never produced by the parser — lowering resolves it from `Contains`.
    StringContains,
}

impl std::fmt::Display for CompOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::Ne => write!(f, "!="),
            Self::Gt => write!(f, ">"),
            Self::Lt => write!(f, "<"),
            Self::Ge => write!(f, ">="),
            Self::Le => write!(f, "<="),
            Self::Contains | Self::StringContains => write!(f, "contains"),
            Self::StartsWith => write!(f, "starts_with"),
        }
    }
}

/// The operator of a binary expression node: a comparison, a Boolean
/// combination of two Boolean operands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    Compare(CompOp),
    And,
    Or,
    /// `+ - * / %` over one numeric type; the result has the operands' type.
    Arith(super::syntax::ArithOp),
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compare(op) => write!(f, "{op}"),
            Self::And => f.write_str("and"),
            Self::Or => f.write_str("or"),
            Self::Arith(op) => f.write_str(op.symbol()),
        }
    }
}

/// Binding strength of an expression's root, for printing with minimal
/// parentheses: `or` < `and` < `not` < comparison and null test < atom.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Precedence {
    Or,
    And,
    Not,
    Comparison,
    Sum,
    Product,
    Negation,
    Atom,
}

/// Lance's search output columns, appended under the target's prefix
/// (`{var}._distance` for `nearest`, `{var}._score` for `bm25`) and reserved
/// by `schema::is_reserved_search_output_column`.
pub const DISTANCE_COLUMN: &str = "_distance";

/// See [`DISTANCE_COLUMN`].
pub const SCORE_COLUMN: &str = "_score";

/// The fused score a `Fuse` appends under its target's binding
/// (`{var}._rrf`), reserved like the two above.
pub const RRF_COLUMN: &str = "_rrf";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// The list aggregate: a group's non-null values in the fixed value
    /// order. A reference argument waits for stage Path, which admits
    /// references as list elements.
    Collect,
    /// The sample standard deviation of a group's non-null numeric values:
    /// `F64`, dividing by n − 1, null over fewer than two values.
    Stddev,
    /// `percentile(x, p)`: the exact percentile of a group's non-null
    /// numeric values with linear interpolation, `F64`, null over empty
    /// input. Its `p`, a literal or a parameter in [0, 1], is the
    /// aggregate's fraction ([`AggFunc::takes_fraction`]).
    Percentile,
}

impl AggFunc {
    /// The aggregate's signature result for an accepted argument: nullable,
    /// since position determines nullability (`count` and `collect` are never
    /// null where they are read).
    pub fn result_type(self, arg: &ExprType) -> Option<ExprType> {
        if matches!(arg, ExprType::Value { scalar: ScalarType::Vector(dim), .. }
            if *dim == 0 || i32::try_from(*dim).is_err())
        {
            return None;
        }
        let value = |scalar: ScalarType| ExprType::Value {
            scalar,
            list: false,
            nullable: true,
        };
        match (self, arg) {
            (Self::Count, _) => Some(value(ScalarType::I64)),
            (
                Self::Sum | Self::Avg,
                ExprType::Value {
                    scalar,
                    list: false,
                    nullable: _,
                },
            ) => scalar.is_numeric().then_some(value(ScalarType::F64)),
            (
                Self::Stddev | Self::Percentile,
                ExprType::Value {
                    scalar,
                    list: false,
                    nullable: _,
                },
            ) => scalar.is_numeric().then_some(value(ScalarType::F64)),
            (
                Self::Min | Self::Max,
                ExprType::Value {
                    scalar,
                    list: false,
                    nullable: _,
                },
            ) => scalar.is_orderable().then_some(value(*scalar)),
            (
                Self::Collect,
                ExprType::Value {
                    scalar,
                    list: false,
                    nullable: _,
                },
            ) => scalar.is_list_element().then_some(ExprType::Value {
                scalar: *scalar,
                list: true,
                nullable: true,
            }),
            (
                Self::Sum
                | Self::Avg
                | Self::Min
                | Self::Max
                | Self::Collect
                | Self::Stddev
                | Self::Percentile,
                ExprType::Value {
                    scalar: _,
                    list: true,
                    nullable: _,
                }
                | ExprType::Node { type_name: _ }
                | ExprType::Edge { type_name: _ }
                | ExprType::ExactInteger { .. },
            ) => None,
        }
    }

    /// Whether the aggregate's value over empty input is a value rather than
    /// null: `count` is 0 and `collect` the empty list.
    pub fn defined_on_empty(self) -> bool {
        matches!(self, Self::Count | Self::Collect)
    }

    /// Whether the aggregate takes a fraction beside its argument:
    /// `percentile`'s `p`.
    pub fn takes_fraction(self) -> bool {
        self == Self::Percentile
    }

    /// Whether the aggregate holds a group's values to finish them in the
    /// fixed value order (`collect`, `stddev`, `percentile`), rather than a
    /// running total a correlated block's semi-join folds.
    pub fn holds_values(self) -> bool {
        matches!(self, Self::Collect | Self::Stddev | Self::Percentile)
    }
}

impl std::fmt::Display for AggFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count => write!(f, "count"),
            Self::Sum => write!(f, "sum"),
            Self::Avg => write!(f, "avg"),
            Self::Min => write!(f, "min"),
            Self::Max => write!(f, "max"),
            Self::Collect => write!(f, "collect"),
            Self::Stddev => write!(f, "stddev"),
            Self::Percentile => write!(f, "percentile"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Literal {
    Null,
    String(String),
    /// An integer value, exact for every declared integer type: `I32`, `I64`,
    /// `U32` and `U64` share the one spelling, and a value is narrowed only at
    /// a typed boundary, from the declared type.
    Integer(i128),
    Float(f64),
    Bool(bool),
    Date(String),
    DateTime(String),
    List(Vec<Literal>),
}

/// Equality and hashing by value, with `Float` compared through
/// `f64::to_bits`: `0.0` and `-0.0` are distinct and `NaN` equals itself, so
/// the derived `Eq` and `Hash` of every expression type hold.
impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Literal::Null, Literal::Null) => true,
            (Literal::String(a), Literal::String(b)) => a == b,
            (Literal::Integer(a), Literal::Integer(b)) => a == b,
            (Literal::Float(a), Literal::Float(b)) => a.to_bits() == b.to_bits(),
            (Literal::Bool(a), Literal::Bool(b)) => a == b,
            (Literal::Date(a), Literal::Date(b)) => a == b,
            (Literal::DateTime(a), Literal::DateTime(b)) => a == b,
            (Literal::List(a), Literal::List(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Literal {}

impl std::hash::Hash for Literal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Literal::Null => {}
            Literal::String(text) | Literal::Date(text) | Literal::DateTime(text) => {
                text.hash(state)
            }
            Literal::Integer(value) => value.hash(state),
            Literal::Float(value) => value.to_bits().hash(state),
            Literal::Bool(value) => value.hash(state),
            Literal::List(items) => items.hash(state),
        }
    }
}

/// The literal as explain text. It is the spelling the parser accepts except
/// for `null`, a negative number and a non-finite float, which GQ has no
/// literal for. A float prints in fixed notation with a decimal point.
impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Null => f.write_str("null"),
            Literal::String(text) => write_gq_string(f, text),
            Literal::Integer(value) => write!(f, "{value}"),
            Literal::Float(value) => {
                let text = value.to_string();
                f.write_str(&text)?;
                if value.is_finite() && !text.contains('.') {
                    f.write_str(".0")?;
                }
                Ok(())
            }
            Literal::Bool(value) => write!(f, "{value}"),
            Literal::Date(text) => {
                f.write_str("date(")?;
                write_gq_string(f, text)?;
                f.write_str(")")
            }
            Literal::DateTime(text) => {
                f.write_str("datetime(")?;
                write_gq_string(f, text)?;
                f.write_str(")")
            }
            Literal::List(items) => {
                f.write_str("[")?;
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{item}")?;
                }
                f.write_str("]")
            }
        }
    }
}

/// A GQ string literal: double-quoted, with the escapes
/// [`crate::error::decode_string_literal`] decodes (`\"`, `\\`, `\n`, `\r`,
/// `\t`), so the text stays on one line.
fn write_gq_string(f: &mut std::fmt::Formatter<'_>, text: &str) -> std::fmt::Result {
    f.write_char('"')?;
    for character in text.chars() {
        match character {
            '"' => f.write_str("\\\"")?,
            '\\' => f.write_str("\\\\")?,
            '\n' => f.write_str("\\n")?,
            '\r' => f.write_str("\\r")?,
            '\t' => f.write_str("\\t")?,
            other => f.write_char(other)?,
        }
    }
    f.write_char('"')
}
