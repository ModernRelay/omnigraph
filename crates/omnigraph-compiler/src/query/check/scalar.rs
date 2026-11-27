//! The checker's working form of a scalar expression: an operand with its
//! core type ([`Ty`], which keeps a reference's nullability), built while the
//! checker resolves names and types. It is never stored: every expression a
//! term holds is the bound [`IRExpr`] that [`SExpr::to_ir`] gives.

use crate::ir::core::Ty;
use crate::ir::expr::{IRExpr, ScalarFunc, TermsMode};
use crate::query::syntax::ArithOp;
use crate::query::value::{BinaryOp, CompOp, Literal, NOW_PARAM_NAME};
use crate::types::ExprType;

/// A typed scalar expression: the operand language of Filter, Project, Group
/// and Order. `Clone`, `PartialEq` and `Debug` are written out only so that
/// each level of a deep expression enters through [`crate::stack::deep`].
pub struct SExpr {
    pub kind: SKind,
    pub ty: Ty,
    /// Arithmetic and registry rows declared `can_fail`; the planner keeps the
    /// term behind its guards (A3).
    pub can_fail: bool,
}

/// A deep tree is taken apart child by child onto a heap stack, so dropping
/// it never recurses once per level (see [`crate::ir::IRExpr`]'s `Drop`).
impl Drop for SExpr {
    fn drop(&mut self) {
        let mut pending: Vec<SExpr> = Vec::new();
        self.take_children(&mut pending);
        while let Some(mut child) = pending.pop() {
            child.take_children(&mut pending);
        }
    }
}

impl SExpr {
    /// Every child of the node with children of its own moved into `out`, a
    /// leaf in its place.
    fn take_children(&mut self, out: &mut Vec<SExpr>) {
        let mut take = |child: &mut SExpr| {
            if !child.kind.is_leaf() {
                let leaf = SExpr {
                    kind: SKind::Null,
                    ty: child.ty.clone(),
                    can_fail: false,
                };
                out.push(std::mem::replace(child, leaf));
            }
        };
        match &mut self.kind {
            SKind::Arith { left, right, .. }
            | SKind::Cmp { left, right, .. }
            | SKind::In {
                needle: left,
                list: right,
            }
            | SKind::And(left, right)
            | SKind::Or(left, right) => {
                take(left);
                take(right);
            }
            SKind::Neg(child)
            | SKind::IsNull { expr: child, .. }
            | SKind::Not(child)
            | SKind::Cast(child) => take(child),
            SKind::Call { args, options, .. } => {
                args.iter_mut().for_each(&mut take);
                options.iter_mut().for_each(|(_, option)| take(option));
            }
            SKind::Lit(_)
            | SKind::Null
            | SKind::Param(_)
            | SKind::Col(_)
            | SKind::Prop { .. }
            | SKind::Meta { .. } => {}
        }
    }
}

impl SKind {
    /// Whether the node has no child expression.
    fn is_leaf(&self) -> bool {
        matches!(
            self,
            Self::Lit(_)
                | Self::Null
                | Self::Param(_)
                | Self::Col(_)
                | Self::Prop { .. }
                | Self::Meta { .. }
        )
    }
}

impl Clone for SExpr {
    fn clone(&self) -> Self {
        crate::stack::deep(|| Self {
            kind: self.kind.clone(),
            ty: self.ty.clone(),
            can_fail: self.can_fail,
        })
    }
}

impl PartialEq for SExpr {
    fn eq(&self, other: &Self) -> bool {
        crate::stack::deep(|| {
            self.kind == other.kind && self.ty == other.ty && self.can_fail == other.can_fail
        })
    }
}

impl std::fmt::Debug for SExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        crate::stack::deep(|| {
            f.debug_struct("SExpr")
                .field("kind", &self.kind)
                .field("ty", &self.ty)
                .field("can_fail", &self.can_fail)
                .finish()
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SKind {
    Lit(Literal),
    /// `null`, admitted only as an assignment value.
    Null,
    /// A declared parameter of the innermost declaration (a `Call` frame binds
    /// it; it is never textually substituted).
    Param(String),
    /// A column of the current row.
    Col(String),
    /// A property of a reference column.
    Prop {
        col: String,
        prop: String,
    },
    /// A meta-field (`@id`, `@src`, `@dst`, `@type`) of a reference column.
    Meta {
        col: String,
        field: String,
    },
    Arith {
        op: ArithOp,
        left: Box<SExpr>,
        right: Box<SExpr>,
    },
    Neg(Box<SExpr>),
    Cmp {
        op: CompOp,
        left: Box<SExpr>,
        right: Box<SExpr>,
    },
    In {
        needle: Box<SExpr>,
        list: Box<SExpr>,
    },
    IsNull {
        expr: Box<SExpr>,
        negated: bool,
    },
    Not(Box<SExpr>),
    And(Box<SExpr>, Box<SExpr>),
    Or(Box<SExpr>, Box<SExpr>),
    /// A registry scalar row, its options in registry order with defaults
    /// filled (so a spelled default and an omitted one are one term).
    Call {
        func: String,
        args: Vec<SExpr>,
        options: Vec<(String, SExpr)>,
    },
    /// An explicit numeric conversion (comparison casts stay explicit, as
    /// today; arithmetic widening is one too).
    Cast(Box<SExpr>),
}

impl SExpr {
    pub fn new(kind: SKind, ty: Ty) -> Self {
        let can_fail = match &kind {
            SKind::Arith { .. } => true,
            // A negated literal's value is known: it fails only where the
            // negation leaves its type.
            SKind::Neg(inner) => match (&inner.kind, &ty) {
                (SKind::Lit(Literal::Float(_)), _) => false,
                (SKind::Lit(Literal::Integer(n)), Ty::Value(t)) => {
                    !crate::ir::expr::negation_fits(*n, &crate::types::ExprType::from_prop(t))
                }
                _ => true,
            },
            SKind::Cmp { left, right, .. }
            | SKind::And(left, right)
            | SKind::Or(left, right)
            | SKind::In {
                needle: left,
                list: right,
            } => left.can_fail || right.can_fail,
            SKind::Not(e) | SKind::Cast(e) | SKind::IsNull { expr: e, .. } => e.can_fail,
            SKind::Call {
                func,
                args,
                options,
            } => {
                args.iter().any(|a| a.can_fail)
                    || options.iter().any(|(_, a)| a.can_fail)
                    || ScalarFunc::from_name(func).is_some_and(|f| {
                        let arg_types: Vec<_> = args.iter().map(|a| a.ty.expr_type()).collect();
                        f.row_can_fail(&arg_types, &ty.expr_type())
                    })
            }
            SKind::Lit(_)
            | SKind::Null
            | SKind::Param(_)
            | SKind::Col(_)
            | SKind::Prop { .. }
            | SKind::Meta { .. } => false,
        };
        Self { kind, ty, can_fail }
    }

    /// The columns the expression reads.
    pub fn columns(&self, out: &mut Vec<String>) {
        crate::stack::deep(|| match &self.kind {
            SKind::Col(c) | SKind::Prop { col: c, .. } | SKind::Meta { col: c, .. } => {
                if !out.contains(c) {
                    out.push(c.clone());
                }
            }
            SKind::Arith { left, right, .. }
            | SKind::Cmp { left, right, .. }
            | SKind::And(left, right)
            | SKind::Or(left, right)
            | SKind::In {
                needle: left,
                list: right,
            } => {
                left.columns(out);
                right.columns(out);
            }
            SKind::Neg(e) | SKind::Not(e) | SKind::Cast(e) | SKind::IsNull { expr: e, .. } => {
                e.columns(out)
            }
            SKind::Call { args, options, .. } => {
                for a in args {
                    a.columns(out);
                }
                for (_, a) in options {
                    a.columns(out);
                }
            }
            SKind::Lit(_) | SKind::Null | SKind::Param(_) => {}
        })
    }

    pub fn is_reference_read(&self) -> bool {
        matches!(
            self.kind,
            SKind::Col(_) | SKind::Prop { .. } | SKind::Meta { .. }
        )
    }
}

impl SExpr {
    /// The bound expression a term stores. A column read is a `Variable` when
    /// it holds a reference and an `AliasRef` when it holds a value; a meta
    /// field is a `PropAccess` of its `@` name; `in` is membership with the
    /// list on the left; a comparison row (`contains`, `starts_with`), a
    /// search predicate and `now()` take their own nodes, and the other
    /// registry rows a typed `Call`.
    pub fn to_ir(&self) -> IRExpr {
        crate::stack::deep(|| {
            let ty = self.ty.expr_type();
            let boxed = |e: &SExpr| Box::new(e.to_ir());
            match &self.kind {
                SKind::Lit(literal) => IRExpr::Literal(literal.clone(), ty),
                SKind::Null => IRExpr::Literal(Literal::Null, ty),
                SKind::Param(name) => IRExpr::Param(name.clone(), ty),
                SKind::Col(name) if self.ty.is_reference() => IRExpr::Variable(name.clone(), ty),
                SKind::Col(name) => IRExpr::AliasRef(name.clone(), ty),
                SKind::Prop { col, prop } => IRExpr::PropAccess {
                    variable: col.clone(),
                    property: prop.clone(),
                    ty,
                },
                SKind::Meta { col, field } => IRExpr::PropAccess {
                    variable: col.clone(),
                    property: if field.starts_with('@') {
                        field.clone()
                    } else {
                        format!("@{field}")
                    },
                    ty,
                },
                SKind::Arith { op, left, right } => IRExpr::Binary {
                    left: boxed(left),
                    op: BinaryOp::Arith(*op),
                    right: boxed(right),
                    ty,
                },
                SKind::Neg(e) => IRExpr::Neg { expr: boxed(e), ty },
                SKind::Cmp { op, left, right } => IRExpr::Binary {
                    left: boxed(left),
                    op: BinaryOp::Compare(*op),
                    right: boxed(right),
                    ty,
                },
                SKind::In { needle, list } => IRExpr::Binary {
                    left: boxed(list),
                    op: BinaryOp::Compare(CompOp::Contains),
                    right: boxed(needle),
                    ty,
                },
                SKind::IsNull { expr, negated } => IRExpr::IsNull {
                    expr: boxed(expr),
                    negated: *negated,
                    ty,
                },
                SKind::Not(e) => IRExpr::Not(boxed(e), ty),
                SKind::And(left, right) => IRExpr::Binary {
                    left: boxed(left),
                    op: BinaryOp::And,
                    right: boxed(right),
                    ty,
                },
                SKind::Or(left, right) => IRExpr::Binary {
                    left: boxed(left),
                    op: BinaryOp::Or,
                    right: boxed(right),
                    ty,
                },
                SKind::Cast(e) => IRExpr::Cast { expr: boxed(e), ty },
                SKind::Call {
                    func,
                    args,
                    options,
                } => call_to_ir(func, args, options, ty),
            }
        })
    }
}

fn call_to_ir(func: &str, args: &[SExpr], options: &[(String, SExpr)], ty: ExprType) -> IRExpr {
    let option = |name: &str| {
        options
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, value)| value)
    };
    let pair = || {
        let [field, query] = args else {
            unreachable!("the checker gives `{func}` two arguments")
        };
        (Box::new(field.to_ir()), Box::new(query.to_ir()))
    };
    match func {
        "contains" | "starts_with" => {
            let (left, right) = pair();
            IRExpr::Binary {
                left,
                op: BinaryOp::Compare(if func == "contains" {
                    CompOp::StringContains
                } else {
                    CompOp::StartsWith
                }),
                right,
                ty,
            }
        }
        "now" => IRExpr::Param(NOW_PARAM_NAME.to_string(), ty),
        "match_terms" => {
            let mode = match option("mode").map(|e| &e.kind) {
                Some(SKind::Lit(Literal::String(mode))) if mode == "all" => TermsMode::All,
                _ => TermsMode::Any,
            };
            let mut ir: Vec<IRExpr> = args.iter().map(SExpr::to_ir).collect();
            ir.extend(option("max_edits").map(SExpr::to_ir));
            IRExpr::Call {
                func: ScalarFunc::MatchTerms { mode },
                args: ir,
                ty,
            }
        }
        // `round`'s `digits`, filled with its default, is its second argument.
        other => match ScalarFunc::from_name(other) {
            Some(func) => IRExpr::Call {
                func,
                args: args
                    .iter()
                    .chain(option("digits"))
                    .map(SExpr::to_ir)
                    .collect(),
                ty,
            },
            None => unreachable!("the checker admits no scalar row `{other}`"),
        },
    }
}

/// A `terms(…)` value as the checker builds it: the text, the mode and the
/// edit budget, defaults filled.
#[derive(Debug, Clone, PartialEq)]
pub struct Terms {
    pub text: SExpr,
    pub mode_all: bool,
    pub max_edits: SExpr,
}

impl Terms {
    /// The lexical query a `bm25` score stores.
    pub fn lexical(&self) -> crate::ir::core::LexicalQuery {
        crate::ir::core::LexicalQuery {
            text: self.text.to_ir(),
            mode_all: self.mode_all,
            max_edits: self.max_edits.to_ir(),
        }
    }
}
