//! The bound form of every expression a checked declaration holds: a
//! property leaf on its physical column (`@id`, `@src`, `@dst` through the
//! catalog's system columns; a single-type edge's `@type` its name), every
//! comparison, membership and Boolean node built through [`coerce`], which
//! stores the explicit casts of invariant 9, and literal-only nodes folded
//! ([`fold`]). The type checker types; this pass binds what it typed, so
//! the planner, the engine and the bound-plan mirror read one form.

use std::collections::HashMap;

use super::literal::literal_type;
use crate::catalog::Catalog;
use crate::catalog::schema_ir::SYSTEM_COLUMNS_META;
use crate::error::Result;
use crate::ir::core::{AssignValue, CoreDecl, LexicalQuery, ScoreQuery, Sink, Term};
use crate::ir::expr::IRExpr;
use crate::ir::{coerce, fold};
use crate::query::value::{BinaryOp, CompOp, Literal};
use crate::traversal::{EDGE_TYPE_COLUMN, EDGE_TYPE_META, EdgeSelection};
use crate::types::ExprType;

/// What a variable of the declaration binds: a node, or an edge of these
/// types.
#[derive(Debug, Clone)]
enum Bound {
    Node,
    Edge(Vec<String>),
}

struct Binder<'a> {
    catalog: &'a Catalog,
    bound: HashMap<String, Bound>,
}

/// Bind every expression of `decl` in place.
pub(super) fn bind_decl(catalog: &Catalog, decl: &mut CoreDecl) -> Result<()> {
    let mut binder = Binder {
        catalog,
        bound: HashMap::new(),
    };
    match decl {
        CoreDecl::Read(read) => {
            binder.collect(&read.term);
            binder.term(&mut read.term)
        }
        CoreDecl::Write(write) => {
            for group in &write.groups {
                binder.collect(&group.input);
            }
            for group in &mut write.groups {
                binder.term(&mut group.input)?;
                for sink in &mut group.sinks {
                    let assignments = match sink {
                        Sink::Insert { assignments, .. } | Sink::Update { assignments, .. } => {
                            assignments
                        }
                        Sink::Delete { .. } => continue,
                    };
                    for (_, value) in assignments {
                        if let AssignValue::Expr(e) = value {
                            *e = binder.expr(e)?;
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

impl Binder<'_> {
    /// Every variable the term binds: a scanned node or edge type, an
    /// expansion's destination and its edge.
    fn collect(&mut self, term: &Term) {
        match term {
            Term::Scan { var, ty } => {
                let bound = match ty {
                    crate::ir::core::Ty::Edge { type_name, .. } => {
                        Bound::Edge(type_name.split('|').map(str::to_string).collect())
                    }
                    _ => Bound::Node,
                };
                self.bound.insert(var.clone(), bound);
            }
            Term::Expand {
                dst,
                edges,
                edge_var,
                ..
            } => {
                self.bound.insert(dst.clone(), Bound::Node);
                if let Some(e) = edge_var {
                    self.bound.insert(e.clone(), Bound::Edge(edge_types(edges)));
                }
            }
            _ => {}
        }
        for child in term.children() {
            self.collect(child);
        }
    }

    fn term(&self, term: &mut Term) -> Result<()> {
        match term {
            Term::Filter { pred, .. } => *pred = search_compared(self.expr(pred)?),
            Term::Project { items, .. } => {
                for (_, e) in items {
                    *e = self.expr(e)?;
                }
            }
            Term::Group { keys, aggs, .. } => {
                for (_, e) in keys {
                    *e = self.expr(e)?;
                }
                for (_, agg) in aggs {
                    if let Some(arg) = &mut agg.arg {
                        *arg = self.expr(arg)?;
                    }
                }
            }
            Term::Order { keys, .. } => {
                for (e, _) in keys {
                    *e = self.expr(e)?;
                }
            }
            Term::Score { query, .. } => match query {
                ScoreQuery::Vector(e) => *e = self.expr(e)?,
                ScoreQuery::Lexical(LexicalQuery {
                    text, max_edits, ..
                }) => {
                    *text = self.expr(text)?;
                    *max_edits = self.expr(max_edits)?;
                }
            },
            Term::ApproxTopK { query, .. } => *query = self.expr(query)?,
            Term::Call { bindings, .. } => {
                for (_, e) in bindings {
                    *e = self.expr(e)?;
                }
            }
            _ => {}
        }
        match term {
            Term::Unit | Term::Env(_) | Term::Scan { .. } | Term::RecRef { .. } => Ok(()),
            Term::Expand { input, .. }
            | Term::Filter { input, .. }
            | Term::Project { input, .. }
            | Term::Group { input, .. }
            | Term::Order { input, .. }
            | Term::Cut { input, .. }
            | Term::Score { input, .. }
            | Term::ApproxTopK { input, .. } => self.term(input),
            Term::Correlate { outer, inner } => {
                self.term(outer)?;
                self.term(inner)
            }
            Term::Union { left, right } => {
                self.term(left)?;
                self.term(right)
            }
            Term::Fuse { arms, .. } => arms.iter_mut().try_for_each(|a| self.term(a)),
            Term::Fix { body, .. } | Term::Call { body, .. } => self.term(body),
        }
    }

    /// A reference's identity column as a String leaf.
    fn identity(&self, reference: &IRExpr) -> IRExpr {
        let IRExpr::Variable(variable, _) = reference else {
            unreachable!("is_reference holds")
        };
        IRExpr::PropAccess {
            variable: variable.clone(),
            property: self.catalog.system_columns.id.to_string(),
            ty: crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                crate::types::ScalarType::String,
                false,
            )),
        }
    }

    fn physical(&self, property: &str) -> String {
        let system = self.catalog.system_columns;
        match property {
            EDGE_TYPE_META => EDGE_TYPE_COLUMN.to_string(),
            name if name == SYSTEM_COLUMNS_META.id => system.id.to_string(),
            name if name == SYSTEM_COLUMNS_META.src => system.src.to_string(),
            name if name == SYSTEM_COLUMNS_META.dst => system.dst.to_string(),
            other => other.to_string(),
        }
    }

    /// A comparison operand as [`coerce`] takes it: without the widening
    /// cast the type checker put on it, and a numeric literal at its own
    /// type, so the comparison's casts are `coerce`'s to choose.
    fn operand(&self, e: &IRExpr) -> Result<IRExpr> {
        let e = match e {
            IRExpr::Cast { expr, .. } => expr,
            other => other,
        };
        Ok(match self.expr(e)? {
            IRExpr::Literal(lit, ty) => {
                let own = match &lit {
                    Literal::Integer(_) | Literal::Float(_) => literal_type(&lit).ok(),
                    Literal::List(items) if !items.is_empty() => literal_type(&lit).ok(),
                    _ => None,
                };
                let ty = own.map(|t| ExprType::from_prop(&t)).unwrap_or(ty);
                IRExpr::Literal(lit, ty)
            }
            other => other,
        })
    }

    fn expr(&self, e: &IRExpr) -> Result<IRExpr> {
        let b = |x: &IRExpr| self.expr(x).map(Box::new);
        Ok(match e {
            IRExpr::PropAccess {
                variable,
                property,
                ty,
            } => {
                if property == EDGE_TYPE_META
                    && let Some(Bound::Edge(types)) = self.bound.get(variable)
                    && let [name] = types.as_slice()
                {
                    return Ok(IRExpr::Literal(Literal::String(name.clone()), ty.clone()));
                }
                IRExpr::PropAccess {
                    variable: variable.clone(),
                    property: self.physical(property),
                    ty: ty.clone(),
                }
            }
            IRExpr::Binary {
                left,
                op: BinaryOp::Compare(CompOp::Contains),
                right,
                ..
            } => coerce::in_list(self.operand(left)?, self.operand(right)?)?,
            // Two references compare by identity: their `@id` columns.
            IRExpr::Binary {
                left,
                op: op @ BinaryOp::Compare(CompOp::Eq | CompOp::Ne),
                right,
                ..
            } if is_reference(left) && is_reference(right) => {
                coerce::binary(self.identity(left), *op, self.identity(right))?
            }
            IRExpr::Binary {
                left,
                op: op @ BinaryOp::Compare(_),
                right,
                ..
            } => coerce::binary(self.operand(left)?, *op, self.operand(right)?)?,
            IRExpr::Binary {
                left,
                op: op @ (BinaryOp::And | BinaryOp::Or),
                right,
                ..
            } => coerce::binary(self.expr(left)?, *op, self.expr(right)?)?,
            IRExpr::Binary {
                left,
                op,
                right,
                ty,
            } => IRExpr::Binary {
                left: b(left)?,
                op: *op,
                right: b(right)?,
                ty: ty.clone(),
            },
            IRExpr::Not(inner, _) => fold::not(self.expr(inner)?),
            IRExpr::IsNull { expr, negated, .. } => fold::is_null(self.expr(expr)?, *negated),
            IRExpr::Neg { expr, ty } => IRExpr::Neg {
                expr: b(expr)?,
                ty: ty.clone(),
            },
            IRExpr::Cast { expr, ty } => IRExpr::Cast {
                expr: b(expr)?,
                ty: ty.clone(),
            },
            IRExpr::Call { func, args, ty } => IRExpr::Call {
                func: *func,
                args: args.iter().map(|a| self.expr(a)).collect::<Result<_>>()?,
                ty: ty.clone(),
            },
            IRExpr::Search { field, query, ty } => IRExpr::Search {
                field: b(field)?,
                query: b(query)?,
                ty: ty.clone(),
            },
            IRExpr::MatchText { field, query, ty } => IRExpr::MatchText {
                field: b(field)?,
                query: b(query)?,
                ty: ty.clone(),
            },
            IRExpr::Fuzzy {
                field,
                query,
                max_edits,
                ty,
            } => IRExpr::Fuzzy {
                field: b(field)?,
                query: b(query)?,
                max_edits: max_edits.as_deref().map(&b).transpose()?,
                ty: ty.clone(),
            },
            IRExpr::Aggregate {
                func,
                distinct,
                arg,
                signature,
            } => IRExpr::Aggregate {
                func: *func,
                distinct: *distinct,
                arg: b(arg)?,
                signature: signature.clone(),
            },
            other => other.clone(),
        })
    }
}

/// A search predicate standing alone as a filter, compared with `true`: the
/// one shape a search filter has in a plan.
fn search_compared(pred: IRExpr) -> IRExpr {
    match pred {
        call @ (IRExpr::Search { .. } | IRExpr::Fuzzy { .. } | IRExpr::MatchText { .. }) => {
            let truth = IRExpr::Literal(
                Literal::Bool(true),
                crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                    crate::types::ScalarType::Bool,
                    false,
                )),
            );
            coerce::binary(call.clone(), BinaryOp::Compare(CompOp::Eq), truth).unwrap_or(call)
        }
        other => other,
    }
}

fn is_reference(e: &IRExpr) -> bool {
    matches!(
        e,
        IRExpr::Variable(
            _,
            crate::types::ExprType::Node { .. } | crate::types::ExprType::Edge { .. }
        )
    )
}

fn edge_types(edges: &EdgeSelection) -> Vec<String> {
    edges
        .members()
        .iter()
        .map(|m| m.edge_type.clone())
        .collect()
}
