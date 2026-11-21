//! The bound form of every expression a checked declaration holds: a
//! property leaf on its physical column (`@id`, `@src`, `@dst` through the
//! catalog's system columns; a single-type edge's `@type` its name), every
//! comparison, membership and Boolean node built through [`coerce`], which
//! stores the explicit casts of invariant 9, and literal-only nodes folded
//! ([`fold`]). The type checker types; this pass binds what it typed, so
//! the planner, the engine and the bound-plan mirror read one form.

use std::collections::HashMap;
use std::sync::Arc;

use super::literal::literal_type;
use crate::catalog::Catalog;
use crate::catalog::schema_ir::SYSTEM_COLUMNS_META;
use crate::error::Result;
use crate::ir::core::{AssignValue, CoreDecl, LexicalQuery, ScoreQuery, Sink, Term};
use crate::ir::expr::{IRExpr, ScalarFunc};
use crate::ir::{coerce, fold};
use crate::query::value::{BinaryOp, CompOp, Literal};
use crate::traversal::{EDGE_TYPE_COLUMN, EDGE_TYPE_META, EdgeSelection};
use crate::types::ExprType;

/// What a variable binds in one scope: a node, or an edge of these types.
#[derive(Debug, Clone, PartialEq)]
enum Bound {
    Node,
    Edge(Vec<String>),
}

/// The variables one term's rows carry, by name. Each scope has its own: a
/// write group's, a block's, an `optional`'s or a union arm's names never
/// reach a sibling scope that reuses them.
type Scope = HashMap<String, Bound>;

struct Binder<'a> {
    catalog: &'a Catalog,
    /// Each shared subterm bound so far, by the node it was before binding:
    /// that node (kept alive, so its address is not reused while the walk
    /// runs), the node bound, and its scope. A subterm two parents share (an
    /// `optional`'s pattern) is bound once, and both parents keep sharing
    /// the bound node; its binding reads only its own leaves.
    bound: std::cell::RefCell<HashMap<*const Term, BoundOnce>>,
}

/// A shared subterm bound once: the node it was, kept alive; the node bound;
/// its scope.
type BoundOnce = (Arc<Term>, Arc<Term>, Scope);

/// Bind every expression of `decl` in place.
pub(super) fn bind_decl(catalog: &Catalog, decl: &mut CoreDecl) -> Result<()> {
    let binder = Binder {
        catalog,
        bound: Default::default(),
    };
    match decl {
        CoreDecl::Read(read) => binder.term(&mut read.term, &Scope::new()).map(|_| ()),
        CoreDecl::Write(write) => {
            for group in &mut write.groups {
                // A group's statements read the names its own selection binds.
                let scope = binder.term(&mut group.input, &Scope::new())?;
                for sink in &mut group.sinks {
                    let assignments = match sink {
                        Sink::Insert { assignments, .. } | Sink::Update { assignments, .. } => {
                            assignments
                        }
                        Sink::Delete { .. } => continue,
                    };
                    for (_, value) in assignments {
                        if let AssignValue::Expr(e) = value {
                            *e = binder.expr(e, &scope)?;
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

/// What a column of type `ty` binds, if it is a reference.
fn bound_of(ty: &crate::ir::core::Ty) -> Option<Bound> {
    match ty {
        crate::ir::core::Ty::Edge { type_name, .. } => Some(Bound::Edge(
            type_name.split('|').map(str::to_string).collect(),
        )),
        crate::ir::core::Ty::Node { .. } => Some(Bound::Node),
        crate::ir::core::Ty::Value(_) => None,
    }
}

/// The columns of `cols` that are references.
fn scope_of(cols: &[crate::ir::core::Col]) -> Scope {
    cols.iter()
        .filter_map(|c| bound_of(&c.ty).map(|b| (c.name.clone(), b)))
        .collect()
}

/// `b`'s names joined into `a`: a name both bind as edges binds every type
/// either does.
fn merge(mut a: Scope, b: Scope) -> Scope {
    for (name, bound) in b {
        match (a.get_mut(&name), bound) {
            (Some(Bound::Edge(types)), Bound::Edge(more)) => {
                for t in more {
                    if !types.contains(&t) {
                        types.push(t);
                    }
                }
            }
            (Some(_), _) => {}
            (None, bound) => {
                a.insert(name, bound);
            }
        }
    }
    a
}

/// The projected columns that are references: an item that is a variable
/// of `scope` binds the item's name.
fn projected(scope: &Scope, items: &[(String, IRExpr)]) -> Scope {
    items
        .iter()
        .filter_map(|(name, e)| match e {
            IRExpr::Variable(v, _) => scope.get(v).map(|b| (name.clone(), b.clone())),
            _ => None,
        })
        .collect()
}

impl Binder<'_> {
    /// Bind `term`'s expressions, each in the scope of the rows it reads,
    /// and return the names `term`'s rows carry. `outer` is the scope a
    /// correlated term's `Env` leaf and a call's arguments read.
    fn child(&self, child: &mut Arc<Term>, outer: &Scope) -> Result<Scope> {
        let key = Arc::as_ptr(child);
        if let Some((_, done, scope)) = self.bound.borrow().get(&key) {
            *child = Arc::clone(done);
            return Ok(scope.clone());
        }
        let before = Arc::clone(child);
        let scope = self.term(Arc::make_mut(child), outer)?;
        self.bound
            .borrow_mut()
            .insert(key, (before, Arc::clone(child), scope.clone()));
        Ok(scope)
    }

    /// The scope `term` binds, its expressions bound in place.
    fn term(&self, term: &mut Term, outer: &Scope) -> Result<Scope> {
        crate::stack::deep(|| {
            Ok(match term {
                Term::Unit => Scope::new(),
                Term::Env(cols) => scope_of(cols),
                Term::RecRef { ty, .. } => scope_of(&ty.cols),
                Term::Scan { var, ty } => bound_of(ty)
                    .map(|b| Scope::from([(var.clone(), b)]))
                    .unwrap_or_default(),
                Term::Expand {
                    input,
                    dst,
                    edges,
                    edge_var,
                    ..
                } => {
                    let mut scope = self.child(input, outer)?;
                    scope.insert(dst.clone(), Bound::Node);
                    if let Some(e) = edge_var {
                        scope.insert(e.clone(), Bound::Edge(edge_types(edges)));
                    }
                    scope
                }
                Term::Filter { input, pred, .. } => {
                    let scope = self.child(input, outer)?;
                    *pred = self.expr(pred, &scope)?;
                    scope
                }
                Term::Project {
                    input, items, keep, ..
                } => {
                    let scope = self.child(input, outer)?;
                    for (_, e) in items.iter_mut() {
                        *e = self.expr(e, &scope)?;
                    }
                    let out = projected(&scope, items);
                    if *keep { merge(scope, out) } else { out }
                }
                Term::Group { input, keys, aggs } => {
                    let scope = self.child(input, outer)?;
                    for (_, e) in keys.iter_mut() {
                        *e = self.expr(e, &scope)?;
                    }
                    for (_, agg) in aggs {
                        if let Some(arg) = &mut agg.arg {
                            *arg = self.expr(arg, &scope)?;
                        }
                    }
                    projected(&scope, keys)
                }
                Term::Order { input, keys } => {
                    let scope = self.child(input, outer)?;
                    for key in keys {
                        key.expr = self.expr(&key.expr, &scope)?;
                    }
                    scope
                }
                Term::Cut { input, .. } => self.child(input, outer)?,
                Term::Score { input, query, .. } => {
                    let scope = self.child(input, outer)?;
                    match query {
                        ScoreQuery::Vector(e) => *e = self.expr(e, &scope)?,
                        ScoreQuery::Lexical(LexicalQuery {
                            text, max_edits, ..
                        }) => {
                            *text = self.expr(text, &scope)?;
                            *max_edits = self.expr(max_edits, &scope)?;
                        }
                    }
                    scope
                }
                Term::ApproxTopK { input, query, .. } => {
                    let scope = self.child(input, outer)?;
                    *query = self.expr(query, &scope)?;
                    scope
                }
                Term::Correlate {
                    outer: left, inner, ..
                } => {
                    let scope = self.child(left, outer)?;
                    let joined = self.child(inner, &scope)?;
                    merge(scope, joined)
                }
                Term::Union { left, right } => {
                    let l = self.child(left, outer)?;
                    let r = self.child(right, outer)?;
                    merge(l, r)
                }
                Term::Fuse { arms, .. } => {
                    let mut scope = Scope::new();
                    for arm in arms {
                        let arm_scope = self.term(arm, outer)?;
                        scope = merge(scope, arm_scope);
                    }
                    scope
                }
                Term::Call { bindings, body, .. } => {
                    // The arguments are the caller's; the body is its own scope.
                    for (_, e) in bindings {
                        *e = self.expr(e, outer)?;
                    }
                    self.child(body, &Scope::new())?
                }
                Term::Fix { body, .. } => self.child(body, outer)?,
            })
        })
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
    fn operand(&self, e: &IRExpr, scope: &Scope) -> Result<IRExpr> {
        let e = match e {
            IRExpr::Cast { expr, .. } => expr,
            other => other,
        };
        let bound = self.expr(e, scope)?;
        Ok(match &bound {
            IRExpr::Literal(lit, ty) => {
                let own = match lit {
                    Literal::Integer(_) | Literal::Float(_) => literal_type(lit).ok(),
                    Literal::List(items) if !items.is_empty() => literal_type(lit).ok(),
                    _ => None,
                };
                let ty = own.map_or_else(|| ty.clone(), |t| ExprType::from_prop(&t));
                IRExpr::Literal(lit.clone(), ty)
            }
            _ => bound,
        })
    }

    fn expr(&self, e: &IRExpr, scope: &Scope) -> Result<IRExpr> {
        crate::stack::deep(|| {
            let b = |x: &IRExpr| self.expr(x, scope).map(Box::new);
            Ok(match e {
                IRExpr::PropAccess {
                    variable,
                    property,
                    ty,
                } => {
                    if property == EDGE_TYPE_META
                        && let Some(Bound::Edge(types)) = scope.get(variable)
                        && let [name] = types.as_slice()
                    {
                        // A single-type edge's rows carry no type column: its
                        // type is its name, and null on a row where the edge is
                        // null (an `optional` with no match).
                        let name = IRExpr::Literal(Literal::String(name.clone()), ty.clone());
                        if !ty.nullable() {
                            return Ok(name);
                        }
                        let present = IRExpr::IsNull {
                            expr: Box::new(IRExpr::PropAccess {
                                variable: variable.clone(),
                                property: self.catalog.system_columns.id.to_string(),
                                ty: ExprType::from_prop(&crate::types::PropType::scalar(
                                    crate::types::ScalarType::String,
                                    true,
                                )),
                            }),
                            negated: true,
                            ty: ExprType::from_prop(&crate::types::PropType::scalar(
                                crate::types::ScalarType::Bool,
                                false,
                            )),
                        };
                        return Ok(IRExpr::Call {
                            func: ScalarFunc::If,
                            args: vec![present, name, IRExpr::Literal(Literal::Null, ty.clone())],
                            ty: ty.clone(),
                        });
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
                } => coerce::in_list(self.operand(left, scope)?, self.operand(right, scope)?)?,
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
                } => coerce::binary(self.operand(left, scope)?, *op, self.operand(right, scope)?)?,
                IRExpr::Binary {
                    left,
                    op: op @ (BinaryOp::And | BinaryOp::Or),
                    right,
                    ..
                } => coerce::binary(self.expr(left, scope)?, *op, self.expr(right, scope)?)?,
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
                IRExpr::Not(inner, _) => fold::not(self.expr(inner, scope)?),
                IRExpr::IsNull { expr, negated, .. } => {
                    fold::is_null(self.expr(expr, scope)?, *negated)
                }
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
                    args: args
                        .iter()
                        .map(|a| self.expr(a, scope))
                        .collect::<Result<_>>()?,
                    ty: ty.clone(),
                },
                IRExpr::Aggregate {
                    func,
                    distinct,
                    arg,
                    fraction,
                    signature,
                } => IRExpr::Aggregate {
                    func: *func,
                    distinct: *distinct,
                    arg: b(arg)?,
                    fraction: match fraction {
                        Some(p) => Some(b(p)?),
                        None => None,
                    },
                    signature: signature.clone(),
                },
                other => other.clone(),
            })
        })
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
