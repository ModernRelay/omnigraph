//! A core write as the mutation executor runs it: one keyed statement per
//! sink, group by group in order.
//!
//! A group's clauses select the rows its statements run over. The executor
//! stages a statement against one table, so a group lowers when its clauses
//! are a scan of the statement's target, filtered by a predicate over that
//! variable alone: an `update` or `delete` takes the predicate as its
//! selection; an `insert` with no clauses runs once; an `insert` in a group
//! with clauses runs once per selected row, the count the group's earlier
//! `update` or `delete` matched. Any other group is `P006`.

use omnigraph_compiler::ir::IRExpr;
use omnigraph_compiler::ir::IRParam;
use omnigraph_compiler::ir::core::{AssignValue, CoreWrite, MutationTarget, Sink, Term, Ty};
use omnigraph_compiler::query::codes::P006;
use omnigraph_compiler::query::diagnostic::QueryDiagnostic;
use omnigraph_compiler::query::file::Param;
use omnigraph_compiler::query::value::Literal;
use omnigraph_compiler::types::{ExprType, PropType, ScalarType};

use crate::error::{OmniError, Result};

/// One assignment of an insert or update: the property and its value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Assignment {
    pub property: String,
    pub value: IRExpr,
}

/// One statement of a write.
#[derive(Debug, Clone)]
pub(crate) enum WriteOp {
    Insert {
        target: MutationTarget,
        assignments: Vec<Assignment>,
        /// In a group with clauses: the statement runs once per row the
        /// group's earlier statement matched.
        per_row: bool,
    },
    Update {
        target: MutationTarget,
        assignments: Vec<Assignment>,
        predicate: IRExpr,
    },
    Delete {
        target: MutationTarget,
        predicate: IRExpr,
    },
}

/// A checked write's statements, in run order.
#[derive(Debug, Clone)]
pub(crate) struct WritePlan {
    pub name: String,
    pub write_mode: omnigraph_compiler::query::file::MutationWriteMode,
    pub params: Vec<IRParam>,
    pub ops: Vec<WriteOp>,
}

fn unsupported(name: &str, what: &str) -> OmniError {
    OmniError::Compiler(omnigraph_compiler::error::CompilerError::query(
        QueryDiagnostic::plan(P006, format!("`{name}`: the planner does not lower {what}")),
    ))
}

impl WritePlan {
    pub(crate) fn of(write: &CoreWrite) -> Result<Self> {
        let name = write.name.as_str();
        let mut ops = Vec::new();
        for group in &write.groups {
            let selection = selection(name, &group.input)?;
            let mut matched = false;
            for sink in &group.sinks {
                let op = match sink {
                    Sink::Insert {
                        target,
                        assignments,
                    } => {
                        if selection.is_some() && !matched {
                            return Err(unsupported(
                                name,
                                "an insert over a group's rows before a statement matches them",
                            ));
                        }
                        WriteOp::Insert {
                            target: target.clone(),
                            assignments: constants(name, assignments)?,
                            per_row: selection.is_some(),
                        }
                    }
                    Sink::Update {
                        var,
                        target,
                        assignments,
                    } => {
                        let predicate = selected(name, &selection, var)?;
                        matched = true;
                        WriteOp::Update {
                            target: target.clone(),
                            assignments: constants(name, assignments)?,
                            predicate,
                        }
                    }
                    Sink::Delete { var, target } => {
                        let predicate = selected(name, &selection, var)?;
                        matched = true;
                        WriteOp::Delete {
                            target: target.clone(),
                            predicate,
                        }
                    }
                };
                ops.push(op);
            }
        }
        Ok(Self {
            name: write.name.clone(),
            write_mode: write.write_mode.clone(),
            params: write
                .params
                .iter()
                .map(|(name, ty)| IRParam {
                    declaration: Param {
                        name: name.clone(),
                        type_name: ty.display_name().trim_end_matches('?').to_string(),
                        nullable: ty.nullable,
                    },
                    ty: ExprType::from_prop(ty),
                })
                .collect(),
            ops,
        })
    }
}

/// A group's selection: the variable it scans and the conjunction of its
/// filters over that variable; `None` for a group with no clauses.
fn selection(name: &str, input: &Term) -> Result<Option<(String, IRExpr)>> {
    let mut conjuncts = Vec::new();
    let mut term = input;
    loop {
        match term {
            Term::Unit => return Ok(None),
            Term::Filter { input, pred } => {
                conjuncts.push(pred.clone());
                term = input;
            }
            Term::Scan {
                var,
                ty: Ty::Node { .. } | Ty::Edge { .. },
            } => {
                let mut cols = Vec::new();
                for c in &conjuncts {
                    c.columns(&mut cols);
                }
                if cols.iter().any(|c| c != var) {
                    return Err(unsupported(
                        name,
                        "a write group whose filters read another variable",
                    ));
                }
                if conjuncts.iter().any(reads_search) {
                    return Err(unsupported(
                        name,
                        "a search predicate in a write group's filters",
                    ));
                }
                conjuncts.reverse();
                let predicate = IRExpr::and_all(conjuncts).unwrap_or_else(|| {
                    IRExpr::Literal(
                        Literal::Bool(true),
                        ExprType::from_prop(&PropType::scalar(ScalarType::Bool, false)),
                    )
                });
                return Ok(Some((var.clone(), predicate)));
            }
            _ => {
                return Err(unsupported(
                    name,
                    "a write group whose selection is more than a filtered scan of the target",
                ));
            }
        }
    }
}

/// Whether `e` holds a search predicate, which a staged write's selection
/// does not evaluate.
fn reads_search(e: &IRExpr) -> bool {
    match e {
        IRExpr::Search { .. } | IRExpr::Fuzzy { .. } | IRExpr::MatchText { .. } => true,
        IRExpr::Binary { left, right, .. } => reads_search(left) || reads_search(right),
        IRExpr::Not(inner, _) => reads_search(inner),
        _ => false,
    }
}

fn selected(name: &str, selection: &Option<(String, IRExpr)>, var: &str) -> Result<IRExpr> {
    match selection {
        Some((scanned, predicate)) if scanned == var => Ok(predicate.clone()),
        _ => Err(unsupported(
            name,
            "a statement over a variable its group does not scan",
        )),
    }
}

fn constants(name: &str, assignments: &[(String, AssignValue)]) -> Result<Vec<Assignment>> {
    assignments
        .iter()
        .map(|(property, value)| {
            let value = match value {
                AssignValue::Null => IRExpr::Literal(
                    Literal::Null,
                    ExprType::from_prop(&PropType::scalar(ScalarType::String, true)),
                ),
                AssignValue::Expr(e) => {
                    let mut cols = Vec::new();
                    e.columns(&mut cols);
                    if !cols.is_empty() {
                        return Err(unsupported(name, "an assignment that reads a row"));
                    }
                    e.clone()
                }
            };
            Ok(Assignment {
                property: property.clone(),
                value,
            })
        })
        .collect()
}
