//! A core write as the mutation executor runs it: one keyed statement per
//! sink, group by group in order.
//!
//! A group's clauses select the rows its statements run over, in one of two
//! forms. When they are a scan of the statements' target filtered by
//! predicates a staged scan evaluates, the group lowers to staged statements
//! directly: an `update` or `delete` takes the predicate as its selection,
//! read together with the statements staged before it, and an `insert` after
//! one runs once per row it matched. Every other group with clauses lowers
//! its selection to a read that the read planner shapes ([`ReadShape`]) and
//! the engine runs against the snapshot the write captured. The read returns,
//! per row, the identity of each `update` or `delete` variable and the value
//! of each assignment that reads the row; that statement then selects exactly
//! those identities, and an `insert` runs once per row. An `insert` with no
//! clauses runs once.
//!
//! A selection read sees the snapshot, not the statements staged before it,
//! so a group whose selection reads a table an earlier group of the write
//! changes is not lowered in this release; nor is a selection the read
//! shaper does not lower, or an assignment that reads a Blob from a row.

use std::collections::BTreeSet;

use super::read::ReadShape;
use super::{Construct, ShapeError, Unlowered};
use crate::catalog::Catalog;
use crate::ir::IRExpr;
use crate::ir::IRParam;
use crate::ir::core::{
    AssignValue, CoreRead, CoreWrite, DEFAULT_PLAN_BUDGET, MutationTarget, RelTy, Sink, Term, Ty,
    WriteGroup,
};
use crate::query::file::Param;
use crate::query::value::{BinaryOp, Literal};
use crate::types::{ExprType, PropType, ScalarType};

type Result<T> = std::result::Result<T, ShapeError>;

/// One assignment of an insert or update whose value reads no row: the
/// property and its value.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub property: String,
    pub value: IRExpr,
}

/// An assignment whose value reads its group's row: the property and the
/// selection read's result column that holds the value.
#[derive(Debug, Clone, PartialEq)]
pub struct RowValue {
    pub property: String,
    pub column: usize,
}

/// The rows of a group's selection read that one statement runs over.
#[derive(Debug, Clone)]
pub struct SelectedRows {
    /// The selection read, an index into [`WriteShape::selections`].
    pub read: usize,
    /// The result column holding the identity of an `update` or `delete`
    /// variable; `None` for an `insert`.
    pub identity: Option<usize>,
    /// The assignments that read the row.
    pub values: Vec<RowValue>,
    /// The statement as a refusal names it (`statement 1 of group 2`).
    pub statement: String,
}

/// The rows an `insert` runs over.
#[derive(Debug, Clone)]
pub enum InsertRows {
    /// A group with no clauses: once.
    Once,
    /// A staged group: once per row the group's earlier statement matched.
    Matched,
    /// Once per row of the group's selection read.
    Selected(SelectedRows),
}

/// The entities an `update` or `delete` reaches.
#[derive(Debug, Clone)]
pub enum Selection {
    /// A staged group: the predicate over the target's rows, read together
    /// with the statements staged before it.
    Predicate(IRExpr),
    /// The identities the group's selection read returns.
    Rows(SelectedRows),
}

/// One statement of a write.
#[derive(Debug, Clone)]
pub enum WriteOp {
    Insert {
        target: MutationTarget,
        assignments: Vec<Assignment>,
        rows: InsertRows,
    },
    Update {
        target: MutationTarget,
        assignments: Vec<Assignment>,
        selection: Selection,
    },
    Delete {
        target: MutationTarget,
        selection: Selection,
    },
}

impl WriteOp {
    /// Whether the statement's rows come from a selection read.
    pub fn reads_a_selection(&self) -> bool {
        matches!(
            self,
            Self::Insert {
                rows: InsertRows::Selected(_),
                ..
            } | Self::Update {
                selection: Selection::Rows(_),
                ..
            } | Self::Delete {
                selection: Selection::Rows(_),
                ..
            }
        )
    }
}

/// A checked write's statements, in run order.
#[derive(Debug, Clone)]
pub struct WriteShape {
    pub name: String,
    pub write_mode: crate::query::file::MutationWriteMode,
    pub params: Vec<IRParam>,
    pub ops: Vec<WriteOp>,
    /// The groups' selection reads, each run once against the write's
    /// snapshot.
    pub selections: Vec<ReadShape>,
}

/// A term of the break that this release does not lower, at `construct`.
fn unsupported(name: &str, what: impl std::fmt::Display, construct: Construct) -> ShapeError {
    ShapeError::unlowered(Unlowered::new(name, what, construct))
}

impl WriteShape {
    /// The statements of `write`, or the term this release does not lower.
    pub fn of(write: &CoreWrite, catalog: &Catalog) -> Result<Self> {
        let name = write.name.as_str();
        let mut ops = Vec::new();
        let mut selections = Vec::new();
        // The tables the groups before the current one write.
        let mut written = BTreeSet::new();
        for (g, group) in write.groups.iter().enumerate() {
            match staged(group) {
                Some(selection) => lower_staged(name, g, group, selection, &mut ops)?,
                None => {
                    let mut read = BTreeSet::new();
                    tables_read(&group.input, catalog, &mut read);
                    if let Some(table) = read.intersection(&written).next() {
                        return Err(unsupported(
                            name,
                            format!(
                                "a write group whose selection reads a table an earlier group \
                                 of the write changes (`{table}`)"
                            ),
                            Construct::Group(g),
                        ));
                    }
                    let index = selections.len();
                    let (query, group_ops) = lower_selected(write, g, group, index, catalog)?;
                    selections.push(query);
                    ops.extend(group_ops);
                }
            }
            for sink in &group.sinks {
                tables_written(sink, catalog, &mut written);
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
            selections,
        })
    }
}

/// The selection of a group that lowers to staged statements: no clauses,
/// or a scan of a variable with the conjunction of its filters.
enum Staged {
    Unit,
    Scan { predicate: IRExpr },
}

/// The group's selection as staged statements evaluate it, or `None` when
/// the group needs a selection read: its clauses are more than a filtered
/// scan, a filter holds a term a staged scan does not evaluate (a search
/// predicate, arithmetic, a registry call), a statement names another
/// variable, an assignment reads the row, or an `insert` comes before the
/// statement whose rows it would count.
fn staged(group: &WriteGroup) -> Option<Staged> {
    let mut conjuncts = Vec::new();
    let mut term = &group.input;
    let (var, conjuncts) = loop {
        match term {
            Term::Unit if conjuncts.is_empty() => return Some(Staged::Unit),
            Term::Filter { input, pred } => {
                conjuncts.push(pred.clone());
                term = input;
            }
            Term::Scan {
                var,
                ty: Ty::Node { .. } | Ty::Edge { .. },
            } => break (var, conjuncts),
            _ => return None,
        }
    };
    let mut cols = Vec::new();
    for c in &conjuncts {
        c.columns(&mut cols);
    }
    if cols.iter().any(|c| c != var) || conjuncts.iter().any(|c| !stageable(c)) {
        return None;
    }
    let mut matched = false;
    for sink in &group.sinks {
        let assignments = match sink {
            Sink::Insert { assignments, .. } => {
                if !matched {
                    return None;
                }
                assignments.as_slice()
            }
            Sink::Update {
                var: target,
                assignments,
                ..
            } => {
                matched = true;
                if target != var {
                    return None;
                }
                assignments.as_slice()
            }
            Sink::Delete { var: target, .. } => {
                matched = true;
                if target != var {
                    return None;
                }
                &[]
            }
        };
        if assignments.iter().any(|(_, value)| reads_row(value)) {
            return None;
        }
    }
    let mut conjuncts = conjuncts;
    conjuncts.reverse();
    let predicate = IRExpr::and_all(conjuncts).unwrap_or_else(|| {
        IRExpr::Literal(
            Literal::Bool(true),
            ExprType::from_prop(&PropType::scalar(ScalarType::Bool, false)),
        )
    });
    Some(Staged::Scan { predicate })
}

/// Whether a staged scan evaluates `e`: it pushes the predicate to the
/// table scan, which evaluates no search predicate, arithmetic or registry
/// call.
fn stageable(e: &IRExpr) -> bool {
    match e {
        IRExpr::Search { .. }
        | IRExpr::Fuzzy { .. }
        | IRExpr::MatchText { .. }
        | IRExpr::Neg { .. }
        | IRExpr::Call { .. }
        | IRExpr::Binary {
            op: BinaryOp::Arith(_),
            ..
        } => false,
        IRExpr::Binary { left, right, .. } => stageable(left) && stageable(right),
        IRExpr::Not(inner, _) | IRExpr::Cast { expr: inner, .. } => stageable(inner),
        IRExpr::IsNull { expr, .. } => stageable(expr),
        _ => true,
    }
}

fn reads_row(value: &AssignValue) -> bool {
    match value {
        AssignValue::Null => false,
        AssignValue::Expr(e) => !e.columns_empty(),
    }
}

fn lower_staged(
    name: &str,
    g: usize,
    group: &WriteGroup,
    selection: Staged,
    ops: &mut Vec<WriteOp>,
) -> Result<()> {
    for (i, sink) in group.sinks.iter().enumerate() {
        let op = match (sink, &selection) {
            (
                Sink::Insert {
                    target,
                    assignments,
                },
                _,
            ) => WriteOp::Insert {
                target: target.clone(),
                assignments: constants(assignments),
                rows: match selection {
                    Staged::Unit => InsertRows::Once,
                    Staged::Scan { .. } => InsertRows::Matched,
                },
            },
            (
                Sink::Update {
                    target,
                    assignments,
                    ..
                },
                Staged::Scan { predicate, .. },
            ) => WriteOp::Update {
                target: target.clone(),
                assignments: constants(assignments),
                selection: Selection::Predicate(predicate.clone()),
            },
            (Sink::Delete { target, .. }, Staged::Scan { predicate, .. }) => WriteOp::Delete {
                target: target.clone(),
                selection: Selection::Predicate(predicate.clone()),
            },
            (Sink::Update { .. } | Sink::Delete { .. }, Staged::Unit) => {
                return Err(unsupported(
                    name,
                    "a statement over a variable its group does not bind",
                    Construct::Statement(g, i),
                ));
            }
        };
        ops.push(op);
    }
    Ok(())
}

/// A group's selection read and its statements over the read's rows. The
/// read returns one column per distinct item: the identity of each `update`
/// or `delete` variable, and each assignment value that reads the row (an
/// endpoint reference as its node's identity).
fn lower_selected(
    write: &CoreWrite,
    g: usize,
    group: &WriteGroup,
    read: usize,
    catalog: &Catalog,
) -> Result<(ReadShape, Vec<WriteOp>)> {
    let name = write.name.as_str();
    let mut items: Vec<IRExpr> = Vec::new();
    let mut column = |e: IRExpr| match items.iter().position(|item| *item == e) {
        Some(index) => index,
        None => {
            items.push(e);
            items.len() - 1
        }
    };
    let identity = |var: &str| IRExpr::PropAccess {
        variable: var.to_string(),
        property: catalog.system_columns.id.to_string(),
        ty: ExprType::from_prop(&PropType::scalar(ScalarType::String, false)),
    };
    let mut ops = Vec::new();
    for (i, sink) in group.sinks.iter().enumerate() {
        let statement = format!("statement {} of group {}", i + 1, g + 1);
        let (target, assignments) = match sink {
            Sink::Insert {
                target,
                assignments,
            }
            | Sink::Update {
                target,
                assignments,
                ..
            } => (target, assignments.as_slice()),
            Sink::Delete { target, .. } => (target, &[][..]),
        };
        let mut constant = Vec::new();
        let mut values = Vec::new();
        for (property, value) in assignments {
            if !reads_row(value) {
                constant.push(assignment(property, value));
                continue;
            }
            let AssignValue::Expr(e) = value else {
                unreachable!("a null reads no row")
            };
            if blob_property(catalog, target, property)
                || matches!(
                    e.ty(),
                    ExprType::Value {
                        scalar: ScalarType::Blob,
                        ..
                    }
                )
            {
                return Err(unsupported(
                    name,
                    format!("an assignment that reads a Blob from a row (`{property}`)"),
                    Construct::Assignment(g, i, property.clone()),
                ));
            }
            let item = match e {
                IRExpr::Variable(var, ExprType::Node { .. }) => identity(var),
                other => other.clone(),
            };
            values.push(RowValue {
                property: property.clone(),
                column: column(item),
            });
        }
        let rows = |identity: Option<usize>| SelectedRows {
            read,
            identity,
            values: values.clone(),
            statement: statement.clone(),
        };
        ops.push(match sink {
            Sink::Insert { .. } => WriteOp::Insert {
                target: target.clone(),
                assignments: constant,
                rows: InsertRows::Selected(rows(None)),
            },
            Sink::Update { var, .. } => WriteOp::Update {
                target: target.clone(),
                assignments: constant,
                selection: Selection::Rows(rows(Some(column(identity(var))))),
            },
            Sink::Delete { var, .. } => WriteOp::Delete {
                target: target.clone(),
                selection: Selection::Rows(rows(Some(column(identity(var))))),
            },
        });
    }
    // A read returns at least one column; a group of inserts whose values
    // read no row needs only its row count.
    if items.is_empty() {
        items.push(IRExpr::Literal(
            Literal::Bool(true),
            ExprType::from_prop(&PropType::scalar(ScalarType::Bool, false)),
        ));
    }
    let hidden = |i: usize| format!("^w{i}");
    let finals = items
        .iter()
        .enumerate()
        .map(|(i, e)| (format!("w{i}"), IRExpr::AliasRef(hidden(i), e.ty().clone())))
        .collect();
    let term = Term::Project {
        input: Term::Project {
            input: group.input.clone().boxed(),
            items: items
                .into_iter()
                .enumerate()
                .map(|(i, e)| (hidden(i), e))
                .collect(),
            keep: true,
        }
        .boxed(),
        items: finals,
        keep: false,
    };
    let core = CoreRead {
        name: write.name.clone(),
        params: write.params.clone(),
        term,
        result: RelTy::default(),
        plan_budget: DEFAULT_PLAN_BUDGET,
    };
    // The selection's refusals point into the group's `match`; the
    // projection above it is this function's and lowers.
    let shape = ReadShape::of(&core, catalog).map_err(|e| match e {
        ShapeError::Unlowered(mut u) => {
            u.group = Some(g);
            ShapeError::Unlowered(u)
        }
        other => other,
    })?;
    Ok((shape, ops))
}

fn blob_property(catalog: &Catalog, target: &MutationTarget, property: &str) -> bool {
    match target {
        MutationTarget::Node { type_name } => catalog
            .node_types
            .get(type_name)
            .is_some_and(|t| t.blob_properties.contains(property)),
        MutationTarget::Edge { type_name } => catalog
            .edge_types
            .get(type_name)
            .is_some_and(|t| t.blob_properties.contains(property)),
    }
}

/// The table keys a selection term reads: every node type it scans, expands
/// from or reaches (an interface as each type implementing it), and every
/// edge type it scans or traverses.
fn tables_read(term: &Term, catalog: &Catalog, out: &mut BTreeSet<String>) {
    let nodes = |name: &str, out: &mut BTreeSet<String>| {
        if catalog.node_types.contains_key(name) {
            out.insert(format!("node:{name}"));
        } else if catalog.interfaces.contains_key(name) {
            out.extend(
                catalog
                    .node_types
                    .values()
                    .filter(|n| n.implements.iter().any(|i| i == name))
                    .map(|n| format!("node:{}", n.name)),
            );
        }
    };
    match term {
        Term::Scan {
            ty: Ty::Node { type_name, .. },
            ..
        } => nodes(type_name, out),
        Term::Scan {
            ty: Ty::Edge { type_name, .. },
            ..
        } => out.extend(type_name.split('|').map(|t| format!("edge:{t}"))),
        Term::Expand {
            src_type,
            dst_type,
            edges,
            ..
        } => {
            nodes(src_type, out);
            nodes(dst_type, out);
            out.extend(
                edges
                    .members()
                    .iter()
                    .map(|m| format!("edge:{}", m.edge_type)),
            );
        }
        _ => {}
    }
    for child in term.children() {
        tables_read(child, catalog, out);
    }
}

/// The table keys a statement writes: its target's, and for a node delete
/// every edge type whose endpoint it is, which the delete cascades to.
fn tables_written(sink: &Sink, catalog: &Catalog, out: &mut BTreeSet<String>) {
    match sink {
        Sink::Insert { target, .. } | Sink::Update { target, .. } => {
            out.insert(table_key(target));
        }
        Sink::Delete { target, .. } => {
            out.insert(table_key(target));
            if let MutationTarget::Node { type_name } = target {
                out.extend(
                    catalog
                        .edge_types
                        .iter()
                        .filter(|(_, e)| e.from_type == *type_name || e.to_type == *type_name)
                        .map(|(edge, _)| format!("edge:{edge}")),
                );
            }
        }
    }
}

fn table_key(target: &MutationTarget) -> String {
    match target {
        MutationTarget::Node { type_name } => format!("node:{type_name}"),
        MutationTarget::Edge { type_name } => format!("edge:{type_name}"),
    }
}

fn constants(assignments: &[(String, AssignValue)]) -> Vec<Assignment> {
    assignments
        .iter()
        .map(|(property, value)| assignment(property, value))
        .collect()
}

fn assignment(property: &str, value: &AssignValue) -> Assignment {
    Assignment {
        property: property.to_string(),
        value: match value {
            AssignValue::Null => IRExpr::Literal(
                Literal::Null,
                ExprType::from_prop(&PropType::scalar(ScalarType::String, true)),
            ),
            AssignValue::Expr(e) => e.clone(),
        },
    }
}
