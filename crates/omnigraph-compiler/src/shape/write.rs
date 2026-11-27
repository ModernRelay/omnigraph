//! A core write as the mutation executor runs it: one keyed statement per
//! sink, group by group in order.
//!
//! A group's clauses select the rows its statements run over, evaluated
//! once, before its first statement, in one of two forms. When they are a
//! scan of the statements' target filtered by predicates a staged scan
//! evaluates, and the group's one `update` or `delete` is its first
//! statement, the group lowers to staged statements directly: the `update`
//! or `delete` takes the predicate as its selection, read together with the
//! groups staged before it, and an `insert` after it runs once per row it
//! matched. Every other group with clauses lowers
//! its selection to a read that the read planner shapes ([`ReadShape`]) and
//! the engine runs against the snapshot the write captured. The read returns,
//! per row, the identity of each `update` or `delete` variable and the value
//! of each assignment that reads the row; that statement then selects exactly
//! those identities, and an `insert` runs once per row. An `insert` with no
//! clauses runs once. A Blob is no read value, so an assignment of a Blob
//! read from the row is lowered to the cells it copies ([`BlobValue`]): the
//! read returns the identity of each node or edge whose Blob it names, the
//! `if` conditions that choose among them, and an edge's type where its
//! binding matches more than one, and the engine reads the cells those rows
//! select at the same snapshot.
//!
//! Every group selects on the write's one snapshot, never on the effects the
//! groups before it staged, so the type checker refuses a group whose
//! selection reads a table an earlier group of the write changes (`T91`,
//! [`earlier_group_change`]): there the snapshot and the staged effects
//! would answer differently. A selection the read shaper does not lower is
//! not lowered in this release.

use std::collections::BTreeSet;

use super::ShapeError;
use super::read::ReadShape;
use crate::catalog::Catalog;
use crate::ir::IRExpr;
use crate::ir::IRParam;
use crate::ir::core::{
    AssignValue, CoreRead, CoreWrite, MutationTarget, RelTy, Sink, Term, Ty, WriteGroup,
};
use crate::ir::expr::ScalarFunc;
use crate::query::file::Param;
use crate::query::syntax::InsertMode;
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

/// An assignment whose value reads its group's row: the property and where
/// the selection read holds its value.
#[derive(Debug, Clone, PartialEq)]
pub struct RowValue {
    pub property: String,
    pub value: RowSource,
}

/// Where a row-read assignment's value is.
#[derive(Debug, Clone, PartialEq)]
pub enum RowSource {
    /// The selection read's result column at this index. A String assigned
    /// to a Blob property is the Blob's input spelling, as a constant's is.
    Column(usize),
    /// A Blob read from the row: the cells the row selects.
    Blob(BlobValue),
}

/// A Blob value an assignment reads from its group's row, as the term the
/// type checker built states it, over the selection read's result columns.
/// The engine reads the cells each row selects at the write's snapshot.
#[derive(Debug, Clone, PartialEq)]
pub enum BlobValue {
    /// The Blob `property` of the node or edge whose identity is in the
    /// column `id`, stored in the table of one of `sources`: the one, or for
    /// an edge binding that matches several types the one the column
    /// `edge_type` names. No cell, a null value, where the identity is null
    /// (an `optional` that matched nothing).
    Cell {
        sources: Vec<MutationTarget>,
        property: String,
        id: usize,
        edge_type: Option<usize>,
    },
    /// A value that reads no row, a parameter or `null`: the Blob's input
    /// spelling, evaluated once.
    Constant(IRExpr),
    /// `if(c, a, b)`: `then` where the column `condition` is true, else
    /// `otherwise`.
    If {
        condition: usize,
        then: Box<BlobValue>,
        otherwise: Box<BlobValue>,
    },
    /// `coalesce(a, …)`: the first argument that is not null, else null.
    Coalesce(Vec<BlobValue>),
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
        /// What the statement does on an existing key.
        mode: InsertMode,
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
    /// The core read of each selection, at the same index: what plan
    /// acceptance derives the placement of its conjuncts from.
    pub selection_reads: Vec<CoreRead>,
}

impl WriteShape {
    /// The statements of `write`, or the term this release does not lower.
    pub fn of(write: &CoreWrite, catalog: &Catalog) -> Result<Self> {
        let name = write.name.as_str();
        let mut ops = Vec::new();
        let mut selections = Vec::new();
        let mut selection_reads = Vec::new();
        // The type checker refuses this (`T91`); a write that reaches the
        // shaper with it was never checked.
        if let Some(conflict) = earlier_group_change(write, catalog) {
            return Err(ShapeError::Internal(format!(
                "`{name}`: group {} selects on `{}`, which an earlier group changes",
                conflict.group + 1,
                conflict.table
            )));
        }
        for (g, group) in write.groups.iter().enumerate() {
            match staged(group) {
                Some(selection) => lower_staged(name, g, group, selection, &mut ops)?,
                None => {
                    let index = selections.len();
                    let (core, query, group_ops) = lower_selected(write, g, group, index, catalog)?;
                    selections.push(query);
                    selection_reads.push(core);
                    ops.extend(group_ops);
                }
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
                        default: write
                            .defaults
                            .iter()
                            .find(|(declared, _)| declared == name)
                            .map(|(_, value)| value.clone()),
                    },
                    ty: ExprType::from_prop(ty),
                })
                .collect(),
            ops,
            selections,
            selection_reads,
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
/// variable, an assignment reads the row, an `insert` comes before the
/// statement whose rows it would count, or more than one statement selects
/// rows. Either form evaluates the selection once, before the group's first
/// statement. A group whose selection reads a table an earlier group changes
/// never reaches either form (`T91`), so the staged scan, which reads the
/// rows staged before it, and the selection read, which reads the snapshot,
/// select the same rows.
fn staged(group: &WriteGroup) -> Option<Staged> {
    let mut conjuncts = Vec::new();
    let mut term = &group.input;
    let (var, conjuncts) = loop {
        match term {
            Term::Unit if conjuncts.is_empty() => return Some(Staged::Unit),
            Term::Filter { input, pred, .. } => {
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
    // A group's selection is evaluated once, before its first statement. A
    // staged statement evaluates its predicate against the rows staged
    // before it, so only a group whose one `update` or `delete` comes first
    // is staged; with a second, the later one would select again, after the
    // first's effects, and the selection read evaluates it once instead.
    let selecting = group
        .sinks
        .iter()
        .filter(|s| matches!(s, Sink::Update { .. } | Sink::Delete { .. }))
        .count();
    if selecting > 1 {
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
    crate::stack::deep(|| match e {
        IRExpr::Neg { .. }
        | IRExpr::Call { .. }
        | IRExpr::Binary {
            op: BinaryOp::Arith(_),
            ..
        } => false,
        IRExpr::Binary { left, right, .. } => stageable(left) && stageable(right),
        IRExpr::Not(inner, _) | IRExpr::Cast { expr: inner, .. } => stageable(inner),
        IRExpr::IsNull { expr, .. } => stageable(expr),
        _ => true,
    })
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
                    mode,
                    target,
                    assignments,
                },
                _,
            ) => WriteOp::Insert {
                mode: *mode,
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
                return Err(ShapeError::Internal(format!(
                    "statement {} of group {} of `{name}` names a variable its group does not bind, which the type checker refuses (T77)",
                    i + 1,
                    g + 1
                )));
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
) -> Result<(CoreRead, ReadShape, Vec<WriteOp>)> {
    let name = write.name.as_str();
    let mut items: Vec<IRExpr> = Vec::new();
    let mut column = |e: IRExpr| match items.iter().position(|item| *item == e) {
        Some(index) => index,
        None => {
            items.push(e);
            items.len() - 1
        }
    };
    let identity = |var: &str| identity_of(catalog, var);
    let mut ops = Vec::new();
    for (i, sink) in group.sinks.iter().enumerate() {
        let statement = format!("statement {} of group {}", i + 1, g + 1);
        let (target, assignments) = match sink {
            Sink::Insert {
                target,
                assignments,
                ..
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
            let value = if is_blob(e.ty()) {
                RowSource::Blob(blob_value(name, e, &group.input, catalog, &mut column)?)
            } else {
                RowSource::Column(column(match e {
                    IRExpr::Variable(var, ExprType::Node { .. }) => identity(var),
                    other => other.clone(),
                }))
            };
            values.push(RowValue {
                property: property.clone(),
                value,
            });
        }
        let rows = |identity: Option<usize>| SelectedRows {
            read,
            identity,
            values: values.clone(),
            statement: statement.clone(),
        };
        ops.push(match sink {
            Sink::Insert { mode, .. } => WriteOp::Insert {
                mode: *mode,
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
        fails: false,
        input: Term::Project {
            fails: items.iter().any(IRExpr::can_fail),
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
        defaults: write.defaults.clone(),
        term,
        result: RelTy::default(),
        plan_budget: write.work.0.limit as usize,
        work: write.work.clone(),
        fresh: write.fresh.clone(),
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
    Ok((core, shape, ops))
}

/// The identity of the node or edge `var` holds.
fn identity_of(catalog: &Catalog, var: &str) -> IRExpr {
    IRExpr::PropAccess {
        variable: var.to_string(),
        property: catalog.system_columns.id.to_string(),
        ty: ExprType::from_prop(&PropType::scalar(ScalarType::String, false)),
    }
}

fn is_blob(ty: &ExprType) -> bool {
    matches!(
        ty,
        ExprType::Value {
            scalar: ScalarType::Blob,
            ..
        }
    )
}

/// `e`, a Blob value an assignment of `write` reads from the row of
/// `input`, as the cells it copies, each column it reads added through
/// `column`. The type checker reads a Blob as a value only there and in the
/// `if` branches and `coalesce` arguments that are the value, so every other
/// term is a defect.
fn blob_value(
    write: &str,
    e: &IRExpr,
    input: &Term,
    catalog: &Catalog,
    column: &mut impl FnMut(IRExpr) -> usize,
) -> Result<BlobValue> {
    crate::stack::deep(|| {
        if e.columns_empty() {
            return Ok(BlobValue::Constant(e.clone()));
        }
        match e {
            IRExpr::PropAccess {
                variable, property, ..
            } => {
                let binding = binding_in(input, variable);
                let nullable = binding.as_ref().is_some_and(Ty::nullable);
                let sources = match binding {
                    Some(Ty::Node { type_name, .. }) => {
                        vec![MutationTarget::Node { type_name }]
                    }
                    Some(Ty::Edge { type_name, .. }) => type_name
                        .split('|')
                        .map(|name| MutationTarget::Edge {
                            type_name: name.to_string(),
                        })
                        .collect(),
                    _ => {
                        return Err(ShapeError::Internal(format!(
                            "`{write}` assigns the Blob `${variable}.{property}`, which its selection binds to no node or edge"
                        )));
                    }
                };
                // Both columns are null on a row an `optional` kept without
                // a match, where the binding is nullable.
                let nullable_string =
                    ExprType::from_prop(&PropType::scalar(ScalarType::String, nullable));
                let edge_type = (sources.len() > 1).then(|| {
                    column(IRExpr::PropAccess {
                        variable: variable.clone(),
                        property: crate::traversal::EDGE_TYPE_COLUMN.to_string(),
                        ty: nullable_string.clone(),
                    })
                });
                Ok(BlobValue::Cell {
                    sources,
                    property: property.clone(),
                    id: column(IRExpr::PropAccess {
                        variable: variable.clone(),
                        property: catalog.system_columns.id.to_string(),
                        ty: nullable_string,
                    }),
                    edge_type,
                })
            }
            IRExpr::Call {
                func: ScalarFunc::If,
                args,
                ..
            } => match args.as_slice() {
                [condition, then, otherwise] => Ok(BlobValue::If {
                    condition: column(condition.clone()),
                    then: Box::new(blob_value(write, then, input, catalog, column)?),
                    otherwise: Box::new(blob_value(write, otherwise, input, catalog, column)?),
                }),
                _ => Err(ShapeError::Internal(format!(
                    "`{write}` assigns an `if` of {} arguments",
                    args.len()
                ))),
            },
            IRExpr::Call {
                func: ScalarFunc::Coalesce,
                args,
                ..
            } => Ok(BlobValue::Coalesce(
                args.iter()
                    .map(|arg| blob_value(write, arg, input, catalog, column))
                    .collect::<Result<_>>()?,
            )),
            other => Err(ShapeError::Internal(format!(
                "`{write}` assigns the Blob value `{other}`, which the type checker does not write"
            ))),
        }
    })
}

/// The node or edge type `variable` holds where `term` binds it: a node
/// type, or an edge type or selector (`A|B`).
fn binding_in(term: &Term, variable: &str) -> Option<Ty> {
    let reference = |ty: &ExprType| match ty {
        ExprType::Node {
            type_name,
            nullable,
        } => Some(Ty::Node {
            type_name: type_name.clone(),
            nullable: *nullable,
        }),
        ExprType::Edge {
            type_name,
            nullable,
        } => Some(Ty::Edge {
            type_name: type_name.clone(),
            nullable: *nullable,
        }),
        ExprType::Value { .. } | ExprType::ExactInteger { .. } => None,
    };
    crate::stack::deep(|| match term {
        Term::Unit | Term::RecRef { .. } => None,
        Term::Env(cols) => cols
            .iter()
            .find(|col| col.name == variable && col.ty.is_reference())
            .map(|col| col.ty.clone()),
        Term::Scan { var, ty } => (var == variable && ty.is_reference()).then(|| ty.clone()),
        Term::Expand {
            input,
            src,
            dst,
            src_type,
            dst_type,
            edges,
            edge_var,
            ..
        } => {
            if dst == variable {
                Some(Ty::Node {
                    type_name: dst_type.clone(),
                    nullable: false,
                })
            } else if src == variable {
                Some(Ty::Node {
                    type_name: src_type.clone(),
                    nullable: false,
                })
            } else if edge_var.as_deref() == Some(variable) {
                Some(Ty::Edge {
                    type_name: edges
                        .members()
                        .iter()
                        .map(|member| member.edge_type.as_str())
                        .collect::<Vec<_>>()
                        .join("|"),
                    nullable: false,
                })
            } else {
                binding_in(input, variable)
            }
        }
        Term::Project { input, items, .. } => items
            .iter()
            .find(|(name, _)| name == variable)
            .and_then(|(_, e)| reference(e.ty()))
            .or_else(|| binding_in(input, variable)),
        Term::Group { input, keys, .. } => keys
            .iter()
            .find(|(name, _)| name == variable)
            .and_then(|(_, e)| reference(e.ty()))
            .or_else(|| binding_in(input, variable)),
        Term::Filter { input, .. }
        | Term::Order { input, .. }
        | Term::Cut { input, .. }
        | Term::Score { input, .. }
        | Term::ApproxTopK { input, .. } => binding_in(input, variable),
        Term::Correlate { outer, inner, .. } => {
            binding_in(inner, variable).or_else(|| binding_in(outer, variable))
        }
        // An `optional` is the union of its pattern and a row of nulls where
        // the pattern matched nothing: a name either arm reads as nullable
        // is nullable.
        Term::Union { left, right } => {
            let (left, right) = (binding_in(left, variable), binding_in(right, variable));
            let nullable = left.iter().chain(&right).any(Ty::nullable);
            left.or(right).map(|ty| ty.with_nullable(nullable))
        }
        Term::Fuse { arms, .. } => arms.first().and_then(|arm| binding_in(arm, variable)),
        Term::Fix { body, .. } | Term::Call { body, .. } => binding_in(body, variable),
    })
}

/// A write group whose selection reads a table an earlier group of the same
/// write changes: its index and the table, `node:<T>` or `edge:<e>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EarlierGroupChange {
    pub group: usize,
    pub table: String,
}

/// The first group of `write` whose selection reads a table an earlier group
/// changes, or `None`. Every group selects on the write's one snapshot (the
/// GQ 3 RFC, section 2, the sinks), so such a group would read the table as
/// it was before the earlier group's effects, where reading it after them
/// would answer differently; the type checker refuses it (`T91`). A group
/// with no `match` selects nothing, and a statement's keyed effects (an
/// upsert, an endpoint resolved by key) see the statements before it, so
/// neither is a selection read.
pub fn earlier_group_change(write: &CoreWrite, catalog: &Catalog) -> Option<EarlierGroupChange> {
    let mut written = BTreeSet::new();
    for (group, g) in write.groups.iter().enumerate() {
        let mut read = BTreeSet::new();
        tables_read(&g.input, catalog, &mut read);
        if let Some(table) = read.intersection(&written).next() {
            return Some(EarlierGroupChange {
                group,
                table: table.clone(),
            });
        }
        for sink in &g.sinks {
            tables_written(sink, catalog, &mut written);
        }
    }
    None
}

/// The table keys a selection term reads: every node type it scans, expands
/// from or reaches (an interface as each type implementing it), and every
/// edge type it scans or traverses.
fn tables_read(term: &Term, catalog: &Catalog, out: &mut BTreeSet<String>) {
    for node in term.stored_nodes() {
        tables_read_node(node, catalog, out);
    }
}

/// One node's part of [`tables_read`].
fn tables_read_node(term: &Term, catalog: &Catalog, out: &mut BTreeSet<String>) {
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
