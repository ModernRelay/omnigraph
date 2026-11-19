//! A reference interpreter of the core IR (the GQ 3 RFC,
//! section 2), the GQT corpus's differential oracle.
//!
//! It evaluates a checked [`CoreRead`] directly from the operator table:
//! whole-table reads, nested loops, no index, planner, optimizer or cost
//! model. The graph it reads is a [`Graph`] built from the engine's JSONL
//! export of one branch, the narrowest public read surface a pinned snapshot
//! has; this crate depends on the compiler's IR alone (a test pins that).
//! It is not frozen: when the RFC changes an operator's semantics, this
//! changes with it, in the same change.
//!
//! Every core operator is evaluated, `Fix` by naive iteration to a
//! fixpoint, and so is `Call`, its arguments bound in the caller's scope. A
//! scan of a type the export does not carry (a catalogue type) and an
//! analysis the RFC does not fix (a folding outside the Latin letters) are
//! refused with [`Refusal::Unsupported`], never guessed.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::IRExpr;
use omnigraph_compiler::ir::core::{
    Agg, Col, CoreRead, Count, LexicalQuery, RRF_MAX_K, RelTy, ScoreKind, ScoreQuery, Term, Ty,
};
use omnigraph_compiler::ir::expr::{ScalarFunc, TermsMode};
use omnigraph_compiler::query::syntax::ArithOp;
use omnigraph_compiler::query::value::{AggFunc, BinaryOp, CompOp, Literal, NOW_PARAM_NAME};
use omnigraph_compiler::schema::vector::{VectorDistance, is_zero_vector};
use omnigraph_compiler::traversal::EDGE_TYPE_COLUMN;
use omnigraph_compiler::types::{Direction, ExprType, PropType, ScalarType};
use omnigraph_compiler::{check_datetime_literal, date_literal_days, datetime_literal_millis};
use serde_json::{Map, Value as Json};

mod registry;

/// Why the interpreter returned no rows.
#[derive(Debug, Clone, PartialEq)]
pub enum Refusal {
    /// A form this interpreter does not evaluate.
    Unsupported(String),
    /// A run-time refusal the RFC's `E` namespace names (`E1` overflow,
    /// `E2` a zero divisor, `E3` a non-finite float).
    Runtime(&'static str, String),
    /// The export or a parameter could not be read.
    Input(String),
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(what) => {
                write!(f, "the reference interpreter does not evaluate {what}")
            }
            Self::Runtime(code, what) => write!(f, "{code}: {what}"),
            Self::Input(what) => write!(f, "{what}"),
        }
    }
}

type R<T> = Result<T, Refusal>;

fn unsupported<T>(what: impl Into<String>) -> R<T> {
    Err(Refusal::Unsupported(what.into()))
}

/// One value of a relation's cell.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Str(String),
    List(Vec<Value>),
    /// A node or edge reference: its kind, type and identity.
    Ref {
        edge: bool,
        type_name: String,
        id: String,
    },
}

impl Value {
    fn from_json(v: &Json) -> Self {
        match v {
            Json::Null => Self::Null,
            Json::Bool(b) => Self::Bool(*b),
            Json::Number(n) => match n.as_i64() {
                Some(i) => Self::Int(i128::from(i)),
                None => match n.as_u64() {
                    Some(u) => Self::Int(i128::from(u)),
                    None => Self::Float(n.as_f64().unwrap_or(f64::NAN)),
                },
            },
            Json::String(s) => Self::Str(s.clone()),
            Json::Array(items) => Self::List(items.iter().map(Self::from_json).collect()),
            Json::Object(_) => Self::Str(v.to_string()),
        }
    }

    fn from_literal(l: &Literal) -> Self {
        match l {
            Literal::Null => Self::Null,
            Literal::String(s) | Literal::Date(s) => Self::Str(s.clone()),
            Literal::DateTime(s) => Self::Str(canonical_datetime(s).unwrap_or_else(|| s.clone())),
            Literal::Integer(i) => Self::Int(*i),
            Literal::Float(f) => Self::Float(*f),
            Literal::Bool(b) => Self::Bool(*b),
            Literal::List(items) => Self::List(items.iter().map(Self::from_literal).collect()),
        }
    }

    fn truthy(&self) -> bool {
        matches!(self, Self::Bool(true))
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int(i) => Some(*i as f64),
            Self::Float(f) => Some(*f),
            _ => None,
        }
    }
}

/// The sum of numbers: exact over integers, rounded once.
fn exact_sum(values: &[Value]) -> f64 {
    if values.iter().all(|v| matches!(v, Value::Int(_))) {
        values
            .iter()
            .map(|v| match v {
                Value::Int(i) => *i,
                _ => 0,
            })
            .sum::<i128>() as f64
    } else {
        values.iter().filter_map(Value::as_f64).sum()
    }
}

/// The total value order: null first, then by kind, numbers by value; two
/// floats by the total order, so `-0.0` sorts before `0.0`.
fn cmp_values(a: &Value, b: &Value) -> Ordering {
    use Value::*;
    if let (Float(x), Float(y)) = (a, b) {
        return x.total_cmp(y);
    }
    let rank = |v: &Value| match v {
        Null => 0,
        Bool(_) => 1,
        Int(_) | Float(_) => 2,
        Str(_) => 3,
        List(_) => 4,
        Ref { .. } => 5,
    };
    match (a, b) {
        (Bool(x), Bool(y)) => x.cmp(y),
        (Int(x), Int(y)) => x.cmp(y),
        (Int(_) | Float(_), Int(_) | Float(_)) => a
            .as_f64()
            .unwrap()
            .partial_cmp(&b.as_f64().unwrap())
            .unwrap_or(Ordering::Equal),
        (Str(x), Str(y)) => x.cmp(y),
        (List(x), List(y)) => {
            for (p, q) in x.iter().zip(y) {
                let o = cmp_values(p, q);
                if o != Ordering::Equal {
                    return o;
                }
            }
            x.len().cmp(&y.len())
        }
        (
            Ref {
                edge: e1,
                type_name: t1,
                id: i1,
            },
            Ref {
                edge: e2,
                type_name: t2,
                id: i2,
            },
        ) => (e1, t1, i1).cmp(&(e2, t2, i2)),
        _ => rank(a).cmp(&rank(b)),
    }
}

/// One order key's comparison: a null is placed by `nulls_first` whatever
/// the direction, and two non-null values compare by the total value order,
/// reversed when descending.
fn cmp_key(a: &Value, b: &Value, descending: bool, nulls_first: bool) -> Ordering {
    let null_first = |o: Ordering| if nulls_first { o } else { o.reverse() };
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => null_first(Ordering::Less),
        (_, Value::Null) => null_first(Ordering::Greater),
        _ if descending => cmp_values(a, b).reverse(),
        _ => cmp_values(a, b),
    }
}

#[derive(Debug, Clone)]
struct Entity {
    id: String,
    /// An edge's endpoint ids.
    ends: Option<(String, String)>,
    props: Map<String, Json>,
}

/// The rows of one branch, read from the engine's JSONL export.
#[derive(Debug, Clone, Default)]
pub struct Graph {
    nodes: HashMap<String, Vec<Entity>>,
    edges: HashMap<String, Vec<Entity>>,
    /// Each entity's position in its type's rows by id, so reading a
    /// reference's entity is a lookup, not a scan of its type. Scans still
    /// read the rows in export order.
    positions: HashMap<(bool, String), HashMap<String, usize>>,
}

impl Graph {
    /// Read an export (`Omnigraph::export_jsonl`): one node per
    /// `{"type", "data"}` line, one edge per `{"edge", "from", "to"}` line.
    pub fn from_export(catalog: &Catalog, jsonl: &str) -> R<Self> {
        let mut graph = Self::default();
        for (n, line) in jsonl.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let row: Json = serde_json::from_str(line)
                .map_err(|e| Refusal::Input(format!("export line {}: {e}", n + 1)))?;
            let data = row
                .get("data")
                .and_then(Json::as_object)
                .cloned()
                .unwrap_or_default();
            if let Some(t) = row.get("type").and_then(Json::as_str) {
                let node = catalog.node_types.get(t).ok_or_else(|| {
                    Refusal::Input(format!("export names unknown node type `{t}`"))
                })?;
                let id = match (&node.key, row.get("id").and_then(Json::as_str)) {
                    (_, Some(id)) => id.to_string(),
                    (Some(key), None) => key
                        .iter()
                        .map(|k| match data.get(k) {
                            Some(Json::String(s)) => s.clone(),
                            Some(other) => other.to_string(),
                            None => String::new(),
                        })
                        .collect::<Vec<_>>()
                        .join("\u{1f}"),
                    (None, None) => format!("#{n}"),
                };
                graph.nodes.entry(t.to_string()).or_default().push(Entity {
                    id,
                    ends: None,
                    props: data,
                });
            } else if let Some(e) = row.get("edge").and_then(Json::as_str) {
                let text = |k: &str| match row.get(k) {
                    Some(Json::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                    None => String::new(),
                };
                graph.edges.entry(e.to_string()).or_default().push(Entity {
                    id: row
                        .get("id")
                        .and_then(Json::as_str)
                        .map_or_else(|| format!("#{n}"), str::to_string),
                    ends: Some((text("from"), text("to"))),
                    props: data,
                });
            }
        }
        for (edge, types) in [(false, &graph.nodes), (true, &graph.edges)] {
            for (type_name, rows) in types {
                let mut by_id = HashMap::with_capacity(rows.len());
                for (i, entity) in rows.iter().enumerate() {
                    by_id.entry(entity.id.clone()).or_insert(i);
                }
                graph.positions.insert((edge, type_name.clone()), by_id);
            }
        }
        Ok(graph)
    }

    fn entity(&self, edge: bool, type_name: &str, id: &str) -> Option<&Entity> {
        let position = *self
            .positions
            .get(&(edge, type_name.to_string()))?
            .get(id)?;
        let rows = if edge { &self.edges } else { &self.nodes };
        rows.get(type_name)?.get(position)
    }

    fn node(&self, type_name: &str, id: &str) -> Option<&Entity> {
        self.entity(false, type_name, id)
    }

    fn edge(&self, type_name: &str, id: &str) -> Option<&Entity> {
        self.entity(true, type_name, id)
    }
}

type Row = Vec<(String, Value)>;

fn get<'r>(row: &'r Row, name: &str) -> Option<&'r Value> {
    row.iter().rev().find(|(n, _)| n == name).map(|(_, v)| v)
}

fn set(row: &mut Row, name: &str, value: Value) {
    match row.iter_mut().find(|(n, _)| n == name) {
        Some(slot) => slot.1 = value,
        None => row.push((name.to_string(), value)),
    }
}

/// A relation: its rows, and whether an `Order` (or a `Filter`, `Project`
/// or `Cut` over one) ordered them.
#[derive(Debug, Clone, Default)]
struct Rel {
    rows: Vec<Row>,
    ordered: bool,
}

/// The interpreter over one graph and one declaration's parameters.
pub struct Interpreter<'a> {
    catalog: &'a Catalog,
    graph: &'a Graph,
    params: HashMap<String, Value>,
    /// The caller bound `now()`'s reserved name, which binding refuses.
    binds_now: bool,
    /// The current value of each recursive read inside its `Fix`.
    rec: HashMap<String, Vec<Row>>,
}

impl<'a> Interpreter<'a> {
    pub fn new(catalog: &'a Catalog, graph: &'a Graph, params: &HashMap<String, Literal>) -> Self {
        Self {
            catalog,
            graph,
            binds_now: params.contains_key(NOW_PARAM_NAME),
            params: {
                let mut values: HashMap<String, Value> = params
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::from_literal(v)))
                    .collect();
                // `now()`: one instant for the whole read, as the engine binds it.
                values
                    .entry(NOW_PARAM_NAME.to_string())
                    .or_insert_with(|| Value::Str(utc_now()));
                values
            },
            rec: HashMap::new(),
        }
    }

    /// Run `read` and render its rows as the engine renders them: one JSON
    /// object per row, keyed by result column, a reference as its object.
    pub fn run(&mut self, read: &CoreRead) -> R<(Vec<Json>, bool)> {
        // `now()` reads the clock of the read, never a caller's value.
        if self.binds_now {
            return Err(Refusal::Input(format!(
                "param '{NOW_PARAM_NAME}': reserved for now() and cannot be bound"
            )));
        }
        // A parameter binds at its declared type: an omitted one to its
        // declared default, a nullable one without a default to null, and a
        // required one without a default is refused, as is a value its
        // declared type does not hold.
        for (name, ty) in &read.params {
            let value = match self.params.remove(name) {
                Some(value) => value,
                None => match read.defaults.iter().find(|(n, _)| n == name) {
                    Some((_, default)) => Value::from_literal(default),
                    None if ty.nullable => Value::Null,
                    None => {
                        return Err(Refusal::Input(format!("parameter '{name}' not provided")));
                    }
                },
            };
            let value = bind_param(name, value, ty)?;
            self.params.insert(name.clone(), value);
        }
        // A `percentile`'s fraction is a constant of the read: one outside
        // its domain refuses the read before it reads a row. A called
        // read's is its caller's argument, checked where its group is read.
        let mut pending = vec![&read.term];
        while let Some(term) = pending.pop() {
            if let Term::Group { aggs, .. } = term {
                for (_, agg) in aggs {
                    self.fraction(agg, &Vec::new())?;
                }
            }
            if !matches!(term, Term::Call { .. }) {
                pending.extend(term.children());
            }
        }
        let rel = self.term(&read.term, &Vec::new())?;
        let rows = rel
            .rows
            .iter()
            .map(|row| self.render(row, &read.result))
            .collect();
        Ok((rows, rel.ordered))
    }

    fn render(&self, row: &Row, result: &RelTy) -> Json {
        let mut out = Map::new();
        // A null cell is left out of its row, as the engine renders rows.
        for Col { name, ty } in &result.cols {
            let value = get(row, name).cloned().unwrap_or(Value::Null);
            if value != Value::Null {
                let f32_column = matches!(
                    ty,
                    Ty::Value(p) if matches!(p.scalar, ScalarType::F32 | ScalarType::Vector(_))
                );
                out.insert(name.clone(), self.json_as(&value, f32_column));
            }
        }
        Json::Object(out)
    }

    /// A value as JSON; an `F32` column's numbers at their shortest `F32`
    /// spelling, as the engine renders them.
    fn json_as(&self, v: &Value, f32_column: bool) -> Json {
        match v {
            Value::Float(f) if f32_column => format!("{}", *f as f32)
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map_or(Json::Null, Json::Number),
            Value::List(items) if f32_column => {
                Json::Array(items.iter().map(|i| self.json_as(i, true)).collect())
            }
            other => self.json(other),
        }
    }

    fn json(&self, v: &Value) -> Json {
        match v {
            Value::Null => Json::Null,
            Value::Bool(b) => Json::Bool(*b),
            Value::Int(i) => match (i64::try_from(*i), u64::try_from(*i)) {
                (Ok(i), _) => serde_json::json!(i),
                (_, Ok(u)) => serde_json::json!(u),
                _ => Json::Null,
            },
            Value::Float(f) => serde_json::Number::from_f64(*f).map_or(Json::Null, Json::Number),
            Value::Str(s) => Json::String(s.clone()),
            Value::List(items) => Json::Array(items.iter().map(|i| self.json(i)).collect()),
            Value::Ref {
                edge,
                type_name,
                id,
            } => {
                let entity = if *edge {
                    self.graph.edge(type_name, id)
                } else {
                    self.graph.node(type_name, id)
                };
                let mut obj = Map::new();
                obj.insert("@id".into(), Json::String(id.clone()));
                if let Some(e) = entity {
                    if let Some((src, dst)) = &e.ends {
                        obj.insert("@src".into(), Json::String(src.clone()));
                        obj.insert("@dst".into(), Json::String(dst.clone()));
                    }
                    // A bare reference's object leaves out its Vector and
                    // Blob properties, as the engine's does.
                    let props = if *edge {
                        self.catalog
                            .edge_types
                            .get(type_name)
                            .map(|t| &t.properties)
                    } else {
                        self.catalog
                            .node_types
                            .get(type_name)
                            .map(|t| &t.properties)
                    };
                    for (k, v) in &e.props {
                        let wide = props.and_then(|p| p.get(k)).is_some_and(|p| {
                            matches!(p.scalar, ScalarType::Vector(_) | ScalarType::Blob)
                        });
                        if !v.is_null() && !wide {
                            obj.insert(k.clone(), v.clone());
                        }
                    }
                }
                Json::Object(obj)
            }
        }
    }

    /// A `limit`'s row count: a non-negative integer.
    fn count(&self, k: &Count) -> R<usize> {
        self.count_within(k, 0, u64::MAX)
    }

    /// `k` bound within `min..=max`; a parameter outside it, or not an
    /// integer, is refused, as a literal outside it is a type error: a
    /// `limit` from zero, a `nearest` window from one, `rrf`'s constant
    /// within `1..=RRF_MAX_K` (section 4, Errors: an option out of range).
    fn count_within(&self, k: &Count, min: u64, max: u64) -> R<usize> {
        match k {
            Count::Literal(n) => Ok(*n as usize),
            Count::Param(p) => match self.params.get(p) {
                Some(Value::Int(n)) if (i128::from(min)..=i128::from(max)).contains(n) => {
                    Ok(*n as usize)
                }
                other => Err(Refusal::Input(format!(
                    "`${p}` is no count in {min}..={max}: {other:?}"
                ))),
            },
        }
    }

    // -----------------------------------------------------------------
    // Operators (section 2's table).

    fn term(&mut self, t: &Term, env: &Row) -> R<Rel> {
        omnigraph_compiler::stack::deep(|| {
            Ok(match t {
                Term::Unit => Rel {
                    rows: vec![Vec::new()],
                    ordered: false,
                },
                Term::Env(cols) => Rel {
                    rows: vec![
                        cols.iter()
                            .map(|c| {
                                (
                                    c.name.clone(),
                                    get(env, &c.name).cloned().unwrap_or(Value::Null),
                                )
                            })
                            .collect(),
                    ],
                    ordered: false,
                },
                Term::Scan { var, ty } => {
                    let (edge, type_name) = match ty {
                        Ty::Node { type_name, .. } => (false, type_name),
                        Ty::Edge { type_name, .. } => (true, type_name),
                        Ty::Value(_) => return unsupported("a scan of a value column"),
                    };
                    let mut rows = Vec::new();
                    for t in type_name.split('|') {
                        let declared = if edge {
                            self.catalog.edge_types.contains_key(t)
                        } else {
                            self.catalog.node_types.contains_key(t)
                        };
                        if !declared {
                            // A catalogue type (`@Operator`) is served by the
                            // engine, not stored, so the export carries no row;
                            // an interface's polymorphic scan is not evaluated.
                            return unsupported(format!(
                                "a scan of `{t}`, which the export does not carry"
                            ));
                        }
                        let entities = if edge {
                            self.graph.edges.get(t)
                        } else {
                            self.graph.nodes.get(t)
                        };
                        for e in entities.into_iter().flatten() {
                            rows.push(vec![(
                                var.clone(),
                                Value::Ref {
                                    edge,
                                    type_name: t.to_string(),
                                    id: e.id.clone(),
                                },
                            )]);
                        }
                    }
                    Rel {
                        rows,
                        ordered: false,
                    }
                }
                Term::Expand {
                    input,
                    src,
                    dst,
                    edges,
                    min_hops,
                    max_hops,
                    edge_var,
                    ..
                } => {
                    let input = self.term(input, env)?;
                    let mut rows = Vec::new();
                    for row in input.rows {
                        let Some(Value::Ref { id, .. }) = get(&row, src).cloned() else {
                            continue;
                        };
                        let steps = |from: &str| -> Vec<(String, String, String, String)> {
                            // (edge type, edge id, neighbour type, neighbour id)
                            let mut out = Vec::new();
                            for m in edges.members() {
                                let Some(et) = self.catalog.edge_types.get(&m.edge_type) else {
                                    continue;
                                };
                                for e in self.graph.edges.get(&m.edge_type).into_iter().flatten() {
                                    let (a, b) = e.ends.as_ref().unwrap();
                                    let out_ok = a == from
                                        && matches!(m.direction, Direction::Out | Direction::Both);
                                    let in_ok = b == from
                                        && matches!(m.direction, Direction::In | Direction::Both);
                                    if out_ok {
                                        out.push((
                                            et.name.clone(),
                                            e.id.clone(),
                                            et.to_type.clone(),
                                            b.clone(),
                                        ));
                                    }
                                    if in_ok && !(out_ok && a == b) {
                                        out.push((
                                            et.name.clone(),
                                            e.id.clone(),
                                            et.from_type.clone(),
                                            a.clone(),
                                        ));
                                    }
                                }
                            }
                            out
                        };
                        if let Some(k) = edge_var {
                            for (et, eid, nt, nid) in steps(&id) {
                                // `R + [$y] + [$k]`: the column order the
                                // tie rule reads.
                                let mut r = row.clone();
                                r.push((
                                    dst.clone(),
                                    Value::Ref {
                                        edge: false,
                                        type_name: nt,
                                        id: nid,
                                    },
                                ));
                                r.push((
                                    k.clone(),
                                    Value::Ref {
                                        edge: true,
                                        type_name: et,
                                        id: eid,
                                    },
                                ));
                                rows.push(r);
                            }
                            continue;
                        }
                        // Set semantics over endpoint pairs: each node at its
                        // shortest distance, kept when that distance is in
                        // `min..=max`. The source sits at distance 0, so a
                        // cycle never returns it; a self-loop edge returns it
                        // only to the single hop, and the walk does not
                        // continue from it.
                        let src_type = match get(&row, src) {
                            Some(Value::Ref { type_name, .. }) => type_name.clone(),
                            _ => String::new(),
                        };
                        let mut distance: BTreeMap<(String, String), u32> = BTreeMap::new();
                        distance.insert((src_type.clone(), id.clone()), 0);
                        let mut self_loop = false;
                        let mut frontier = vec![id.clone()];
                        let mut hop = 0u32;
                        while !frontier.is_empty() && max_hops.is_none_or(|m| hop < m) {
                            hop += 1;
                            let mut next = Vec::new();
                            for at in &frontier {
                                for (_, _, nt, nid) in steps(at) {
                                    if hop == 1 && nt == src_type && nid == id {
                                        self_loop = true;
                                        continue;
                                    }
                                    if let std::collections::btree_map::Entry::Vacant(slot) =
                                        distance.entry((nt, nid.clone()))
                                    {
                                        slot.insert(hop);
                                        next.push(nid);
                                    }
                                }
                            }
                            frontier = next;
                        }
                        if self_loop && *min_hops <= 1 && *max_hops == Some(1) {
                            distance.insert((src_type, id.clone()), 1);
                        }
                        let seen: BTreeMap<(String, String), u32> = distance
                            .into_iter()
                            .filter(|(_, d)| {
                                *d >= (*min_hops).max(1) && max_hops.is_none_or(|m| *d <= m)
                            })
                            .collect();
                        for (nt, nid) in seen.into_keys() {
                            let mut r = row.clone();
                            r.push((
                                dst.clone(),
                                Value::Ref {
                                    edge: false,
                                    type_name: nt,
                                    id: nid,
                                },
                            ));
                            rows.push(r);
                        }
                    }
                    Rel {
                        rows,
                        ordered: false,
                    }
                }
                Term::Filter {
                    input,
                    pred,
                    fails: false,
                } if let Some(sunk) = sink(input, pred) => self.term(&sunk, env)?,
                Term::Filter { input, pred, .. } => {
                    let input = self.term(input, env)?;
                    let mut rows = Vec::new();
                    for row in input.rows {
                        if self.expr(pred, &row, env)?.truthy() {
                            rows.push(row);
                        }
                    }
                    Rel {
                        rows,
                        ordered: input.ordered,
                    }
                }
                Term::Project {
                    input, items, keep, ..
                } => {
                    let input = self.term(input, env)?;
                    let mut rows = Vec::new();
                    for row in input.rows {
                        let mut out = if *keep { row.clone() } else { Vec::new() };
                        for (name, e) in items {
                            let v = self.expr(e, &row, env)?;
                            set(&mut out, name, v);
                        }
                        rows.push(out);
                    }
                    Rel {
                        rows,
                        ordered: input.ordered,
                    }
                }
                Term::Group { input, keys, aggs } => {
                    let input = self.term(input, env)?;
                    let mut groups: Vec<(Vec<Value>, Vec<Row>)> = Vec::new();
                    for row in input.rows {
                        let key = keys
                            .iter()
                            .map(|(_, e)| self.expr(e, &row, env))
                            .collect::<R<Vec<_>>>()?;
                        match groups.iter_mut().find(|(k, _)| {
                            k.len() == key.len() && k.iter().zip(&key).all(|(p, q)| identical(p, q))
                        }) {
                            Some((_, members)) => members.push(row),
                            None => groups.push((key, vec![row])),
                        }
                    }
                    if keys.is_empty() && groups.is_empty() {
                        groups.push((Vec::new(), Vec::new()));
                    }
                    for (_, agg) in aggs {
                        self.fraction(agg, env)?;
                    }
                    let mut rows = Vec::new();
                    for (key, members) in groups {
                        let mut out: Row = keys.iter().map(|(n, _)| n.clone()).zip(key).collect();
                        for (name, agg) in aggs {
                            let v = self.aggregate(agg, &members, env)?;
                            out.push((name.clone(), v));
                        }
                        rows.push(out);
                    }
                    Rel {
                        rows,
                        ordered: false,
                    }
                }
                Term::Correlate { outer, inner, .. } => {
                    let outer = self.term(outer, env)?;
                    let mut rows = Vec::new();
                    for row in outer.rows {
                        let mut inner_env = env.clone();
                        for (n, v) in &row {
                            set(&mut inner_env, n, v.clone());
                        }
                        for r in self.term(inner, &inner_env)?.rows {
                            let mut out = row.clone();
                            for (n, v) in r {
                                if get(&row, &n).is_none() {
                                    out.push((n, v));
                                }
                            }
                            rows.push(out);
                        }
                    }
                    Rel {
                        rows,
                        ordered: false,
                    }
                }
                Term::Union { left, right } => {
                    // The arms match by column name, in the left arm's column
                    // order.
                    let names = columns(left);
                    let mut l = self.term(left, env)?;
                    let r = self.term(right, env)?;
                    for row in r.rows {
                        let aligned = names
                            .iter()
                            .map(|name| match get(&row, name) {
                                Some(v) => Ok((name.clone(), v.clone())),
                                None => Err(Refusal::Unsupported(format!(
                                    "a union arm has no column `{name}`"
                                ))),
                            })
                            .collect::<R<Row>>()?;
                        l.rows.push(aligned);
                    }
                    l.ordered = false;
                    l
                }
                Term::Order { input, keys } => {
                    let input = self.term(input, env)?;
                    let mut keyed = Vec::new();
                    for row in input.rows {
                        let k = keys
                            .iter()
                            .map(|key| self.expr(&key.expr, &row, env))
                            .collect::<R<Vec<_>>>()?;
                        keyed.push((k, row));
                    }
                    keyed.sort_by(|(ka, ra), (kb, rb)| {
                        for ((a, b), key) in ka.iter().zip(kb).zip(keys) {
                            let o = cmp_key(a, b, key.descending, key.places_nulls_first());
                            if o != Ordering::Equal {
                                return o;
                            }
                        }
                        tie(ra, rb)
                    });
                    Rel {
                        rows: keyed.into_iter().map(|(_, r)| r).collect(),
                        ordered: true,
                    }
                }
                Term::Cut { input, k } => {
                    let mut input = self.term(input, env)?;
                    if !input.ordered {
                        input.rows.sort_by(tie);
                    }
                    input.rows.truncate(self.count(k)?);
                    Rel {
                        rows: input.rows,
                        ordered: true,
                    }
                }
                Term::Score {
                    input,
                    column,
                    property,
                    kind,
                    query,
                    out,
                } => {
                    let input = self.term(input, env)?;
                    let mut rows = Vec::new();
                    match (kind, query) {
                        (ScoreKind::Distance, ScoreQuery::Vector(q)) => {
                            let q = self.vector(q, env)?;
                            for mut row in input.rows {
                                let Some(d) = self.field_distance(&row, column, property, &q)?
                                else {
                                    continue;
                                };
                                row.push((out.clone(), Value::Float(finite_distance(d)?)));
                                rows.push(row);
                            }
                        }
                        // An empty population scores nothing, whatever the field's
                        // analysis.
                        (ScoreKind::Bm25, ScoreQuery::Lexical(_)) if input.rows.is_empty() => {}
                        (ScoreKind::Bm25, ScoreQuery::Lexical(lex)) => {
                            let scorer = self.bm25(&input.rows, column, property, lex, env)?;
                            for mut row in input.rows {
                                if let Some(s) = scorer.score(self, &row, column, property)? {
                                    row.push((out.clone(), Value::Float(s)));
                                    rows.push(row);
                                }
                            }
                        }
                        _ => return unsupported("a score whose query does not match its kind"),
                    }
                    Rel {
                        rows,
                        ordered: false,
                    }
                }
                Term::ApproxTopK {
                    input,
                    column,
                    property,
                    query,
                    k,
                    out,
                    ..
                } => {
                    // Exact: every target's distance, the `k` nearest kept.
                    let input = self.term(input, env)?;
                    let q = self.vector(query, env)?;
                    let k = self.count_within(k, 1, u64::MAX)?;
                    let mut best: Vec<(f64, Value)> = Vec::new();
                    for row in &input.rows {
                        let Some(target) = get(row, column).cloned() else {
                            continue;
                        };
                        if best.iter().any(|(_, t)| t == &target) {
                            continue;
                        }
                        if let Some(d) = self.field_distance(row, column, property, &q)? {
                            best.push((d, target));
                        }
                    }
                    best.sort_by(|a, b| {
                        a.0.partial_cmp(&b.0)
                            .unwrap_or(Ordering::Equal)
                            .then(cmp_values(&a.1, &b.1))
                    });
                    best.truncate(k);
                    // A kept target's distance must be finite; an overflowing
                    // one past the window was never selected.
                    for (d, _) in &best {
                        finite_distance(*d)?;
                    }
                    let mut rows = Vec::new();
                    for mut row in input.rows {
                        let target = get(&row, column).cloned();
                        if let Some((d, _)) = best.iter().find(|(_, t)| Some(t) == target.as_ref())
                        {
                            row.push((out.clone(), Value::Float(*d)));
                            rows.push(row);
                        }
                    }
                    Rel {
                        rows,
                        ordered: false,
                    }
                }
                Term::Fuse {
                    arms,
                    target,
                    k,
                    weights,
                    out,
                } => {
                    let k = self.count_within(k, 1, RRF_MAX_K)? as f64;
                    let mut fused: Vec<(Value, f64)> = Vec::new();
                    for (i, arm) in arms.iter().enumerate() {
                        let best = self.ranked_targets(arm, target, env)?;
                        let w = weights.get(i).copied().unwrap_or(1.0);
                        for (rank, t) in best.into_iter().enumerate() {
                            let add = w / (k + (rank + 1) as f64);
                            match fused.iter_mut().find(|(f, _)| *f == t) {
                                Some((_, s)) => *s += add,
                                None => fused.push((t, add)),
                            }
                        }
                    }
                    Rel {
                        rows: fused
                            .into_iter()
                            .map(|(t, s)| vec![(target.clone(), t), (out.clone(), Value::Float(s))])
                            .collect(),
                        ordered: false,
                    }
                }
                Term::Fix {
                    name, body, rounds, ..
                } => {
                    let mut current: Vec<Row> = Vec::new();
                    let limit = rounds.unwrap_or(1_000);
                    for _ in 0..limit {
                        self.rec.insert(name.clone(), current.clone());
                        let next = self.term(body, env)?.rows;
                        let mut merged = current.clone();
                        for r in next {
                            if !merged.iter().any(|m| same_row(m, &r)) {
                                merged.push(r);
                            }
                        }
                        if merged.len() == current.len() {
                            break;
                        }
                        current = merged;
                    }
                    self.rec.remove(name);
                    Rel {
                        rows: current,
                        ordered: false,
                    }
                }
                Term::RecRef { name, .. } => Rel {
                    rows: self.rec.get(name).cloned().unwrap_or_default(),
                    ordered: false,
                },
                Term::Call { bindings, body, .. } => {
                    // Every argument is evaluated in the caller's scope, the
                    // enclosing row included, before any is bound: binding, not
                    // substitution.
                    let values = bindings
                        .iter()
                        .map(|(name, e)| Ok((name.clone(), self.expr(e, &Vec::new(), env)?)))
                        .collect::<R<Vec<_>>>()?;
                    let mut saved = Vec::new();
                    for (name, v) in values {
                        saved.push((name.clone(), self.params.insert(name, v)));
                    }
                    // The body is closed: it reads no column of the caller.
                    let out = self.term(body, &Vec::new());
                    for (name, old) in saved {
                        match old {
                            Some(v) => self.params.insert(name, v),
                            None => self.params.remove(&name),
                        };
                    }
                    out?
                }
            })
        })
    }

    /// An `rrf` arm's distinct targets in rank order: a ranking function's
    /// by its own score in its kind's direction, then the target; an inline
    /// read with an `order` by each target's first row in that order.
    fn ranked_targets(&mut self, arm: &Term, target: &str, env: &Row) -> R<Vec<Value>> {
        let scored = match arm {
            Term::ApproxTopK { out, .. } => Some((out.clone(), false)),
            Term::Score {
                out,
                kind: ScoreKind::Distance,
                ..
            } => Some((out.clone(), false)),
            Term::Score { out, .. } | Term::Fuse { out, .. } => Some((out.clone(), true)),
            _ => None,
        };
        let rel = self.term(arm, env)?;
        let Some((score, descending)) = scored else {
            if !rel.ordered {
                return unsupported("an `rrf` arm that is not a ranking or an ordered read");
            }
            let mut targets: Vec<Value> = Vec::new();
            for row in &rel.rows {
                if let Some(t) = get(row, target)
                    && !targets.contains(t)
                {
                    targets.push(t.clone());
                }
            }
            return Ok(targets);
        };
        let mut best: Vec<(Value, f64)> = Vec::new();
        for row in &rel.rows {
            let (Some(t), Some(s)) = (get(row, target), get(row, &score).and_then(Value::as_f64))
            else {
                continue;
            };
            if !best.iter().any(|(b, _)| b == t) {
                best.push((t.clone(), s));
            }
        }
        best.sort_by(|a, b| {
            let o = a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal);
            (if descending { o.reverse() } else { o }).then(cmp_values(&a.0, &b.0))
        });
        Ok(best.into_iter().map(|(t, _)| t).collect())
    }

    /// `percentile`'s fraction `p`: a number in [0, 1], or `E5` (a null
    /// parameter included). `None` for every other aggregate.
    fn fraction(&self, agg: &Agg, env: &Row) -> R<Option<f64>> {
        let Some(p) = &agg.fraction else {
            return Ok(None);
        };
        match self.expr(p, &Vec::new(), env)? {
            Value::Null => Err(Refusal::Runtime(
                "E5",
                "`percentile`'s fraction p is null".into(),
            )),
            v => match v.as_f64() {
                Some(p) if (0.0..=1.0).contains(&p) => Ok(Some(p)),
                Some(p) => Err(Refusal::Runtime(
                    "E5",
                    format!("`percentile`'s fraction p is {p}, outside [0, 1]"),
                )),
                None => Err(Refusal::Input(format!("a percentile fraction is {v:?}"))),
            },
        }
    }

    fn aggregate(&self, agg: &Agg, rows: &[Row], env: &Row) -> R<Value> {
        let Some(arg) = &agg.arg else {
            return Ok(Value::Int(rows.len() as i128));
        };
        let mut values = Vec::new();
        for row in rows {
            let v = self.expr(arg, row, env)?;
            if v != Value::Null && !(agg.distinct && values.iter().any(|w| identical(w, &v))) {
                values.push(v);
            }
        }
        Ok(match agg.func {
            AggFunc::Count => Value::Int(values.len() as i128),
            // A group's non-null values in the fixed value order; empty
            // input is the empty list.
            AggFunc::Collect => {
                values.sort_by(cmp_values);
                Value::List(values)
            }
            _ if values.is_empty() => Value::Null,
            // An integer sum is exact, rounded to `F64` once.
            AggFunc::Sum => Value::Float(exact_sum(&values)),
            // An integer mean divides the exact total once; a float mean
            // sums in `F64`, as the plan's `float64` leaf names.
            AggFunc::Avg => Value::Float(exact_sum(&values) / values.len() as f64),
            AggFunc::Min => values.into_iter().min_by(cmp_values).unwrap(),
            AggFunc::Max => values.into_iter().max_by(cmp_values).unwrap(),
            // The group's values ascending in the fixed value order, each
            // the nearest `F64`, so the answer does not depend on the order
            // the rows arrive in.
            AggFunc::Stddev | AggFunc::Percentile => {
                values.sort_by(cmp_values);
                let x = values
                    .iter()
                    .map(|v| {
                        v.as_f64()
                            .ok_or_else(|| Refusal::Input(format!("{} of {v:?}", agg.func)))
                    })
                    .collect::<R<Vec<f64>>>()?;
                let n = x.len();
                match self.fraction(agg, env)? {
                    // The sample deviation: squared differences from the
                    // mean over n − 1, each sum added in the sorted order;
                    // null over one value.
                    None if n < 2 => Value::Null,
                    None => {
                        let mut total = 0.0;
                        for v in &x {
                            total += v;
                        }
                        let mean = total / n as f64;
                        let mut squares = 0.0;
                        for v in &x {
                            squares += (v - mean) * (v - mean);
                        }
                        Value::Float((squares / (n - 1) as f64).sqrt())
                    }
                    // PERCENTILE_CONT: the value at the index p · (n − 1)
                    // when it is whole, otherwise interpolated linearly
                    // between its two neighbors.
                    Some(p) => {
                        let h = p * (n - 1) as f64;
                        let lo = h.floor();
                        let below = x[lo as usize];
                        if h == lo {
                            Value::Float(below)
                        } else {
                            let above = x[lo as usize + 1];
                            Value::Float(below + (above - below) * (h - lo))
                        }
                    }
                }
            }
        })
    }

    // -----------------------------------------------------------------
    // Scores.

    fn vector(&self, q: &IRExpr, env: &Row) -> R<Vec<f64>> {
        match self.expr(q, &Vec::new(), env)? {
            Value::List(items) => items
                .iter()
                .map(|v| {
                    v.as_f64()
                        .ok_or_else(|| Refusal::Input("a non-numeric vector element".into()))
                })
                .collect(),
            Value::Str(_) => {
                unsupported("a String `nearest` query, which needs an embedding provider")
            }
            other => Err(Refusal::Input(format!("a query vector is {other:?}"))),
        }
    }

    fn entity(&self, row: &Row, column: &str) -> Option<&Entity> {
        match get(row, column)? {
            Value::Ref {
                edge: false,
                type_name,
                id,
            } => self.graph.node(type_name, id),
            Value::Ref {
                edge: true,
                type_name,
                id,
            } => self.graph.edge(type_name, id),
            _ => None,
        }
    }

    fn field_vector(&self, row: &Row, column: &str, property: &str) -> Option<Vec<f64>> {
        match self.entity(row, column)?.props.get(property)? {
            Json::Array(items) => items.iter().map(Json::as_f64).collect(),
            _ => None,
        }
    }

    /// The distance of the row's vector `property` to `q` under the
    /// field's declared distance, rounded to `F32` as the engine reports it:
    /// `None` for a null vector and, under `cosine`, a zero vector, which
    /// drops the row; a zero `q` on a `cosine` field is refused. A distance
    /// from finite components can overflow `F32`: the caller refuses it
    /// ([`finite_distance`]) where it scores or selects the row.
    fn field_distance(&self, row: &Row, column: &str, property: &str, q: &[f64]) -> R<Option<f64>> {
        let distance = match get(row, column) {
            Some(Value::Ref {
                edge: false,
                type_name,
                ..
            }) => self
                .catalog
                .node_types
                .get(type_name)
                .map(|node_type| node_type.vector_distance(property))
                .unwrap_or_default(),
            _ => VectorDistance::L2,
        };
        let q: Vec<f32> = q.iter().map(|x| *x as f32).collect();
        if distance.refuses_zero_vector() && is_zero_vector(&q) {
            return Err(Refusal::Input(format!(
                "a zero query vector on a {distance} field"
            )));
        }
        let Some(v) = self.field_vector(row, column, property) else {
            return Ok(None);
        };
        let v: Vec<f32> = v.iter().map(|x| *x as f32).collect();
        Ok(distance.evaluate(&v, &q).map(f64::from))
    }

    fn field_text(&self, row: &Row, column: &str, property: &str) -> Option<String> {
        self.entity(row, column)?
            .props
            .get(property)?
            .as_str()
            .map(str::to_string)
    }

    fn bm25(
        &self,
        rows: &[Row],
        column: &str,
        property: &str,
        lex: &LexicalQuery,
        env: &Row,
    ) -> R<Bm25> {
        let text = match self.expr(&lex.text, &Vec::new(), env)? {
            Value::Str(s) => s,
            other => return Err(Refusal::Input(format!("a lexical query is {other:?}"))),
        };
        let max_edits = self.max_edits(Some(&lex.max_edits), 0, &Vec::new(), env)?;
        // The field corpus: every entity of the type owning the property
        // with a non-empty analyzed value, not only the population.
        let type_name = rows.iter().find_map(|r| match get(r, column) {
            Some(Value::Ref { type_name, .. }) => Some(type_name.clone()),
            _ => None,
        });
        let analysis = Analysis::of(self.catalog, type_name.as_deref(), property)?;
        let mut corpus = Vec::new();
        for e in type_name
            .as_deref()
            .and_then(|t| self.graph.nodes.get(t))
            .into_iter()
            .flatten()
        {
            if let Some(text) = e.props.get(property).and_then(Json::as_str) {
                let doc = analysis.tokens(text)?;
                if !doc.is_empty() {
                    corpus.push(doc);
                }
            }
        }
        Ok(Bm25::new(
            analysis.tokens(&text)?,
            lex.mode_all,
            max_edits,
            analysis,
            corpus,
        ))
    }

    /// An edit budget: the written `max_edits`, else `default`.
    fn max_edits(&self, e: Option<&IRExpr>, default: usize, row: &Row, env: &Row) -> R<usize> {
        match e {
            None => Ok(default),
            Some(e) => match self.expr(e, row, env)? {
                Value::Int(n) if (0..=2).contains(&n) => Ok(n as usize),
                other => Err(Refusal::Input(format!("an edit budget is {other:?}"))),
            },
        }
    }

    /// The analysis of the property `field` reads: the profile its owner's
    /// full-text index declares.
    fn field_analysis(&self, field: &IRExpr, row: &Row, env: &Row) -> R<Analysis> {
        let IRExpr::PropAccess {
            variable, property, ..
        } = field
        else {
            return unsupported(format!("a lexical call over `{field}`, not a property"));
        };
        let owner = match get(row, variable).or_else(|| get(env, variable)) {
            Some(Value::Ref {
                edge: false,
                type_name,
                ..
            }) => Some(type_name.as_str()),
            _ => None,
        };
        Analysis::of(self.catalog, owner, property)
    }

    /// `match_terms(field, terms(q, mode, max_edits))`: every (`all`) or some
    /// (`any`) analyzed query term has a document term within the budget. A
    /// null or token-empty document does not match: the predicate is false.
    /// `q` is a non-null String (section 4, The lexical query): a parameter
    /// bound to null is refused.
    #[allow(clippy::too_many_arguments)]
    fn lexical(
        &self,
        field: &IRExpr,
        query: &IRExpr,
        all: bool,
        max_edits: usize,
        row: &Row,
        env: &Row,
    ) -> R<Value> {
        let analysis = self.field_analysis(field, row, env)?;
        let q = match self.expr(query, row, env)? {
            Value::Str(q) => analysis.tokens(&q)?,
            other => return Err(Refusal::Input(format!("a lexical query is {other:?}"))),
        };
        let doc = match self.expr(field, row, env)? {
            Value::Str(f) => analysis.tokens(&f)?,
            _ => return Ok(Value::Bool(false)),
        };
        if doc.is_empty() {
            return Ok(Value::Bool(false));
        }
        let hit = |t: &String| doc.iter().any(|w| levenshtein(w, t) <= max_edits);
        Ok(Value::Bool(if all {
            q.iter().all(hit)
        } else {
            q.iter().any(hit)
        }))
    }

    // -----------------------------------------------------------------
    // Expressions.

    fn expr(&self, e: &IRExpr, row: &Row, env: &Row) -> R<Value> {
        omnigraph_compiler::stack::deep(|| {
            let column = |name: &str| get(row, name).or_else(|| get(env, name)).cloned();
            Ok(match e {
                IRExpr::Literal(l, _) => Value::from_literal(l),
                IRExpr::Param(p, _) => self.params.get(p).cloned().unwrap_or(Value::Null),
                IRExpr::Variable(v, _) | IRExpr::AliasRef(v, _) => column(v).unwrap_or(Value::Null),
                IRExpr::PropAccess {
                    variable, property, ..
                } => {
                    let Some(r @ Value::Ref { .. }) = column(variable) else {
                        return Ok(Value::Null);
                    };
                    let Value::Ref { type_name, id, .. } = &r else {
                        unreachable!()
                    };
                    let sys = self.catalog.system_columns;
                    if property == sys.id {
                        return Ok(Value::Str(id.clone()));
                    }
                    if property == EDGE_TYPE_COLUMN {
                        return Ok(Value::Str(type_name.clone()));
                    }
                    let probe = vec![("__probe".to_string(), r.clone())];
                    let Some(entity) = self.entity(&probe, "__probe") else {
                        return Ok(Value::Null);
                    };
                    if let Some((src, dst)) = &entity.ends {
                        if property == sys.src {
                            return Ok(Value::Str(src.clone()));
                        }
                        if property == sys.dst {
                            return Ok(Value::Str(dst.clone()));
                        }
                    }
                    let declared = if let Some((_, _)) = &entity.ends {
                        self.catalog
                            .edge_types
                            .get(type_name)
                            .and_then(|t| t.properties.get(property))
                    } else {
                        self.catalog
                            .node_types
                            .get(type_name)
                            .and_then(|t| t.properties.get(property))
                    };
                    entity.props.get(property).map_or(Value::Null, |json| {
                        typed(Value::from_json(json), declared.map(|p| p.scalar))
                    })
                }
                IRExpr::Not(inner, _) => match self.expr(inner, row, env)? {
                    Value::Bool(b) => Value::Bool(!b),
                    _ => Value::Null,
                },
                IRExpr::IsNull { expr, negated, .. } => {
                    Value::Bool((self.expr(expr, row, env)? == Value::Null) != *negated)
                }
                IRExpr::Cast { expr, ty } => cast(self.expr(expr, row, env)?, ty)?,
                // Negation is `0 - x` in the operand's type: it overflows where
                // the negated value leaves the type (`-i64::MIN`), `E1`.
                IRExpr::Neg { expr, ty } => match self.expr(expr, row, env)? {
                    Value::Int(i) => arith(ArithOp::Sub, Value::Int(0), Value::Int(i), ty)?,
                    Value::Float(f) => Value::Float(-f),
                    _ => Value::Null,
                },
                IRExpr::Binary {
                    left,
                    op,
                    right,
                    ty,
                } => {
                    let l = self.expr(left, row, env)?;
                    // `and` and `or` are guards, left to right (section 2,
                    // scalar expressions): `a and b` evaluates `b` only where `a`
                    // is not false, `a or b` only where `a` is not true, so a
                    // right side that can fail never runs on a decided row.
                    match op {
                        BinaryOp::And => {
                            if l == Value::Bool(false) {
                                return Ok(Value::Bool(false));
                            }
                            match (l, self.expr(right, row, env)?) {
                                (_, Value::Bool(false)) => Value::Bool(false),
                                (Value::Bool(true), Value::Bool(true)) => Value::Bool(true),
                                _ => Value::Null,
                            }
                        }
                        BinaryOp::Or => {
                            if l == Value::Bool(true) {
                                return Ok(Value::Bool(true));
                            }
                            match (l, self.expr(right, row, env)?) {
                                (_, Value::Bool(true)) => Value::Bool(true),
                                (Value::Bool(false), Value::Bool(false)) => Value::Bool(false),
                                _ => Value::Null,
                            }
                        }
                        BinaryOp::Compare(op) => compare(*op, l, self.expr(right, row, env)?),
                        BinaryOp::Arith(op) => arith(*op, l, self.expr(right, row, env)?, ty)?,
                    }
                }
                // The registry's two lazy rows: `if` evaluates its condition and
                // then only the branch it selects, `coalesce` its arguments left
                // to right up to the first non-null, so a branch not taken never
                // fails.
                IRExpr::Call {
                    func: ScalarFunc::If,
                    args,
                    ..
                } => match self.expr(&args[0], row, env)? {
                    Value::Bool(true) => self.expr(&args[1], row, env)?,
                    _ => self.expr(&args[2], row, env)?,
                },
                IRExpr::Call {
                    func: ScalarFunc::Coalesce,
                    args,
                    ..
                } => {
                    for a in args {
                        let v = self.expr(a, row, env)?;
                        if v != Value::Null {
                            return Ok(v);
                        }
                    }
                    Value::Null
                }
                IRExpr::Call { func, args, .. } => {
                    let vals = args
                        .iter()
                        .map(|a| self.expr(a, row, env))
                        .collect::<R<Vec<_>>>()?;
                    match func {
                        ScalarFunc::If | ScalarFunc::Coalesce => {
                            unreachable!("evaluated lazily above")
                        }
                        ScalarFunc::Concat => {
                            let mut out = String::new();
                            for v in vals {
                                match v {
                                    Value::Str(s) => out.push_str(&s),
                                    Value::Null => return Ok(Value::Null),
                                    other => out.push_str(&format!("{other:?}")),
                                }
                            }
                            Value::Str(out)
                        }
                        // A list holding parameters: its elements' values, in
                        // order, at the list's element type.
                        ScalarFunc::List => typed(
                            Value::List(vals),
                            match e.ty() {
                                ExprType::Value { scalar, .. } => Some(*scalar),
                                _ => None,
                            },
                        ),
                        ScalarFunc::Lower
                        | ScalarFunc::Upper
                        | ScalarFunc::Trim
                        | ScalarFunc::LTrim
                        | ScalarFunc::RTrim
                        | ScalarFunc::Length
                        | ScalarFunc::Left
                        | ScalarFunc::Right
                        | ScalarFunc::Substring
                        | ScalarFunc::Abs
                        | ScalarFunc::Floor
                        | ScalarFunc::Ceil
                        | ScalarFunc::Round
                        | ScalarFunc::Sqrt
                        | ScalarFunc::Exp
                        | ScalarFunc::Ln
                        | ScalarFunc::Log10
                        | ScalarFunc::Power
                        | ScalarFunc::Size
                        | ScalarFunc::Cast => {
                            let arg_types: Vec<&ExprType> = args.iter().map(IRExpr::ty).collect();
                            registry::row(*func, vals, &arg_types, e.ty())?
                        }
                        ScalarFunc::MatchTerms { mode } => {
                            let max_edits = self.max_edits(args.get(2), 0, row, env)?;
                            self.lexical(
                                &args[0],
                                &args[1],
                                matches!(mode, TermsMode::All),
                                max_edits,
                                row,
                                env,
                            )?
                        }
                    }
                }
                // `search` and `match_text` are `terms(q, mode: any)`; `fuzzy`
                // adds an edit budget, two when unwritten (section 4, Migration).
                IRExpr::Nearest { .. } => {
                    return unsupported("a ranking as a scalar");
                }
                IRExpr::Aggregate { .. } => return unsupported("an aggregate outside a group"),
            })
        })
    }
}

/// A distance as a score reports it: a non-finite one, which finite
/// components can reach by overflowing `F32`, is `E3` (section 2, `Score`:
/// a non-finite input is a typed error).
fn finite_distance(d: f64) -> R<f64> {
    if d.is_finite() {
        Ok(d)
    } else {
        Err(Refusal::Runtime("E3", "a distance is not finite".into()))
    }
}

/// A `DateTime` as the engine renders it: the instant the string names
/// (the compiler's one parse, `datetime_literal_millis`) in UTC, as
/// `YYYY-MM-DDTHH:MM:SS` with `.mmm` where the milliseconds are not zero, so
/// two spellings of one instant are one value and compare as one. `None`
/// for a string that names no instant, or a year outside `0..=9999`.
pub(crate) fn canonical_datetime(s: &str) -> Option<String> {
    let millis = datetime_literal_millis(s).ok()?;
    let (days, of_day) = (millis.div_euclid(86_400_000), millis.rem_euclid(86_400_000));
    // Days since 1970-01-01 to the civil date (H. Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    if !(0..=9999).contains(&year) {
        return None;
    }
    let (hour, minute, second, ms) = (
        of_day / 3_600_000,
        of_day / 60_000 % 60,
        of_day / 1000 % 60,
        of_day % 1000,
    );
    let mut out = format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}");
    if ms != 0 {
        out.push_str(&format!(".{ms:03}"));
    }
    Some(out)
}

/// A parameter's value at its declared type: a null for a non-nullable
/// declaration, or a number its declared numeric type does not hold (an
/// integer out of range, a float that is not finite or rounds to an
/// infinite `F32`), is refused; a list is judged item by item.
fn bind_param(name: &str, value: Value, ty: &PropType) -> R<Value> {
    fn holds(value: &Value, scalar: ScalarType) -> bool {
        match (value, scalar) {
            (Value::Int(i), ScalarType::I32) => i32::try_from(*i).is_ok(),
            (Value::Int(i), ScalarType::I64) => i64::try_from(*i).is_ok(),
            (Value::Int(i), ScalarType::U32) => u32::try_from(*i).is_ok(),
            (Value::Int(i), ScalarType::U64) => u64::try_from(*i).is_ok(),
            (Value::Float(f), ScalarType::F32 | ScalarType::Vector(_)) => (*f as f32).is_finite(),
            (Value::Float(f), ScalarType::F64) => f.is_finite(),
            (Value::List(items), scalar) => items.iter().all(|item| holds(item, scalar)),
            _ => true,
        }
    }
    if value == Value::Null && !ty.nullable {
        return Err(Refusal::Input(format!(
            "param '{name}': null for a non-nullable parameter"
        )));
    }
    let spelled = |value: &Value| match (value, ty.scalar) {
        (Value::Str(s), ScalarType::DateTime) => check_datetime_literal(s)
            .and_then(|()| datetime_literal_millis(s).map(drop))
            .is_ok(),
        (Value::Str(s), ScalarType::Date) => date_literal_days(s).is_ok(),
        _ => true,
    };
    let spelled_items = match &value {
        Value::List(items) => items.iter().all(spelled),
        other => spelled(other),
    };
    if !spelled_items {
        return Err(Refusal::Input(format!(
            "param '{name}': {value:?} is not a {}",
            ty.scalar
        )));
    }
    if !holds(&value, ty.scalar) {
        return Err(Refusal::Input(format!(
            "param '{name}': {value:?} is outside its declared type"
        )));
    }
    Ok(typed(value, Some(ty.scalar)))
}

/// An exported value at its declared type: an integer the export spells as
/// a string (beyond JSON's exact range) reads back as the integer.
fn typed(v: Value, scalar: Option<ScalarType>) -> Value {
    let integer = matches!(
        scalar,
        Some(ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64)
    );
    let single = matches!(scalar, Some(ScalarType::F32 | ScalarType::Vector(_)));
    match v {
        Value::Str(s) if integer => s.parse::<i128>().map_or(Value::Str(s), Value::Int),
        Value::Str(s) if scalar == Some(ScalarType::DateTime) => {
            Value::Str(canonical_datetime(&s).unwrap_or(s))
        }
        // An `F32` or vector element is stored at single precision.
        Value::Int(i) if single => Value::Float(f64::from(i as f32)),
        Value::Float(f) if single => Value::Float(f64::from(f as f32)),
        Value::List(items) => Value::List(items.into_iter().map(|i| typed(i, scalar)).collect()),
        other => other,
    }
}

/// A term's column names, in column order.
/// `Filter(input, pred)` with a conjunct that cannot fail evaluated where
/// the columns it reads first exist: into the outer or the inner side of a
/// `Correlate` that cannot fail, below an expansion it reads no new column
/// of, past another conjunct that cannot fail and below a `let` that cannot
/// fail and that it does not read. `None` when it stays where it is
/// written. A conjunct that cannot fail is pure and total, so this changes
/// no answer; it keeps a product of independent bindings from being built
/// whole before its key equalities filter it. Nothing moves below a term
/// that can fail.
fn sink(input: &Term, pred: &IRExpr) -> Option<Term> {
    omnigraph_compiler::stack::deep(|| {
        let mut read = Vec::new();
        pred.columns(&mut read);
        let within = |cols: &[String]| read.iter().all(|c| cols.contains(c));
        let filter = |input: Term| Term::Filter {
            input: input.boxed(),
            pred: pred.clone(),
            fails: false,
        };
        let sunk = |input: &Term| sink(input, pred).unwrap_or_else(|| filter(input.clone()));
        match input {
            Term::Correlate {
                outer,
                inner,
                fails: false,
            } => {
                let outer_cols = columns(outer);
                if within(&outer_cols) {
                    return Some(Term::Correlate {
                        outer: sunk(outer).boxed(),
                        inner: inner.clone(),
                        fails: false,
                    });
                }
                let added: Vec<String> = columns(inner)
                    .into_iter()
                    .filter(|c| !outer_cols.contains(c))
                    .collect();
                (!read.is_empty() && within(&added)).then(|| Term::Correlate {
                    outer: outer.clone(),
                    inner: sunk(inner).boxed(),
                    fails: false,
                })
            }
            // Past another conjunct that cannot fail only to sink further.
            Term::Filter {
                input: below,
                pred: other,
                fails: false,
            } => sink(below, pred).map(|below| Term::Filter {
                input: below.boxed(),
                pred: other.clone(),
                fails: false,
            }),
            Term::Expand {
                input: below,
                src,
                dst,
                src_type,
                dst_type,
                edges,
                min_hops,
                max_hops,
                edge_var,
            } if within(&columns(below)) => Some(Term::Expand {
                input: sunk(below).boxed(),
                src: src.clone(),
                dst: dst.clone(),
                src_type: src_type.clone(),
                dst_type: dst_type.clone(),
                edges: edges.clone(),
                min_hops: *min_hops,
                max_hops: *max_hops,
                edge_var: edge_var.clone(),
            }),
            Term::Project {
                input: below,
                items,
                keep: true,
                fails: false,
            } if !items.iter().any(|(name, _)| read.contains(name)) => Some(Term::Project {
                input: sunk(below).boxed(),
                items: items.clone(),
                keep: true,
                fails: false,
            }),
            _ => None,
        }
    })
}

fn columns(t: &Term) -> Vec<String> {
    omnigraph_compiler::stack::deep(|| match t {
        Term::Unit => Vec::new(),
        Term::Env(cols) => cols.iter().map(|c| c.name.clone()).collect(),
        Term::Scan { var, .. } => vec![var.clone()],
        Term::Expand {
            input,
            dst,
            edge_var,
            ..
        } => {
            let mut out = columns(input);
            out.push(dst.clone());
            out.extend(edge_var.clone());
            out
        }
        Term::Filter { input, .. } | Term::Order { input, .. } | Term::Cut { input, .. } => {
            columns(input)
        }
        Term::Project {
            input, items, keep, ..
        } => {
            let mut out = if *keep { columns(input) } else { Vec::new() };
            for (name, _) in items {
                if !out.contains(name) {
                    out.push(name.clone());
                }
            }
            out
        }
        Term::Group { keys, aggs, .. } => keys
            .iter()
            .map(|(n, _)| n.clone())
            .chain(aggs.iter().map(|(n, _)| n.clone()))
            .collect(),
        Term::Correlate { outer, inner, .. } => {
            let mut out = columns(outer);
            for name in columns(inner) {
                if !out.contains(&name) {
                    out.push(name);
                }
            }
            out
        }
        Term::Union { left, .. } => columns(left),
        Term::Score { input, out, .. } | Term::ApproxTopK { input, out, .. } => {
            let mut cols = columns(input);
            cols.push(out.clone());
            cols
        }
        Term::Fuse { target, out, .. } => vec![target.clone(), out.clone()],
        Term::Fix { body, .. } | Term::Call { body, .. } => columns(body),
        Term::RecRef { ty, .. } => ty.names(),
    })
}

/// The tie rule (section 2, `Order`): every reference column's identity in
/// column order, then every other column's value.
fn tie(a: &Row, b: &Row) -> Ordering {
    let refs = |r: &Row| -> Vec<Value> {
        r.iter()
            .filter(|(_, v)| matches!(v, Value::Ref { .. }))
            .map(|(_, v)| v.clone())
            .collect()
    };
    let rest = |r: &Row| -> Vec<Value> {
        r.iter()
            .filter(|(_, v)| !matches!(v, Value::Ref { .. }))
            .map(|(_, v)| v.clone())
            .collect()
    };
    cmp_values(&Value::List(refs(a)), &Value::List(refs(b)))
        .then_with(|| cmp_values(&Value::List(rest(a)), &Value::List(rest(b))))
}

fn same_row(a: &Row, b: &Row) -> bool {
    a.len() == b.len()
        && a.iter()
            .all(|(n, v)| get(b, n).is_some_and(|w| identical(v, w)))
}

/// Whether two values are one value, the identity grouping, `distinct` and
/// `count(distinct)` read: two floats by the total order, so `-0.0` and
/// `0.0` stay apart, where a comparison (`cmp_compared`) finds them equal.
fn identical(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Float(x), Value::Float(y)) => x.total_cmp(y) == Ordering::Equal,
        (Value::List(x), Value::List(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| identical(p, q))
        }
        _ => a == b,
    }
}

/// The order a comparison reads: the value order, except that two numbers
/// compare as IEEE 754 does, `-0.0` equal to `0.0`.
fn cmp_compared(a: &Value, b: &Value) -> Ordering {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) if !matches!((a, b), (Value::Int(_), Value::Int(_))) => {
            x.partial_cmp(&y).unwrap_or(Ordering::Equal)
        }
        _ => cmp_values(a, b),
    }
}

fn compare(op: CompOp, l: Value, r: Value) -> Value {
    match op {
        CompOp::Contains => match l {
            Value::List(items) if r != Value::Null => Value::Bool(
                items
                    .iter()
                    .any(|i| cmp_compared(i, &r) == Ordering::Equal && *i != Value::Null),
            ),
            Value::Str(s) => match r {
                Value::Str(n) => Value::Bool(s.contains(&n)),
                _ => Value::Null,
            },
            _ => Value::Null,
        },
        CompOp::StringContains => match (l, r) {
            (Value::Str(s), Value::Str(n)) => Value::Bool(s.contains(&n)),
            _ => Value::Null,
        },
        CompOp::StartsWith => match (l, r) {
            (Value::Str(s), Value::Str(n)) => Value::Bool(s.starts_with(&n)),
            _ => Value::Null,
        },
        _ if l == Value::Null || r == Value::Null => Value::Null,
        _ => {
            let o = cmp_compared(&l, &r);
            Value::Bool(match op {
                CompOp::Eq => o == Ordering::Equal,
                CompOp::Ne => o != Ordering::Equal,
                CompOp::Gt => o == Ordering::Greater,
                CompOp::Lt => o == Ordering::Less,
                CompOp::Ge => o != Ordering::Less,
                CompOp::Le => o != Ordering::Greater,
                _ => unreachable!(),
            })
        }
    }
}

fn arith(op: ArithOp, l: Value, r: Value, ty: &ExprType) -> R<Value> {
    let float = matches!(
        ty,
        ExprType::Value {
            scalar: ScalarType::F32 | ScalarType::F64,
            ..
        }
    );
    Ok(match (l, r) {
        (Value::Null, _) | (_, Value::Null) => Value::Null,
        (Value::Int(a), Value::Int(b)) if !float => {
            let out = match op {
                ArithOp::Add => a.checked_add(b),
                ArithOp::Sub => a.checked_sub(b),
                ArithOp::Mul => a.checked_mul(b),
                ArithOp::Div | ArithOp::Rem if b == 0 => {
                    return Err(Refusal::Runtime("E2", "a divisor is zero".into()));
                }
                ArithOp::Div => a.checked_div(b),
                ArithOp::Rem => a.checked_rem(b),
            };
            let (lo, hi) = match ty {
                ExprType::Value {
                    scalar: ScalarType::I32,
                    ..
                } => (i128::from(i32::MIN), i128::from(i32::MAX)),
                ExprType::Value {
                    scalar: ScalarType::U32,
                    ..
                } => (0, i128::from(u32::MAX)),
                ExprType::Value {
                    scalar: ScalarType::U64,
                    ..
                } => (0, i128::from(u64::MAX)),
                _ => (i128::from(i64::MIN), i128::from(i64::MAX)),
            };
            match out {
                Some(v) if v >= lo && v <= hi => Value::Int(v),
                _ => {
                    return Err(Refusal::Runtime(
                        "E1",
                        "integer arithmetic overflowed its type".into(),
                    ));
                }
            }
        }
        (a, b) => {
            let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) else {
                return Ok(Value::Null);
            };
            // `-0.0` is a zero divisor too.
            if matches!(op, ArithOp::Div | ArithOp::Rem) && b == 0.0 {
                return Err(Refusal::Runtime("E2", "a divisor is zero".into()));
            }
            let single = matches!(
                ty,
                ExprType::Value {
                    scalar: ScalarType::F32,
                    ..
                }
            );
            // An `F32` result is computed and rounded in `F32`, as the engine
            // computes it, so its overflow and its equalities are `F32`'s.
            let out = if single {
                let (a, b) = (a as f32, b as f32);
                f64::from(match op {
                    ArithOp::Add => a + b,
                    ArithOp::Sub => a - b,
                    ArithOp::Mul => a * b,
                    ArithOp::Div => a / b,
                    ArithOp::Rem => a % b,
                })
            } else {
                match op {
                    ArithOp::Add => a + b,
                    ArithOp::Sub => a - b,
                    ArithOp::Mul => a * b,
                    ArithOp::Div => a / b,
                    ArithOp::Rem => a % b,
                }
            };
            if !out.is_finite() {
                return Err(Refusal::Runtime(
                    "E3",
                    "a float result is not finite".into(),
                ));
            }
            Value::Float(out)
        }
    })
}

fn cast(v: Value, ty: &ExprType) -> R<Value> {
    let ExprType::Value { scalar, list, .. } = ty else {
        return Ok(v);
    };
    let one = |v: Value| match (v, scalar) {
        (Value::Int(i), ScalarType::F64) => Ok(Value::Float(i as f64)),
        (Value::Int(i), ScalarType::F32) => Ok(Value::Float(f64::from(i as f32))),
        (Value::Float(f), ScalarType::F32) => Ok(Value::Float(f64::from(f as f32))),
        (
            Value::Float(_),
            ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64,
        ) => unsupported("a cast of a float to an integer"),
        (other, _) => Ok(other),
    };
    match v {
        Value::List(items) if *list => Ok(Value::List(
            items.into_iter().map(one).collect::<R<Vec<_>>>()?,
        )),
        other => one(other),
    }
}

/// The analysis of a full-text property (section 4, Schema: Text): its
/// profile's pipeline, `english_v1` where the index names none. NFC, the
/// simple tokenizer, lowercasing, the token-length filter, the English
/// (Snowball) stemmer, the stop list of each word and its stem, and ASCII
/// folding are evaluated, in the profile's order. The stemmer is the
/// Snowball algorithm the profile names, and the stop list is the one the
/// profile declares (the compiler's).
#[derive(Debug, Clone, Copy)]
struct Analysis {
    fold: bool,
    /// Tokens of this many bytes or more are dropped, as Lance's token-length
    /// filter drops them.
    max_bytes: Option<usize>,
    /// The profile stems with the English Snowball stemmer.
    stem: bool,
    /// The stop list the profile removes, after stemming.
    stop_words: Option<&'static [&'static str]>,
}

impl Analysis {
    fn of(catalog: &Catalog, owner: Option<&str>, property: &str) -> R<Self> {
        use omnigraph_compiler::schema::analysis::{AnalyzerFilter, AnalyzerLanguage};
        let Some(profile) = owner
            .and_then(|t| catalog.node_types.get(t))
            .and_then(|t| t.full_text_profile(property))
        else {
            return unsupported(format!(
                "a lexical call over `{property}`, which has no full-text profile"
            ));
        };
        let pipeline = profile.pipeline();
        Ok(Self {
            fold: pipeline.filters.contains(&AnalyzerFilter::AsciiFolding),
            max_bytes: pipeline.max_token_length,
            stem: pipeline
                .filters
                .contains(&AnalyzerFilter::Stem(AnalyzerLanguage::English)),
            stop_words: pipeline.stop_words(),
        })
    }

    /// The analyzed tokens of `text`: NFC, then split on every character
    /// that is not alphanumeric, long tokens dropped, lowercased, stemmed,
    /// stop words removed, folded.
    fn tokens(&self, text: &str) -> R<Vec<String>> {
        use unicode_normalization::UnicodeNormalization;
        let text: String = text.nfc().collect();
        let stemmer = self
            .stem
            .then(|| frostem::Stemmer::new(frostem::Algorithm::English));
        let mut out = Vec::new();
        for word in text.split(|c: char| !c.is_alphanumeric()) {
            if word.is_empty() || self.max_bytes.is_some_and(|m| word.len() >= m) {
                continue;
            }
            let mut token = word.to_lowercase();
            if let Some(stemmer) = &stemmer {
                token = stemmer.stem(&token).into_owned();
            }
            if self
                .stop_words
                .is_some_and(|stop| stop.contains(&token.as_str()))
            {
                continue;
            }
            out.push(if self.fold { fold(&token)? } else { token });
        }
        Ok(out)
    }
}

/// ASCII folding of a lowercased token: the Latin letters with diacritics
/// and the Latin ligatures; any other non-ASCII letter is refused, since the
/// RFC fixes no folding table beyond them.
fn fold(token: &str) -> R<String> {
    const TABLE: &[(&str, &str)] = &[
        ("àáâãäåāăą", "a"),
        ("çćĉċč", "c"),
        ("ďđð", "d"),
        ("èéêëēĕėęě", "e"),
        ("ĝğġģ", "g"),
        ("ĥħ", "h"),
        ("ìíîïĩīĭįı", "i"),
        ("ĵ", "j"),
        ("ķĸ", "k"),
        ("ĺļľŀł", "l"),
        ("ñńņňŋ", "n"),
        ("òóôõöøōŏő", "o"),
        ("ŕŗř", "r"),
        ("śŝşšſ", "s"),
        ("ţťŧ", "t"),
        ("ùúûüũūŭůűų", "u"),
        ("ŵ", "w"),
        ("ýÿŷ", "y"),
        ("źżž", "z"),
        ("ß", "ss"),
        ("æ", "ae"),
        ("œ", "oe"),
        ("þ", "th"),
        ("ĳ", "ij"),
    ];
    let mut out = String::with_capacity(token.len());
    for c in token.chars() {
        if c.is_ascii() {
            out.push(c);
        } else if let Some((_, to)) = TABLE.iter().find(|(from, _)| from.contains(c)) {
            out.push_str(to);
        } else {
            return unsupported(format!("ASCII folding of `{c}`"));
        }
    }
    Ok(out)
}

fn levenshtein(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut cur = vec![i + 1];
        for (j, cb) in b.iter().enumerate() {
            cur.push(
                (prev[j] + usize::from(ca != *cb))
                    .min(prev[j + 1] + 1)
                    .min(cur[j] + 1),
            );
        }
        prev = cur;
    }
    prev[b.len()]
}

/// `bm25_v1` (section 4): `F64`, `log1p` IDF over the field corpus, each
/// query term's part the best stored term within the edit budget weighted
/// `2^-edits`, parts summed in ascending UTF-8 query-term order; membership
/// is the `terms` relation.
struct Bm25 {
    /// The distinct analyzed query terms, ascending.
    query: Vec<String>,
    mode_all: bool,
    max_edits: usize,
    analysis: Analysis,
    docs: f64,
    avg_len: f64,
    /// The documents holding an alternative of each query term.
    df: Vec<f64>,
}

impl Bm25 {
    fn new(
        mut query: Vec<String>,
        mode_all: bool,
        max_edits: usize,
        analysis: Analysis,
        corpus: Vec<Vec<String>>,
    ) -> Self {
        query.sort();
        query.dedup();
        let df = query
            .iter()
            .map(|q| {
                corpus
                    .iter()
                    .filter(|doc| doc.iter().any(|t| levenshtein(q, t) <= max_edits))
                    .count() as f64
            })
            .collect();
        let docs = corpus.len() as f64;
        let avg_len = corpus.iter().map(Vec::len).sum::<usize>() as f64 / docs.max(1.0);
        Self {
            query,
            mode_all,
            max_edits,
            analysis,
            docs,
            avg_len,
            df,
        }
    }

    /// The row's score, `None` when its field does not match.
    fn score(
        &self,
        it: &Interpreter<'_>,
        row: &Row,
        column: &str,
        property: &str,
    ) -> R<Option<f64>> {
        let Some(text) = it.field_text(row, column, property) else {
            return Ok(None);
        };
        let doc = self.analysis.tokens(&text)?;
        if doc.is_empty() {
            return Ok(None);
        }
        let norm = 1.2 * (0.25 + 0.75 * doc.len() as f64 / self.avg_len);
        let mut matched = 0;
        let mut total = 0f64;
        for (q, df) in self.query.iter().zip(&self.df) {
            let mut best: Option<f64> = None;
            for t in &doc {
                let edits = levenshtein(q, t);
                if edits > self.max_edits {
                    continue;
                }
                let tf = doc.iter().filter(|w| *w == t).count() as f64;
                let weight = 2f64.powi(-(edits as i32)) * 2.2 * tf / (tf + norm);
                best = Some(best.map_or(weight, |b: f64| b.max(weight)));
            }
            if let Some(best) = best {
                matched += 1;
                let idf = libm::log1p((self.docs - df + 0.5) / (df + 0.5));
                total += idf * best;
            }
        }
        let member = if self.mode_all {
            matched == self.query.len()
        } else {
            matched > 0
        };
        Ok(member.then_some(total))
    }
}

#[cfg(test)]
mod tests;

/// The current instant as an RFC 3339 UTC timestamp, to the millisecond.
fn utc_now() -> String {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);
    let (days, rem) = (ms.div_euclid(86_400_000), ms.rem_euclid(86_400_000));
    // Civil date from days since 1970-01-01 (Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000
    )
}
