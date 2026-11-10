//! A reference interpreter of the core IR (docs/rfcs/2026-11-07-gq-3.md,
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
//! Every operator the planner lowers is evaluated. `Union` and `Fix` (by
//! naive iteration to a fixpoint) are evaluated too; a `Call` whose
//! bindings read the row it is drawn beside is refused with
//! [`Refusal::Unsupported`].

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::IRExpr;
use omnigraph_compiler::ir::core::{
    Agg, Col, CoreRead, Count, LexicalQuery, RelTy, ScoreKind, ScoreQuery, Term, Ty,
};
use omnigraph_compiler::ir::expr::{ScalarFunc, TermsMode};
use omnigraph_compiler::query::syntax::ArithOp;
use omnigraph_compiler::query::value::{AggFunc, BinaryOp, CompOp, Literal, NOW_PARAM_NAME};
use omnigraph_compiler::traversal::EDGE_TYPE_COLUMN;
use omnigraph_compiler::types::{Direction, ExprType, ScalarType};
use serde_json::{Map, Value as Json};

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
            Literal::String(s) | Literal::Date(s) | Literal::DateTime(s) => Self::Str(s.clone()),
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

/// The total value order: null first, then by kind, numbers by value.
fn cmp_values(a: &Value, b: &Value) -> Ordering {
    use Value::*;
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
        Ok(graph)
    }

    fn node(&self, type_name: &str, id: &str) -> Option<&Entity> {
        self.nodes.get(type_name)?.iter().find(|e| e.id == id)
    }

    fn edge(&self, type_name: &str, id: &str) -> Option<&Entity> {
        self.edges.get(type_name)?.iter().find(|e| e.id == id)
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
    /// The current value of each recursive read inside its `Fix`.
    rec: HashMap<String, Vec<Row>>,
}

impl<'a> Interpreter<'a> {
    pub fn new(catalog: &'a Catalog, graph: &'a Graph, params: &HashMap<String, Literal>) -> Self {
        Self {
            catalog,
            graph,
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
        // A parameter binds at its declared type.
        for (name, ty) in &read.params {
            if let Some(v) = self.params.remove(name) {
                self.params.insert(name.clone(), typed(v, Some(ty.scalar)));
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

    fn count(&self, k: &Count) -> R<usize> {
        match k {
            Count::Literal(n) => Ok(*n as usize),
            Count::Param(p) => match self.params.get(p) {
                Some(Value::Int(n)) if *n >= 0 => Ok(*n as usize),
                other => Err(Refusal::Input(format!("`${p}` is no count: {other:?}"))),
            },
        }
    }

    // -----------------------------------------------------------------
    // Operators (section 2's table).

    fn term(&mut self, t: &Term, env: &Row) -> R<Rel> {
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
                    // cycle never returns it; a self-loop edge is the one
                    // hop that does, and the walk does not continue from it.
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
                    if self_loop && *min_hops <= 1 {
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
            Term::Filter { input, pred } => {
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
            Term::Project { input, items, keep } => {
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
                    match groups.iter_mut().find(|(k, _)| k == &key) {
                        Some((_, members)) => members.push(row),
                        None => groups.push((key, vec![row])),
                    }
                }
                if keys.is_empty() && groups.is_empty() {
                    groups.push((Vec::new(), Vec::new()));
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
            Term::Correlate { outer, inner } => {
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
                let mut l = self.term(left, env)?;
                let r = self.term(right, env)?;
                l.rows.extend(r.rows);
                l.ordered = false;
                l
            }
            Term::Order { input, keys } => {
                let input = self.term(input, env)?;
                let mut keyed = Vec::new();
                for row in input.rows {
                    let k = keys
                        .iter()
                        .map(|(e, desc)| Ok((self.expr(e, &row, env)?, *desc)))
                        .collect::<R<Vec<_>>>()?;
                    keyed.push((k, row));
                }
                keyed.sort_by(|(ka, ra), (kb, rb)| {
                    for ((a, desc), (b, _)) in ka.iter().zip(kb) {
                        let o = cmp_values(a, b);
                        let o = if *desc { o.reverse() } else { o };
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
                            let Some(v) = self.field_vector(&row, column, property) else {
                                continue;
                            };
                            row.push((out.clone(), Value::Float(l2(&v, &q))));
                            rows.push(row);
                        }
                    }
                    (ScoreKind::Bm25, ScoreQuery::Lexical(lex)) => {
                        let scorer = self.bm25(&input.rows, column, property, lex, env)?;
                        for mut row in input.rows {
                            if let Some(s) = scorer.score(self, &row, column, property) {
                                // A score is an `F32`.
                                row.push((out.clone(), Value::Float(f64::from(s as f32))));
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
                let k = self.count(k)?;
                let mut best: Vec<(f64, Value)> = Vec::new();
                for row in &input.rows {
                    let Some(target) = get(row, column).cloned() else {
                        continue;
                    };
                    if best.iter().any(|(_, t)| t == &target) {
                        continue;
                    }
                    if let Some(v) = self.field_vector(row, column, property) {
                        best.push((l2(&v, &q), target));
                    }
                }
                best.sort_by(|a, b| {
                    a.0.partial_cmp(&b.0)
                        .unwrap_or(Ordering::Equal)
                        .then(cmp_values(&a.1, &b.1))
                });
                best.truncate(k);
                let mut rows = Vec::new();
                for mut row in input.rows {
                    let target = get(&row, column).cloned();
                    if let Some((d, _)) = best.iter().find(|(_, t)| Some(t) == target.as_ref()) {
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
            } => {
                let k = self.count(k)? as f64;
                let mut fused: Vec<(Value, f64)> = Vec::new();
                for (i, arm) in arms.iter().enumerate() {
                    let (rel, score, descending) = self.scored_arm(arm, env)?;
                    let mut best: Vec<(Value, f64)> = Vec::new();
                    for row in &rel.rows {
                        let (Some(t), Some(s)) =
                            (get(row, target), get(row, &score).and_then(Value::as_f64))
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
                    let w = weights.get(i).copied().unwrap_or(1.0);
                    for (rank, (t, _)) in best.into_iter().enumerate() {
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
                        .map(|(t, s)| vec![(target.clone(), t), ("score".into(), Value::Float(s))])
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
                let mut saved = Vec::new();
                for (name, e) in bindings {
                    let mut cols = Vec::new();
                    e.columns(&mut cols);
                    if !cols.is_empty() {
                        return unsupported("a call whose arguments read the row beside it");
                    }
                    let v = self.expr(e, &Vec::new(), env)?;
                    saved.push((name.clone(), self.params.insert(name.clone(), v)));
                }
                let out = self.term(body, env);
                for (name, old) in saved {
                    match old {
                        Some(v) => self.params.insert(name, v),
                        None => self.params.remove(&name),
                    };
                }
                out?
            }
        })
    }

    /// An `rrf` arm's relation, its score column and its direction.
    fn scored_arm(&mut self, arm: &Term, env: &Row) -> R<(Rel, String, bool)> {
        let (out, descending) = match arm {
            Term::ApproxTopK { out, .. } => (out.clone(), false),
            Term::Score {
                out,
                kind: ScoreKind::Distance,
                ..
            } => (out.clone(), false),
            Term::Score { out, .. } => (out.clone(), true),
            _ => return unsupported("an `rrf` arm that is not a ranking"),
        };
        Ok((self.term(arm, env)?, out, descending))
    }

    fn aggregate(&self, agg: &Agg, rows: &[Row], env: &Row) -> R<Value> {
        let Some(arg) = &agg.arg else {
            return Ok(Value::Int(rows.len() as i128));
        };
        let mut values = Vec::new();
        for row in rows {
            let v = self.expr(arg, row, env)?;
            if v != Value::Null && !(agg.distinct && values.contains(&v)) {
                values.push(v);
            }
        }
        Ok(match agg.func {
            AggFunc::Count => Value::Int(values.len() as i128),
            _ if values.is_empty() => Value::Null,
            // An integer sum is exact, rounded to `F64` once.
            AggFunc::Sum => Value::Float(exact_sum(&values)),
            // A mean sums in `F64`, as the plan's `float64` leaf names.
            AggFunc::Avg => Value::Float(
                values.iter().filter_map(Value::as_f64).sum::<f64>() / values.len() as f64,
            ),
            AggFunc::Min => values.into_iter().min_by(cmp_values).unwrap(),
            AggFunc::Max => values.into_iter().max_by(cmp_values).unwrap(),
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
        // The field corpus: every row of the type, not only the population.
        let type_name = rows.iter().find_map(|r| match get(r, column) {
            Some(Value::Ref { type_name, .. }) => Some(type_name.clone()),
            _ => None,
        });
        let corpus: Vec<Vec<String>> = type_name
            .and_then(|t| self.graph.nodes.get(&t))
            .into_iter()
            .flatten()
            .filter_map(|e| e.props.get(property)?.as_str().map(tokens))
            .collect();
        Ok(Bm25::new(tokens(&text), lex.mode_all, &corpus))
    }

    // -----------------------------------------------------------------
    // Expressions.

    fn expr(&self, e: &IRExpr, row: &Row, env: &Row) -> R<Value> {
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
            IRExpr::Cast { expr, ty } => cast(self.expr(expr, row, env)?, ty),
            IRExpr::Neg { expr, .. } => match self.expr(expr, row, env)? {
                Value::Int(i) => Value::Int(-i),
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
                match op {
                    BinaryOp::And => {
                        let r = self.expr(right, row, env)?;
                        match (l, r) {
                            (Value::Bool(false), _) | (_, Value::Bool(false)) => Value::Bool(false),
                            (Value::Bool(true), Value::Bool(true)) => Value::Bool(true),
                            _ => Value::Null,
                        }
                    }
                    BinaryOp::Or => {
                        let r = self.expr(right, row, env)?;
                        match (l, r) {
                            (Value::Bool(true), _) | (_, Value::Bool(true)) => Value::Bool(true),
                            (Value::Bool(false), Value::Bool(false)) => Value::Bool(false),
                            _ => Value::Null,
                        }
                    }
                    BinaryOp::Compare(op) => compare(*op, l, self.expr(right, row, env)?),
                    BinaryOp::Arith(op) => arith(*op, l, self.expr(right, row, env)?, ty)?,
                }
            }
            IRExpr::Call { func, args, .. } => {
                let vals = args
                    .iter()
                    .map(|a| self.expr(a, row, env))
                    .collect::<R<Vec<_>>>()?;
                match func {
                    ScalarFunc::If => match vals.as_slice() {
                        [Value::Bool(true), a, _] => a.clone(),
                        [_, _, b] => b.clone(),
                        _ => Value::Null,
                    },
                    ScalarFunc::Coalesce => vals
                        .into_iter()
                        .find(|v| *v != Value::Null)
                        .unwrap_or(Value::Null),
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
                    ScalarFunc::MatchTerms { mode } => match vals.as_slice() {
                        [Value::Str(field), Value::Str(q)] => {
                            let f = tokens(field);
                            let q = tokens(q);
                            Value::Bool(match mode {
                                TermsMode::All => q.iter().all(|t| f.contains(t)),
                                TermsMode::Any => q.iter().any(|t| f.contains(t)),
                            })
                        }
                        _ => Value::Null,
                    },
                }
            }
            IRExpr::Search { field, query, .. } | IRExpr::MatchText { field, query, .. } => {
                match (self.expr(field, row, env)?, self.expr(query, row, env)?) {
                    (Value::Str(f), Value::Str(q)) => {
                        let f = tokens(&f);
                        Value::Bool(tokens(&q).iter().all(|t| f.contains(t)))
                    }
                    _ => Value::Null,
                }
            }
            IRExpr::Fuzzy {
                field,
                query,
                max_edits,
                ..
            } => {
                let edits = match max_edits {
                    Some(m) => match self.expr(m, row, env)? {
                        Value::Int(n) => n as usize,
                        _ => 2,
                    },
                    None => 2,
                };
                match (self.expr(field, row, env)?, self.expr(query, row, env)?) {
                    (Value::Str(f), Value::Str(q)) => {
                        let f = tokens(&f);
                        Value::Bool(
                            tokens(&q)
                                .iter()
                                .all(|t| f.iter().any(|w| levenshtein(w, t) <= edits)),
                        )
                    }
                    _ => Value::Null,
                }
            }
            IRExpr::Nearest { .. } | IRExpr::Bm25 { .. } | IRExpr::Rrf { .. } => {
                return unsupported("a ranking as a scalar");
            }
            IRExpr::Aggregate { .. } => return unsupported("an aggregate outside a group"),
        })
    }
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
        // An `F32` or vector element is stored at single precision.
        Value::Int(i) if single => Value::Float(f64::from(i as f32)),
        Value::Float(f) if single => Value::Float(f64::from(f as f32)),
        Value::List(items) => Value::List(items.into_iter().map(|i| typed(i, scalar)).collect()),
        other => other,
    }
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
    a.len() == b.len() && a.iter().all(|(n, v)| get(b, n) == Some(v))
}

/// Squared Euclidean distance, rounded to `F32` as the engine reports it.
fn l2(a: &[f64], b: &[f64]) -> f64 {
    let d: f32 = a
        .iter()
        .zip(b)
        .map(|(x, y)| {
            let d = *x as f32 - *y as f32;
            d * d
        })
        .sum();
    f64::from(d)
}

fn compare(op: CompOp, l: Value, r: Value) -> Value {
    match op {
        CompOp::Contains => match l {
            Value::List(items) if r != Value::Null => Value::Bool(
                items
                    .iter()
                    .any(|i| cmp_values(i, &r) == Ordering::Equal && *i != Value::Null),
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
            let o = cmp_values(&l, &r);
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
            let out = match op {
                ArithOp::Add => a + b,
                ArithOp::Sub => a - b,
                ArithOp::Mul => a * b,
                ArithOp::Div => a / b,
                ArithOp::Rem => a % b,
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

fn cast(v: Value, ty: &ExprType) -> Value {
    let ExprType::Value { scalar, list, .. } = ty else {
        return v;
    };
    let one = |v: Value| match (v, scalar) {
        (Value::Int(i), ScalarType::F64) => Value::Float(i as f64),
        (Value::Int(i), ScalarType::F32) => Value::Float(f64::from(i as f32)),
        (Value::Float(f), ScalarType::F32) => Value::Float(f64::from(f as f32)),
        // A comparison's exact integer domain: a whole float literal.
        (
            Value::Float(f),
            ScalarType::I32 | ScalarType::I64 | ScalarType::U32 | ScalarType::U64,
        ) if f.fract() == 0.0 && f.abs() < 1e38 => Value::Int(f as i128),
        (other, _) => other,
    };
    match v {
        Value::List(items) if *list => Value::List(items.into_iter().map(one).collect()),
        other => one(other),
    }
}

/// The analyzer the reference uses: lowercase alphanumeric words.
fn tokens(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect()
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

/// BM25 over the field corpus: `k1 = 1.2`, `b = 0.75`, the idf
/// `ln(1 + (N - n + 0.5) / (n + 0.5))`, in `F32`.
struct Bm25 {
    query: Vec<String>,
    mode_all: bool,
    docs: f64,
    avgdl: f64,
    df: HashMap<String, f64>,
}

impl Bm25 {
    fn new(query: Vec<String>, mode_all: bool, corpus: &[Vec<String>]) -> Self {
        let mut df = HashMap::new();
        for doc in corpus {
            let mut seen: Vec<&String> = Vec::new();
            for t in doc {
                if query.contains(t) && !seen.contains(&t) {
                    seen.push(t);
                    *df.entry(t.clone()).or_insert(0.0) += 1.0;
                }
            }
        }
        let docs = corpus.len() as f64;
        let avgdl = corpus.iter().map(Vec::len).sum::<usize>() as f64 / docs.max(1.0);
        Self {
            query,
            mode_all,
            docs,
            avgdl,
            df,
        }
    }

    fn score(&self, it: &Interpreter<'_>, row: &Row, column: &str, property: &str) -> Option<f64> {
        let doc = tokens(&it.field_text(row, column, property)?);
        let mut matched = 0;
        let mut total = 0f32;
        let mut distinct: Vec<&String> = Vec::new();
        for t in &self.query {
            if distinct.contains(&t) {
                continue;
            }
            distinct.push(t);
            let tf = doc.iter().filter(|w| *w == t).count() as f64;
            if tf == 0.0 {
                continue;
            }
            matched += 1;
            let n = self.df.get(t).copied().unwrap_or(0.0);
            let idf = (1.0 + (self.docs - n + 0.5) / (n + 0.5)).ln();
            let dl = doc.len() as f64;
            let s = idf * tf * 2.2 / (tf + 1.2 * (0.25 + 0.75 * dl / self.avgdl));
            total += s as f32;
        }
        let ok = if self.mode_all {
            matched == distinct.len()
        } else {
            matched > 0
        };
        ok.then_some(f64::from(total))
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
