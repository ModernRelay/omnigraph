//! Generators of schemas, graphs and well-typed GQ 3 reads, shared by every
//! generated law (`docs/dev/test-ownership.md`, "Generated laws").
//!
//! Every generator reads one [`Choices`] stream: a list of numbers that
//! proptest draws and shrinks. A choice past the end of the list is `0`, and
//! `0` always picks the simplest alternative (no row, no element, a leaf, a
//! literal `0`), so shrinking the list toward shorter and smaller entries
//! shrinks the schema, the graph and the query toward their minimal forms
//! while every intermediate draw stays well-typed: generation is
//! type-directed, never generate-then-filter.
//!
//! Gated on `cfg(test)` or the `test-util` feature: only test builds of
//! this crate and of its dependents' dev-dependencies compile it.

use std::fmt::Write as _;

use proptest::prelude::*;

use crate::query::value::Literal;

/// The strategy of one choice stream of at most `max_len` entries.
pub fn choices(max_len: usize) -> impl Strategy<Value = Vec<u32>> {
    proptest::collection::vec(any::<u32>(), 0..max_len)
}

/// A cursor over a choice stream.
#[derive(Debug, Clone)]
pub struct Choices<'a> {
    data: &'a [u32],
    pos: usize,
}

impl<'a> Choices<'a> {
    pub fn new(data: &'a [u32]) -> Self {
        Self { data, pos: 0 }
    }

    /// A choice in `0..n`; `0` past the end of the stream.
    pub fn below(&mut self, n: usize) -> usize {
        if n <= 1 {
            return 0;
        }
        let v = self.data.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        v as usize % n
    }

    /// True with `percent` in a hundred; false past the end of the stream.
    pub fn flag(&mut self, percent: usize) -> bool {
        percent > 0 && self.below(100) >= 100 - percent.min(100)
    }

    /// One of `items`, the first past the end of the stream.
    pub fn pick<'b, T>(&mut self, items: &'b [T]) -> &'b T {
        &items[self.below(items.len())]
    }
}

// ---------------------------------------------------------------- schema

/// A scalar type an expression can have.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Sc {
    I32,
    I64,
    U32,
    F32,
    F64,
    Bool,
    Str,
    Date,
    DateTime,
}

impl Sc {
    pub const ALL: [Sc; 9] = [
        Sc::I32,
        Sc::I64,
        Sc::U32,
        Sc::F32,
        Sc::F64,
        Sc::Bool,
        Sc::Str,
        Sc::Date,
        Sc::DateTime,
    ];

    pub fn spelling(self) -> &'static str {
        match self {
            Sc::I32 => "I32",
            Sc::I64 => "I64",
            Sc::U32 => "U32",
            Sc::F32 => "F32",
            Sc::F64 => "F64",
            Sc::Bool => "Bool",
            Sc::Str => "String",
            Sc::Date => "Date",
            Sc::DateTime => "DateTime",
        }
    }

    pub fn class(self) -> Class {
        match self {
            Sc::I32 | Sc::I64 | Sc::U32 => Class::Int,
            Sc::F32 | Sc::F64 => Class::Float,
            Sc::Bool => Class::Bool,
            Sc::Str => Class::Str,
            Sc::Date => Class::Date,
            Sc::DateTime => Class::DateTime,
        }
    }
}

/// The class an expression generator is asked for; arithmetic mixes the
/// two numeric classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    Int,
    Float,
    Bool,
    Str,
    Date,
    DateTime,
}

/// A property's declared type.
#[derive(Debug, Clone, PartialEq)]
pub enum PropTy {
    Scalar(Sc),
    /// An enum over a fixed set of members.
    Enum,
    /// `[String]`.
    ListStr,
    /// `Vector(3, distance = …)`.
    Vector(&'static str),
    /// A `String @index(fulltext)` field.
    FullText,
}

pub const ENUM_MEMBERS: [&str; 3] = ["red", "green", "blue"];

#[derive(Debug, Clone)]
pub struct Prop {
    pub name: String,
    pub ty: PropTy,
    pub nullable: bool,
    /// Declares a plain `@index`.
    pub indexed: bool,
}

impl Prop {
    pub fn scalar(&self) -> Option<Sc> {
        match self.ty {
            PropTy::Scalar(s) => Some(s),
            PropTy::FullText => Some(Sc::Str),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeTy {
    pub name: String,
    pub props: Vec<Prop>,
}

#[derive(Debug, Clone)]
pub struct EdgeTy {
    pub name: String,
    pub from: usize,
    pub to: usize,
    pub props: Vec<Prop>,
}

impl EdgeTy {
    /// The edge's name as a traversal writes it.
    pub fn surface(&self) -> String {
        let mut c = self.name.chars();
        match c.next() {
            Some(first) => first.to_lowercase().chain(c).collect(),
            None => String::new(),
        }
    }
}

/// A generated schema: node types `A`, `B`, `C` keyed by `k`, and edges.
#[derive(Debug, Clone)]
pub struct GenSchema {
    pub nodes: Vec<NodeTy>,
    pub edges: Vec<EdgeTy>,
}

/// What a schema may declare.
#[derive(Debug, Clone, Copy)]
pub struct SchemaOptions {
    /// Declare a vector and a full-text field on the first type.
    pub search: bool,
    /// Declare `@index` on some scalar properties.
    pub indexes: bool,
}

impl Default for SchemaOptions {
    fn default() -> Self {
        Self {
            search: true,
            indexes: true,
        }
    }
}

/// The scalar property types a schema declares. `U32` is left to the
/// scalar laws: its literal and negation rules refuse half of the integer
/// expressions a read would draw over it.
const SCALAR_PROP_TYPES: [Sc; 8] = [
    Sc::I32,
    Sc::I64,
    Sc::F32,
    Sc::F64,
    Sc::Bool,
    Sc::Str,
    Sc::Date,
    Sc::DateTime,
];
const DISTANCES: [&str; 3] = ["l2", "cosine", "dot"];

impl GenSchema {
    pub fn generate(c: &mut Choices, options: SchemaOptions) -> Self {
        let n_nodes = 1 + c.below(3);
        let mut nodes = Vec::new();
        for t in 0..n_nodes {
            let mut props = Vec::new();
            // At least one numeric property, so arithmetic has a column.
            let n_props = 1 + c.below(4);
            for p in 0..n_props {
                let ty = if p == 0 {
                    PropTy::Scalar(*c.pick(&[Sc::I64, Sc::I32, Sc::F64, Sc::F32]))
                } else {
                    match c.below(12) {
                        9 => PropTy::Enum,
                        10 => PropTy::ListStr,
                        _ => PropTy::Scalar(*c.pick(&SCALAR_PROP_TYPES)),
                    }
                };
                let indexable = !matches!(ty, PropTy::Scalar(Sc::F32 | Sc::F64));
                props.push(Prop {
                    name: format!("p{p}"),
                    nullable: !c.flag(30),
                    indexed: options.indexes && indexable && c.flag(40),
                    ty,
                });
            }
            if t == 0 && options.search && c.flag(50) {
                props.push(Prop {
                    name: "v".into(),
                    ty: PropTy::Vector(DISTANCES[c.below(3)]),
                    nullable: true,
                    indexed: false,
                });
                props.push(Prop {
                    name: "t".into(),
                    ty: PropTy::FullText,
                    nullable: true,
                    indexed: false,
                });
            }
            nodes.push(NodeTy {
                name: ["A", "B", "C"][t].to_string(),
                props,
            });
        }
        let n_edges = 1 + c.below(3);
        let mut edges = Vec::new();
        for e in 0..n_edges {
            let from = c.below(n_nodes);
            let to = c.below(n_nodes);
            let mut props = Vec::new();
            if c.flag(40) {
                props.push(Prop {
                    name: "w".into(),
                    ty: PropTy::Scalar(*c.pick(&[Sc::I64, Sc::I32, Sc::F64])),
                    nullable: true,
                    indexed: false,
                });
            }
            edges.push(EdgeTy {
                name: format!("Rel{e}"),
                from,
                to,
                props,
            });
        }
        Self { nodes, edges }
    }

    /// The schema as `.pg` text.
    pub fn text(&self) -> String {
        let mut s = String::new();
        let enum_ty = ENUM_MEMBERS.join(", ");
        for node in &self.nodes {
            let _ = writeln!(s, "node {} {{\n    k: String @key", node.name);
            for p in &node.props {
                let _ = writeln!(s, "    {}: {}", p.name, prop_decl(p, &enum_ty));
            }
            s.push_str("}\n");
        }
        for e in &self.edges {
            let _ = write!(
                s,
                "edge {}: {} -> {}",
                e.name, self.nodes[e.from].name, self.nodes[e.to].name
            );
            if e.props.is_empty() {
                s.push('\n');
            } else {
                s.push_str(" {\n");
                for p in &e.props {
                    let _ = writeln!(s, "    {}: {}", p.name, prop_decl(p, &enum_ty));
                }
                s.push_str("}\n");
            }
        }
        s
    }
}

fn prop_decl(p: &Prop, enum_ty: &str) -> String {
    let q = if p.nullable { "?" } else { "" };
    let base = match &p.ty {
        PropTy::Scalar(s) => format!("{}{q}", s.spelling()),
        PropTy::Enum => format!("enum({enum_ty}){q}"),
        PropTy::ListStr => format!("[String]{q}"),
        PropTy::Vector(d) => format!("Vector(3, distance = \"{d}\"){q}"),
        PropTy::FullText => {
            return format!("String{q} @index(fulltext, analyzer = \"english_v1\")");
        }
    };
    if p.indexed {
        format!("{base} @index")
    } else {
        base
    }
}

// ---------------------------------------------------------------- values

/// Integer values; the extremes make overflow reachable.
const INTS: [i64; 10] = [0, 1, -1, 2, 3, -7, 10, 100, i32::MAX as i64, i64::MAX];
const FLOATS: [&str; 8] = [
    "0.0", "-0.0", "0.5", "1.5", "-2.25", "3.0", "100.0", "1.5e3",
];
const STRS: [&str; 6] = ["a", "b", "ab", "graph search", "Search", ""];
/// Strings the registry rows meet (the GQ 3 RFC, section 2): Unicode
/// whitespace around a value and alone (U+00A0, U+2003, a tab), case
/// mappings that change a value's length or need context (`ß`, `İ`, `Σ`),
/// and texts `cast` reads or refuses as a number or a Boolean.
const TEXTS: [&str; 19] = [
    " a ",
    "\u{a0}ab\u{2003}",
    "\tb",
    "  ",
    "\u{2003}",
    "ß",
    "İ",
    "ΑΣ",
    "Σ",
    "12",
    "+3",
    "1e3",
    " 1",
    "1.5",
    "inf",
    "NaN",
    "true",
    "TRUE",
    "yes",
];
/// The texts a `cast` to a number reads or refuses.
const NUMBER_TEXTS: [&str; 11] = [
    "12",
    "+3",
    "1e3",
    " 1",
    "1.5",
    "inf",
    "NaN",
    "-0.0",
    "-7",
    "2147483648",
    "",
];
/// The texts a `cast` to `Bool` reads or refuses.
const BOOL_TEXTS: [&str; 5] = ["true", "TRUE", "False", "yes", "1"];

/// A string value: mostly a plain one, sometimes one of [`TEXTS`].
fn str_value(c: &mut Choices) -> &'static str {
    if c.flag(30) {
        TEXTS[c.below(TEXTS.len())]
    } else {
        STRS[c.below(STRS.len())]
    }
}
const DATES: [&str; 3] = ["2024-01-01", "2023-12-31", "2024-02-29"];
const DATETIMES: [&str; 3] = [
    "2024-01-01T00:00:00Z",
    "2023-12-31T23:59:59Z",
    "2024-02-29T12:00:00Z",
];
/// Words of the full-text values; the last is a stop word, which a query
/// does not use alone.
const WORDS: [&str; 6] = ["graph", "search", "engine", "graphs", "data", "the"];

/// A JSON value of `sc` for a seed row.
fn json_value(c: &mut Choices, sc: Sc) -> String {
    match sc {
        Sc::I32 => {
            let v = INTS[c.below(INTS.len())];
            i64::from(v.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32).to_string()
        }
        Sc::I64 => INTS[c.below(INTS.len())].to_string(),
        Sc::U32 => INTS[c.below(INTS.len())]
            .clamp(0, i64::from(u32::MAX))
            .to_string(),
        Sc::F32 | Sc::F64 => FLOATS[c.below(FLOATS.len())].to_string(),
        Sc::Bool => (c.below(2) == 1).to_string(),
        Sc::Str => json_string(str_value(c)),
        Sc::Date => json_string(DATES[c.below(DATES.len())]),
        Sc::DateTime => json_string(DATETIMES[c.below(DATETIMES.len())]),
    }
}

fn json_string(s: &str) -> String {
    serde_json::to_string(s).expect("a string serializes")
}

/// The GQ literal of an integer: a negative one is a unary minus.
fn int_literal(v: i64) -> String {
    if v < 0 {
        format!("(-{})", (v as i128).unsigned_abs())
    } else {
        v.to_string()
    }
}

// ---------------------------------------------------------------- graph

#[derive(Debug, Clone)]
pub struct GenRow {
    pub key: String,
    /// `(property, JSON value)`; an absent property is null.
    pub values: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct GenEdge {
    pub from: String,
    pub to: String,
    pub values: Vec<(String, String)>,
}

/// A generated graph over a [`GenSchema`].
#[derive(Debug, Clone)]
pub struct GenGraph {
    pub nodes: Vec<Vec<GenRow>>,
    pub edges: Vec<Vec<GenEdge>>,
}

const KEYS: [&str; 6] = ["a", "b", "c", "d", "e", "f"];

fn prop_json(c: &mut Choices, p: &Prop) -> Option<String> {
    if p.nullable && c.flag(25) {
        return None;
    }
    Some(match &p.ty {
        PropTy::Scalar(sc) => json_value(c, *sc),
        PropTy::Enum => json_string(ENUM_MEMBERS[c.below(3)]),
        PropTy::ListStr => {
            let n = c.below(3);
            let items: Vec<String> = (0..n).map(|_| json_string(c.pick(&STRS))).collect();
            format!("[{}]", items.join(","))
        }
        PropTy::Vector(_) => {
            let f = |c: &mut Choices| *c.pick(&["0.0", "1.0", "-1.0", "0.5", "2.0"]);
            let mut v = [f(c), f(c), f(c)];
            // A stored zero vector is refused under `cosine`.
            if v.iter().all(|x| *x == "0.0") {
                v[0] = "1.0";
            }
            format!("[{}]", v.join(","))
        }
        PropTy::FullText => {
            let n = c.below(4);
            let words: Vec<&str> = (0..n).map(|_| *c.pick(&WORDS)).collect();
            json_string(&words.join(" "))
        }
    })
}

impl GenGraph {
    /// At most `max_rows` rows per node type and `max_edges` per edge type.
    pub fn generate(
        c: &mut Choices,
        schema: &GenSchema,
        max_rows: usize,
        max_edges: usize,
    ) -> Self {
        let mut nodes = Vec::new();
        for node in &schema.nodes {
            // Mostly populated; past the stream's end, empty.
            let n = match c.flag(90) {
                true => 1 + c.below(max_rows.min(KEYS.len())),
                false => 0,
            };
            let mut keys: Vec<&str> = KEYS.to_vec();
            let mut rows = Vec::new();
            for _ in 0..n {
                let key = keys.remove(c.below(keys.len()));
                let mut values = Vec::new();
                for p in &node.props {
                    if let Some(v) = prop_json(c, p) {
                        values.push((p.name.clone(), v));
                    }
                }
                rows.push(GenRow {
                    key: key.to_string(),
                    values,
                });
            }
            nodes.push(rows);
        }
        let mut edges = Vec::new();
        for e in &schema.edges {
            let (from, to) = (&nodes[e.from], &nodes[e.to]);
            let mut list = Vec::new();
            if !from.is_empty() && !to.is_empty() {
                let n = match c.flag(85) {
                    true => 1 + c.below(max_edges),
                    false => 0,
                };
                for _ in 0..n {
                    let a = from[c.below(from.len())].key.clone();
                    let b = to[c.below(to.len())].key.clone();
                    let mut values = Vec::new();
                    for p in &e.props {
                        if let Some(v) = prop_json(c, p) {
                            values.push((p.name.clone(), v));
                        }
                    }
                    list.push(GenEdge {
                        from: a,
                        to: b,
                        values,
                    });
                }
            }
            edges.push(list);
        }
        Self { nodes, edges }
    }

    /// The graph as load JSONL.
    pub fn jsonl(&self, schema: &GenSchema) -> String {
        let mut s = String::new();
        for (t, rows) in self.nodes.iter().enumerate() {
            for row in rows {
                let mut data = format!("\"k\":{}", json_string(&row.key));
                for (p, v) in &row.values {
                    let _ = write!(data, ",\"{p}\":{v}");
                }
                let _ = writeln!(
                    s,
                    "{{\"type\":\"{}\",\"data\":{{{data}}}}}",
                    schema.nodes[t].name
                );
            }
        }
        for (e, list) in self.edges.iter().enumerate() {
            for edge in list {
                let data: Vec<String> = edge
                    .values
                    .iter()
                    .map(|(p, v)| format!("\"{p}\":{v}"))
                    .collect();
                let _ = writeln!(
                    s,
                    "{{\"edge\":\"{}\",\"from\":{},\"to\":{},\"data\":{{{}}}}}",
                    schema.edges[e].name,
                    json_string(&edge.from),
                    json_string(&edge.to),
                    data.join(",")
                );
            }
        }
        s
    }

    pub fn row_count(&self) -> usize {
        self.nodes.iter().map(Vec::len).sum::<usize>()
            + self.edges.iter().map(Vec::len).sum::<usize>()
    }
}

// ---------------------------------------------------------------- reads

/// A variable in scope.
#[derive(Debug, Clone)]
enum VarKind {
    Node(usize),
    Edge(usize),
    Scalar(Sc),
}

#[derive(Debug, Clone)]
struct Var {
    name: String,
    kind: VarKind,
}

/// What the read generator may produce.
#[derive(Debug, Clone, Copy)]
pub struct ReadOptions {
    pub max_depth: u32,
    pub max_elements: usize,
    /// Rankings and fusion over the first type's search fields.
    pub search: bool,
    /// Blocks, `optional`, `union` and inline reads.
    pub nesting: bool,
    pub aggregates: bool,
    pub order_limit: bool,
    /// The placement law's reads: a `where` of two to five conjuncts mixing
    /// pure ones with ones that can fail (arithmetic over a zero or an
    /// extreme, a block read as a value whose own `where` can fail, a guard
    /// over them) in every written position, after elements that can fail
    /// (a `let`, a correlated `from` and an `optional` over a division).
    pub placement: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            max_depth: 2,
            max_elements: 4,
            search: true,
            nesting: true,
            aggregates: true,
            order_limit: true,
            placement: false,
        }
    }
}

/// A generated read: its source text (one declaration named `q`), its
/// parameter values by name (without the `$`; an omitted nullable one is
/// absent), and whether its rows are ordered.
#[derive(Debug, Clone)]
pub struct GenRead {
    pub source: String,
    pub params: Vec<(String, Literal)>,
    pub ordered: bool,
}

struct ReadGen<'s, 'c, 'd> {
    schema: &'s GenSchema,
    c: &'c mut Choices<'d>,
    options: ReadOptions,
    fresh: usize,
    /// Declared parameters: `(name, declaration, value)`.
    params: Vec<(String, String, Option<Literal>)>,
    /// Nodes an `optional` introduces, which a read does not return whole
    /// yet (`T86`).
    nullable_nodes: Vec<String>,
    /// Draw no term that can fail (arithmetic, unary minus, a numeric row,
    /// a `cast` to a number or from a String, `left`, `right`,
    /// `substring`): the shaper's fixed element order runs a later triple
    /// before an uncorrelated grouped `from` and a block a `let` binds, so
    /// where the triple matches nothing they never run, and the engine
    /// computes a block in `return` on rows the `where` removes, defects
    /// pinned as hunts (law_engine_reference.rs).
    pure: bool,
    /// A block drawn now is pure: the expression is a `let`'s value or a
    /// `return` item.
    pure_blocks: bool,
}

impl GenRead {
    pub fn generate(c: &mut Choices, schema: &GenSchema, options: ReadOptions) -> Self {
        let mut g = ReadGen {
            schema,
            c,
            options,
            fresh: 0,
            params: Vec::new(),
            nullable_nodes: Vec::new(),
            pure: false,
            pure_blocks: false,
        };
        let (body, ordered) = g.read_body();
        let decls: Vec<String> = g.params.iter().map(|(_, d, _)| d.clone()).collect();
        let source = format!("query q({}) {{\n{body}\n}}\n", decls.join(", "));
        // A parameter map is keyed by the name without its `$`.
        let params = g
            .params
            .into_iter()
            .filter_map(|(n, _, v)| v.map(|v| (n.trim_start_matches('$').to_string(), v)))
            .collect();
        GenRead {
            source,
            params,
            ordered,
        }
    }
}

impl ReadGen<'_, '_, '_> {
    fn var(&mut self, prefix: &str) -> String {
        self.fresh += 1;
        format!("${prefix}{}", self.fresh)
    }

    /// The top-level read: elements, `where`, `return`, `order`, `limit`.
    fn read_body(&mut self) -> (String, bool) {
        let mut scope = Vec::new();
        let mut elements = Vec::new();
        self.first_element(&mut scope, &mut elements);
        let n_more = self.c.below(self.options.max_elements);
        for _ in 0..n_more {
            self.element(&mut scope, &mut elements, self.options.max_depth);
        }
        if self.options.placement {
            self.failing_elements(&mut scope, &mut elements);
        }
        let mut text = format!("  match {{ {} }}", elements.join(", "));
        if self.options.placement {
            let w = self.placement_where(&scope);
            let _ = write!(text, "\n  where {w}");
        } else if self.c.flag(45) {
            let w = self.expr(Class::Bool, &scope, self.options.max_depth);
            let _ = write!(text, "\n  where {w}");
        }
        let (ret, aliases) = self.return_clause(&scope);
        let _ = write!(text, "\n  {ret}");
        let mut ordered = false;
        if self.options.order_limit && !aliases.is_empty() && self.c.flag(40) {
            let n = 1 + self.c.below(aliases.len().min(2));
            let keys: Vec<String> = (0..n)
                .map(|_| {
                    let a = self.c.pick(&aliases).clone();
                    let dir = *self.c.pick(&["", " asc", " desc"]);
                    let nulls = *self.c.pick(&["", "", " nulls first", " nulls last"]);
                    format!("{a}{dir}{nulls}")
                })
                .collect();
            let _ = write!(text, "\n  order {{ {} }}", keys.join(", "));
            ordered = true;
        }
        if self.options.order_limit && self.c.flag(30) {
            // `limit 0` included: the return items are computed on every
            // row before the cut, so a failing item refuses under it too.
            let lim = self.c.below(5);
            if self.c.flag(30) {
                let name = self.var("lim");
                self.params.push((
                    name.clone(),
                    format!("{name}: I64"),
                    Some(Literal::Integer(lim as i128)),
                ));
                let _ = write!(text, "\n  limit {name}");
            } else {
                let _ = write!(text, "\n  limit {lim}");
            }
        }
        (text, ordered)
    }

    /// The integer leaves in scope that a division may meet as zero: node
    /// and edge properties and integer `let`s.
    fn int_leaves(&self, scope: &[Var]) -> Vec<String> {
        self.leaves(Class::Int, scope)
    }

    /// A comparison that cannot fail: a property, `let` or key against a
    /// literal.
    fn pure_conjunct(&mut self, scope: &[Var]) -> String {
        let ints = self.int_leaves(scope);
        if !ints.is_empty() && self.c.flag(60) {
            let leaf = self.c.pick(&ints).clone();
            let op = *self.c.pick(&["=", "!=", "<", ">", "<=", ">="]);
            let lit = int_literal(INTS[self.c.below(7)]);
            return format!("({leaf} {op} {lit})");
        }
        let strs = self.leaves(Class::Str, scope);
        if strs.is_empty() {
            return self.literal(Class::Bool);
        }
        let leaf = self.c.pick(&strs).clone();
        let op = *self.c.pick(&["=", "!="]);
        format!("({leaf} {op} {})", self.literal(Class::Str))
    }

    /// A comparison over arithmetic that a zero divisor or an overflow in
    /// the graph makes fail; a pure one when no integer is in scope.
    fn failing_conjunct(&mut self, scope: &[Var]) -> String {
        let ints = self.int_leaves(scope);
        if ints.is_empty() {
            return self.pure_conjunct(scope);
        }
        let leaf = self.c.pick(&ints).clone();
        let lit = int_literal(INTS[1 + self.c.below(6)]);
        match self.c.below(7) {
            0 => format!(
                "(({lit} / {leaf}) > {})",
                int_literal(INTS[self.c.below(7)])
            ),
            1 => format!("(({lit} % {leaf}) = 0)"),
            2 => format!("(({leaf} * {}) > {lit})", int_literal(INTS[8])),
            // Registry rows that can fail over a stored value: the root of
            // a negative and the logarithm of a non-positive (`E5`), a
            // negative count (`E5`), a value no `I32` holds (`E6`).
            3 => format!("(sqrt({leaf}) > 1.0)"),
            4 => format!("(ln({leaf}) < 2.0)"),
            // A count is an integer: a property, not a `let` or a `from`'s
            // `sum`, which may be an `F64`.
            5 if leaf.contains('.') => format!("(left(\"ab\", {leaf}) = \"a\")"),
            5 => format!("(sqrt({leaf}) > 1.0)"),
            _ => format!("(cast(({leaf} * {}), I32) > {lit})", int_literal(INTS[6])),
        }
    }

    /// A block read as a value, its own `where` a conjunct that can fail
    /// or not.
    fn block_conjunct(&mut self, scope: &[Var]) -> Option<String> {
        let (t, new) = self.traversal(scope, false, true, true)?;
        let mut inner = scope.to_vec();
        inner.extend(new);
        let w = if self.c.flag(60) {
            self.failing_conjunct(&inner)
        } else {
            self.pure_conjunct(&inner)
        };
        let body = format!("{{ {t} where {w} }}");
        Some(match self.c.below(4) {
            0 => format!("(exists {body})"),
            1 => format!("(not exists {body})"),
            2 => format!("(count {body} > {})", self.c.below(2)),
            _ => format!("(count {body} = 0)"),
        })
    }

    /// One conjunct of [`Self::placement_where`].
    fn placement_conjunct(&mut self, scope: &[Var]) -> String {
        match self.c.below(7) {
            0 | 1 => self.pure_conjunct(scope),
            2 | 3 => self.failing_conjunct(scope),
            4 => self
                .block_conjunct(scope)
                .unwrap_or_else(|| self.failing_conjunct(scope)),
            5 => {
                let guard = self.pure_conjunct(scope);
                let guarded = self.failing_conjunct(scope);
                match self.c.below(3) {
                    0 => format!("({guard} or {guarded})"),
                    1 => format!("if({guard}, {guarded}, false)"),
                    _ => format!("({guard} and {guarded})"),
                }
            }
            _ => {
                let guard = self.pure_conjunct(scope);
                let block = self
                    .block_conjunct(scope)
                    .unwrap_or_else(|| self.failing_conjunct(scope));
                format!("({guard} or {block})")
            }
        }
    }

    /// The placement law's `where`: two to five conjuncts in written order.
    fn placement_where(&mut self, scope: &[Var]) -> String {
        let n = 2 + self.c.below(4);
        let conjuncts: Vec<String> = (0..n).map(|_| self.placement_conjunct(scope)).collect();
        conjuncts.join(" and ")
    }

    /// Elements that can fail, each with some chance, after the read's
    /// others: a `let` over a division, a correlated `from` summing one, and
    /// an `optional` whose own `where` holds one. The `where` after them
    /// may move below none of them.
    fn failing_elements(&mut self, scope: &mut Vec<Var>, elements: &mut Vec<String>) {
        let ints = self.int_leaves(scope);
        if !ints.is_empty() && self.c.flag(30) {
            let leaf = self.c.pick(&ints).clone();
            let v = self.var("l");
            let lit = int_literal(INTS[1 + self.c.below(6)]);
            elements.push(format!("let {v} = ({lit} / {leaf})"));
            scope.push(Var {
                name: v,
                kind: VarKind::Scalar(Sc::I64),
            });
        }
        if self.options.nesting && self.c.flag(30) {
            if let Some((t, new)) = self.traversal(scope, false, true, true) {
                let node = new.iter().find_map(|v| match v.kind {
                    VarKind::Node(t) => Some((v.name.clone(), t)),
                    _ => None,
                });
                let mut inner = scope.clone();
                inner.extend(new.clone());
                let arg = match node {
                    Some((name, t)) => {
                        let props: Vec<String> = self.schema.nodes[t]
                            .props
                            .iter()
                            .filter(|p| p.scalar().is_some_and(|s| s.class() == Class::Int))
                            .map(|p| format!("{name}.{}", p.name))
                            .collect();
                        if props.is_empty() {
                            None
                        } else {
                            let prop = self.c.pick(&props).clone();
                            Some(format!("({} / {prop})", int_literal(INTS[6])))
                        }
                    }
                    None => None,
                };
                if let Some(arg) = arg {
                    let s = self.var("s");
                    elements.push(format!(
                        "from {{ match {{ {t} }} return {{ sum({arg}) as {} }} }}",
                        &s[1..]
                    ));
                    scope.push(Var {
                        name: s,
                        kind: VarKind::Scalar(Sc::I64),
                    });
                }
            }
        }
        if self.options.nesting && self.c.flag(25) {
            if let Some((t, new)) = self.traversal(scope, false, true, true) {
                let mut inner = scope.clone();
                inner.extend(new.clone());
                let w = self.failing_conjunct(&inner);
                elements.push(format!("optional {{ {t} where {w} }}"));
                self.nullable_nodes
                    .extend(new.iter().map(|v| v.name.clone()));
                scope.extend(new);
            }
        }
    }

    fn node_vars(scope: &[Var]) -> Vec<(String, usize)> {
        scope
            .iter()
            .filter_map(|v| match v.kind {
                VarKind::Node(t) => Some((v.name.clone(), t)),
                _ => None,
            })
            .collect()
    }

    fn first_element(&mut self, scope: &mut Vec<Var>, elements: &mut Vec<String>) {
        let search_ty = self.schema.nodes[0]
            .props
            .iter()
            .any(|p| matches!(p.ty, PropTy::Vector(_)));
        if self.options.search && search_ty && self.c.flag(25) {
            self.ranking(scope, elements);
            return;
        }
        if self.options.nesting && self.c.flag(10) {
            // An uncorrelated grouped inline read, then a binding the
            // product pins by key (the written product of `from`).
            self.grouped_from(scope, elements);
        }
        let t = self.c.below(self.schema.nodes.len());
        let v = self.var("n");
        elements.push(format!("{v}: {}", self.schema.nodes[t].name));
        scope.push(Var {
            name: v,
            kind: VarKind::Node(t),
        });
    }

    /// `from { match { $x: T } return { $x.p as g, count($x) as n } }`.
    fn grouped_from(&mut self, scope: &mut Vec<Var>, elements: &mut Vec<String>) {
        let t = self.c.below(self.schema.nodes.len());
        let x = self.var("g");
        let mut inner = vec![Var {
            name: x.clone(),
            kind: VarKind::Node(t),
        }];
        let props: Vec<Prop> = self.schema.nodes[t]
            .props
            .iter()
            .filter(|p| p.scalar().is_some())
            .cloned()
            .collect();
        let mut items = Vec::new();
        let gname = self.var("k");
        let kname = self.var("m");
        let key_sc = if props.is_empty() || self.c.flag(30) {
            items.push(format!("{x}.k as {}", &gname[1..]));
            Sc::Str
        } else {
            let p = self.c.pick(&props).clone();
            items.push(format!("{x}.{} as {}", p.name, &gname[1..]));
            p.scalar().unwrap()
        };
        items.push(format!("count({x}) as {}", &kname[1..]));
        let w = if self.c.flag(40) {
            self.pure = true;
            let w = self.expr(Class::Bool, &inner, 1);
            self.pure = false;
            format!(" where {w}")
        } else {
            String::new()
        };
        inner.clear();
        elements.push(format!(
            "from {{ match {{ {x}: {} }}{w} return {{ {} }} }}",
            self.schema.nodes[t].name,
            items.join(", ")
        ));
        scope.push(Var {
            name: gname,
            kind: VarKind::Scalar(key_sc),
        });
        scope.push(Var {
            name: kname,
            kind: VarKind::Scalar(Sc::I64),
        });
    }

    /// A population of the first type, ranked: `nearest`, `distance`,
    /// `bm25` or `rrf` of two of them.
    fn ranking(&mut self, scope: &mut Vec<Var>, elements: &mut Vec<String>) {
        let t = 0;
        let d = self.var("d");
        let pop = self.var("pop");
        let ty = &self.schema.nodes[t].name;
        let inner = vec![Var {
            name: d.clone(),
            kind: VarKind::Node(t),
        }];
        let w = if self.c.flag(40) {
            format!(" where {}", self.expr(Class::Bool, &inner, 1))
        } else {
            String::new()
        };
        elements.push(format!(
            "let {pop} = {{ match {{ {d}: {ty} }}{w} return {{ {d} }} }}"
        ));
        let qv = self.vector_param();
        let k = 1 + self.c.below(4);
        let text = self.text_query();
        let nearest = format!("nearest({pop}, {d}.v, {qv}, k: {k})");
        let distance = format!("distance({pop}, {d}.v, {qv})");
        let bm25 = format!("bm25({pop}, {d}.t, {text})");
        let s = self.var("s");
        let call = match self.c.below(5) {
            0 => nearest,
            1 => distance,
            2 => bm25,
            3 => {
                let k2 = 1 + self.c.below(60);
                format!("rrf({nearest}, {bm25}, k: {k2})")
            }
            _ => {
                let (a, b) = (1 + self.c.below(3), 1 + self.c.below(3));
                format!("rrf({nearest}, {bm25}, k: 60, weights: [{a}.0, {b}.0])")
            }
        };
        elements.push(format!("from {call} as {s}"));
        scope.push(Var {
            name: d,
            kind: VarKind::Node(t),
        });
        scope.push(Var {
            name: s,
            kind: VarKind::Scalar(Sc::F64),
        });
    }

    fn vector_param(&mut self) -> String {
        let name = self.var("qv");
        let f = |c: &mut Choices| match c.below(4) {
            0 => 1.0,
            1 => 0.0,
            2 => -1.0,
            _ => 0.5,
        };
        let mut v = [f(self.c), f(self.c), f(self.c)];
        // A zero query vector on a `cosine` field is refused before the
        // read runs, which the reference does only where it scores a row,
        // a defect pinned as a hunt (law_engine_reference.rs).
        if v.iter().all(|x| *x == 0.0) {
            v[0] = 1.0;
        }
        let v = v.into_iter().map(Literal::Float).collect();
        self.params.push((
            name.clone(),
            format!("{name}: Vector(3)"),
            Some(Literal::List(v)),
        ));
        name
    }

    /// A lexical query with a searchable term: a stop word alone is `T20`.
    fn text_query(&mut self) -> String {
        let n = 1 + self.c.below(2);
        let words: Vec<&str> = (0..n)
            .map(|_| *self.c.pick(&WORDS[..WORDS.len() - 1]))
            .collect();
        json_string(&words.join(" "))
    }

    /// One element after the first.
    fn element(&mut self, scope: &mut Vec<Var>, elements: &mut Vec<String>, depth: u32) {
        let choice = self.c.below(10);
        match choice {
            // The placement law's reads draw their `optional` last (a
            // failing element): the shaper's fixed element order runs an
            // `optional` after elements written later, a defect of the
            // order pinned as a hunt (law_engine_reference.rs).
            1 if self.options.nesting && !self.options.placement => {
                self.optional(scope, elements, depth)
            }
            2 => {
                let ty = *self
                    .c
                    .pick(&[Class::Int, Class::Float, Class::Str, Class::Bool]);
                let v = self.var("l");
                self.pure_blocks = true;
                let e = self.expr(ty, scope, depth);
                self.pure_blocks = false;
                elements.push(format!("let {v} = {e}"));
                // The checker types the value; the generator only needs a
                // class it can read back: comparisons and arithmetic
                // accept either numeric class.
                scope.push(Var {
                    name: v,
                    kind: VarKind::Scalar(match ty {
                        Class::Int => Sc::I64,
                        Class::Float => Sc::F64,
                        Class::Str => Sc::Str,
                        _ => Sc::Bool,
                    }),
                });
            }
            3 if self.options.nesting && self.options.aggregates => {
                let v = self.var("l");
                self.pure_blocks = true;
                let block = self.block(Class::Int, scope, depth);
                self.pure_blocks = false;
                if let Some(block) = block {
                    elements.push(format!("let {v} = {block}"));
                    scope.push(Var {
                        name: v,
                        kind: VarKind::Scalar(Sc::I64),
                    });
                }
            }
            4 if self.options.nesting => self.union_from(scope, elements, depth),
            _ => {
                if let Some((text, new)) = self.traversal(scope, choice == 5, false, false) {
                    elements.push(text);
                    scope.extend(new);
                }
            }
        }
    }

    /// A triple from a node in scope to a fresh node, optionally binding
    /// the edge; with `close`, between two nodes in scope when one fits.
    /// With `keeps_rows` false, the triple is an element of the scope an
    /// `optional` introduced its nodes into: it reads none of them, which is
    /// `T90`, since its null would drop the row. A block, a later `optional`
    /// and an inline read correlate on one.
    fn traversal(
        &mut self,
        scope: &[Var],
        close: bool,
        nested: bool,
        keeps_rows: bool,
    ) -> Option<(String, Vec<Var>)> {
        let mut nodes = Self::node_vars(scope);
        if !keeps_rows {
            nodes.retain(|(n, _)| !self.nullable_nodes.contains(n));
        }
        if nodes.is_empty() {
            return None;
        }
        let (a, ta) = self.c.pick(&nodes).clone();
        let candidates: Vec<usize> = (0..self.schema.edges.len())
            .filter(|&e| {
                let edge = &self.schema.edges[e];
                edge.from == ta || edge.to == ta
            })
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let e = *self.c.pick(&candidates);
        let edge = &self.schema.edges[e];
        let forward = if edge.from == ta && edge.to == ta {
            self.c.below(2) == 0
        } else {
            edge.from == ta
        };
        let other_ty = if forward { edge.to } else { edge.from };
        let same = edge.from == edge.to;
        let name = edge.surface();
        // A hop range or an undirected selector on a same-type edge.
        let mut range = String::new();
        let mut selector = name.clone();
        let mut bind_edge = None;
        if same && self.c.flag(30) {
            range = (*self.c.pick(&["{1,2}", "{2,2}", "{1,3}", "{2,3}"])).to_string();
        }
        if same && self.c.flag(20) {
            selector = format!("<{name}>");
        }
        // Two edges of one orientation make an alternation.
        let alt: Vec<usize> = (0..self.schema.edges.len())
            .filter(|&o| {
                o != e
                    && self.schema.edges[o].from == edge.from
                    && self.schema.edges[o].to == edge.to
            })
            .collect();
        if !alt.is_empty() && !selector.starts_with('<') && self.c.flag(20) {
            let o = *self.c.pick(&alt);
            selector = format!("({name} | {})", self.schema.edges[o].surface());
        } else if range.is_empty() && !selector.starts_with('<') && self.c.flag(30) {
            bind_edge = Some(self.var("k"));
        }
        let bound_other: Vec<(String, usize)> = nodes
            .iter()
            .filter(|(_, t)| *t == other_ty)
            .cloned()
            .collect();
        let (b, fresh) = if close && !bound_other.is_empty() && bind_edge.is_none() {
            (self.c.pick(&bound_other).0.clone(), false)
        } else {
            (self.var("n"), true)
        };
        let edge_part = match &bind_edge {
            Some(k) => format!("{k}:{selector}"),
            None => selector,
        };
        let mut text = if forward {
            format!("{a} {edge_part}{range} {b}")
        } else {
            format!("{b} {edge_part}{range} {a}")
        };
        // A name new to a block, an `optional` or an inline read is bound
        // by a binding element inside it.
        if fresh && (nested || self.c.flag(30)) {
            text = format!("{b}: {}, {text}", self.schema.nodes[other_ty].name);
        }
        let mut new = Vec::new();
        if fresh {
            new.push(Var {
                name: b,
                kind: VarKind::Node(other_ty),
            });
        }
        if let Some(k) = bind_edge {
            new.push(Var {
                name: k,
                kind: VarKind::Edge(e),
            });
        }
        Some((text, new))
    }

    fn optional(&mut self, scope: &mut Vec<Var>, elements: &mut Vec<String>, depth: u32) {
        let Some((t, new)) = self.traversal(scope, false, true, true) else {
            return;
        };
        let mut inner = scope.clone();
        inner.extend(new.clone());
        let w = if depth > 0 && self.c.flag(40) {
            format!(" where {}", self.expr(Class::Bool, &inner, depth - 1))
        } else {
            String::new()
        };
        elements.push(format!("optional {{ {t}{w} }}"));
        self.nullable_nodes
            .extend(new.iter().map(|v| v.name.clone()));
        scope.extend(new);
    }

    /// `from union({ … return { $x as u } }, { … return { $y as u } })`,
    /// correlated on a node in scope, binding `$u`.
    fn union_from(&mut self, scope: &mut Vec<Var>, elements: &mut Vec<String>, depth: u32) {
        let Some((t1, new1)) = self.traversal(scope, false, true, true) else {
            return;
        };
        let Some(VarKind::Node(ty)) = new1.first().map(|v| v.kind.clone()) else {
            return;
        };
        let x1 = new1[0].name.clone();
        let u = self.var("u");
        let mut inner = scope.clone();
        inner.extend(new1.clone());
        let w1 = if depth > 0 && self.c.flag(50) {
            format!(" where {}", self.expr(Class::Bool, &inner, depth - 1))
        } else {
            String::new()
        };
        // The second arm: a scan of the same type, filtered.
        let x2 = self.var("n");
        let inner2 = vec![Var {
            name: x2.clone(),
            kind: VarKind::Node(ty),
        }];
        let w2 = if depth > 0 && self.c.flag(60) {
            format!(" where {}", self.expr(Class::Bool, &inner2, depth - 1))
        } else {
            String::new()
        };
        let un = &u[1..];
        elements.push(format!(
            "from union({{ match {{ {t1} }}{w1} return {{ {x1} as {un} }} }}, {{ match {{ {x2}: {} }}{w2} return {{ {x2} as {un} }} }})",
            self.schema.nodes[ty].name
        ));
        scope.push(Var {
            name: u,
            kind: VarKind::Node(ty),
        });
    }

    /// A block over a fresh traversal from a node in scope: `count`,
    /// `sum`, `min`, `max` (`Int`), `avg` (`Float`), `exists` (`Bool`).
    fn block(&mut self, class: Class, scope: &[Var], depth: u32) -> Option<String> {
        if depth == 0 {
            return None;
        }
        let pure = self.pure;
        self.pure |= self.pure_blocks;
        let block = self.block_body(class, scope, depth);
        self.pure = pure;
        block
    }

    fn block_body(&mut self, class: Class, scope: &[Var], depth: u32) -> Option<String> {
        let (t, new) = self.traversal(scope, false, true, true)?;
        let mut inner = scope.to_vec();
        inner.extend(new.clone());
        let w = if self.c.flag(40) {
            format!(" where {}", self.expr(Class::Bool, &inner, depth - 1))
        } else {
            String::new()
        };
        let body = format!("{{ {t}{w} }}");
        let new_node = new.iter().find_map(|v| match v.kind {
            VarKind::Node(t) => Some((v.name.clone(), t)),
            _ => None,
        });
        Some(match class {
            Class::Bool => match self.c.below(3) {
                0 => format!("(exists {body})"),
                1 => format!("(not exists {body})"),
                _ => format!("(count {body} > {})", self.c.below(3)),
            },
            Class::Float => {
                let arg = self.numeric_arg(new_node.as_ref(), &inner, depth - 1, Class::Float);
                format!("(avg({arg}) {body})")
            }
            _ => {
                if self.c.flag(60) {
                    format!("(count {body})")
                } else {
                    let agg = *self.c.pick(&["sum", "min", "max"]);
                    let arg = self.numeric_arg(new_node.as_ref(), &inner, depth - 1, Class::Int);
                    format!("({agg}({arg}) {body})")
                }
            }
        })
    }

    /// A numeric operand reading the block's fresh node when it has an
    /// integer property, any integer expression in scope otherwise.
    fn numeric_arg(
        &mut self,
        node: Option<&(String, usize)>,
        scope: &[Var],
        depth: u32,
        class: Class,
    ) -> String {
        if let Some((v, t)) = node {
            let props: Vec<&Prop> = self.schema.nodes[*t]
                .props
                .iter()
                .filter(|p| p.scalar().is_some_and(|s| s.class() == Class::Int))
                .collect();
            if !props.is_empty() {
                return format!("{v}.{}", self.c.pick(&props).name);
            }
        }
        self.expr(class, scope, depth)
    }

    /// `return`: plain items, `distinct` items, or groups and aggregates.
    /// The aliases it returns are the items an `order` may read: a list,
    /// a `collect`'s, is not one.
    fn return_clause(&mut self, scope: &[Var]) -> (String, Vec<String>) {
        let mut items = Vec::new();
        let mut aliases = Vec::new();
        let n = 1 + self.c.below(3);
        let aggregate = self.options.aggregates && self.c.flag(30);
        self.pure_blocks = true;
        for i in 0..n {
            let alias = format!("c{i}");
            let item = if aggregate && (i > 0 || self.c.flag(30)) {
                let item = self.aggregate(scope);
                if item.starts_with("collect(") {
                    items.push(format!("{item} as {alias}"));
                    continue;
                }
                item
            } else if !aggregate
                && self.options.nesting
                && self.options.aggregates
                && self.c.flag(8)
                && let Some(block) = self.collect_block(scope)
            {
                // A list per row is no group key and no `distinct` item, so
                // the read stays a plain projection.
                items.push(format!("{block} as {alias}"));
                continue;
            } else if self.c.flag(15) {
                // A node or edge reference.
                // A node reference; an edge returned whole is refused
                // (`T86`).
                let refs: Vec<&Var> = scope
                    .iter()
                    .filter(|v| {
                        matches!(v.kind, VarKind::Node(_)) && !self.nullable_nodes.contains(&v.name)
                    })
                    .collect();
                if refs.is_empty() {
                    self.any_expr(scope, 1)
                } else {
                    self.c.pick(&refs).name.clone()
                }
            } else {
                self.any_expr(scope, self.options.max_depth)
            };
            items.push(format!("{item} as {alias}"));
            aliases.push(alias);
        }
        self.pure_blocks = false;
        let distinct = !aggregate
            && !items
                .iter()
                .any(|item: &String| item.starts_with("(collect("))
            && self.c.flag(15);
        (
            format!(
                "return {}{{ {} }}",
                if distinct { "distinct " } else { "" },
                items.join(", ")
            ),
            aliases,
        )
    }

    /// `collect(x) { … }`, a block value: the list of a correlated
    /// pattern's values per outer row.
    fn collect_block(&mut self, scope: &[Var]) -> Option<String> {
        let pure = self.pure;
        self.pure |= self.pure_blocks;
        let block = self.collect_block_body(scope);
        self.pure = pure;
        block
    }

    fn collect_block_body(&mut self, scope: &[Var]) -> Option<String> {
        let (t, new) = self.traversal(scope, false, true, true)?;
        let mut inner = scope.to_vec();
        inner.extend(new);
        let w = if self.c.flag(40) {
            format!(" where {}", self.expr(Class::Bool, &inner, 0))
        } else {
            String::new()
        };
        let arg = self.any_expr(&inner, 1);
        Some(format!("(collect({arg}) {{ {t}{w} }})"))
    }

    fn aggregate(&mut self, scope: &[Var]) -> String {
        match self.c.below(11) {
            9 => {
                let class = *self.c.pick(&[Class::Int, Class::Float]);
                format!("stddev({})", self.expr(class, scope, 1))
            }
            10 => {
                let class = *self.c.pick(&[Class::Int, Class::Float]);
                let p = *self.c.pick(&["0", "0.25", "0.5", "0.3", "1"]);
                format!("percentile({}, {p})", self.expr(class, scope, 1))
            }
            7 => format!("collect({})", self.any_expr(scope, 1)),
            8 => format!("collect(distinct {})", self.any_expr(scope, 1)),
            0 | 1 => {
                let refs: Vec<&Var> = scope
                    .iter()
                    .filter(|v| matches!(v.kind, VarKind::Node(_)))
                    .collect();
                match refs.is_empty() {
                    true => "count(1)".to_string(),
                    false => format!("count({})", self.c.pick(&refs).name),
                }
            }
            2 => format!("sum({})", self.expr(Class::Int, scope, 1)),
            3 => format!("avg({})", self.expr(Class::Float, scope, 1)),
            4 => {
                let class = *self
                    .c
                    .pick(&[Class::Int, Class::Float, Class::Str, Class::Date]);
                format!("min({})", self.expr(class, scope, 1))
            }
            5 => {
                let class = *self
                    .c
                    .pick(&[Class::Int, Class::Float, Class::Str, Class::DateTime]);
                format!("max({})", self.expr(class, scope, 1))
            }
            _ => format!("count(distinct {})", self.any_expr(scope, 1)),
        }
    }

    fn any_expr(&mut self, scope: &[Var], depth: u32) -> String {
        let class = *self.c.pick(&[
            Class::Int,
            Class::Float,
            Class::Str,
            Class::Bool,
            Class::Date,
            Class::DateTime,
        ]);
        self.expr(class, scope, depth)
    }

    /// Readable leaves of `class`: properties of references in scope and
    /// scalar variables.
    fn leaves(&self, class: Class, scope: &[Var]) -> Vec<String> {
        let mut out = Vec::new();
        for v in scope {
            match v.kind {
                VarKind::Node(t) => {
                    for p in &self.schema.nodes[t].props {
                        if p.scalar().is_some_and(|s| s.class() == class) {
                            out.push(format!("{}.{}", v.name, p.name));
                        }
                    }
                    if class == Class::Str {
                        out.push(format!("{}.k", v.name));
                    }
                }
                VarKind::Edge(e) => {
                    for p in &self.schema.edges[e].props {
                        if p.scalar().is_some_and(|s| s.class() == class) {
                            out.push(format!("{}.{}", v.name, p.name));
                        }
                    }
                }
                VarKind::Scalar(s) if s.class() == class => out.push(v.name.clone()),
                VarKind::Scalar(_) => {}
            }
        }
        out
    }

    /// A parameter of a scalar type in `class`, declared now.
    fn param(&mut self, class: Class) -> String {
        let name = self.var("p");
        let sc = match class {
            Class::Int => *self.c.pick(&[Sc::I64, Sc::I32]),
            Class::Float => *self.c.pick(&[Sc::F64, Sc::F32]),
            Class::Bool => Sc::Bool,
            Class::Str => Sc::Str,
            Class::Date => Sc::Date,
            Class::DateTime => Sc::DateTime,
        };
        let nullable = self.c.flag(30);
        let value = if nullable && self.c.flag(40) {
            // Omitted: a nullable parameter without a value is null.
            None
        } else {
            Some(match sc {
                Sc::I32 | Sc::I64 | Sc::U32 => {
                    let v = INTS[self.c.below(8)];
                    Literal::Integer(i128::from(v))
                }
                Sc::F32 | Sc::F64 => {
                    Literal::Float(FLOATS[self.c.below(FLOATS.len())].parse().unwrap())
                }
                Sc::Bool => Literal::Bool(self.c.below(2) == 1),
                Sc::Str => Literal::String(str_value(self.c).to_string()),
                Sc::Date => Literal::Date(DATES[self.c.below(DATES.len())].to_string()),
                Sc::DateTime => {
                    Literal::DateTime(DATETIMES[self.c.below(DATETIMES.len())].to_string())
                }
            })
        };
        let q = if nullable { "?" } else { "" };
        self.params
            .push((name.clone(), format!("{name}: {}{q}", sc.spelling()), value));
        name
    }

    fn literal(&mut self, class: Class) -> String {
        match class {
            // A literal fits every integer type it meets (`T3` refuses one
            // outside a property's range); overflow comes from the values.
            Class::Int => int_literal(INTS[self.c.below(INTS.len() - 1)]),
            Class::Float => {
                let f = FLOATS[self.c.below(FLOATS.len())];
                match f.strip_prefix('-') {
                    Some(magnitude) => format!("(-{magnitude})"),
                    None => f.to_string(),
                }
            }
            Class::Bool => ["false", "true"][self.c.below(2)].to_string(),
            Class::Str => json_string(str_value(self.c)),
            Class::Date => format!("date({})", json_string(DATES[self.c.below(3)])),
            Class::DateTime => format!("datetime({})", json_string(DATETIMES[self.c.below(3)])),
        }
    }

    fn leaf(&mut self, class: Class, scope: &[Var]) -> String {
        let leaves = self.leaves(class, scope);
        match self.c.below(4) {
            0 | 1 if !leaves.is_empty() => self.c.pick(&leaves).clone(),
            2 => self.param(class),
            _ => self.literal(class),
        }
    }

    /// A well-typed expression of `class` over `scope`.
    pub(crate) fn expr(&mut self, class: Class, scope: &[Var], depth: u32) -> String {
        if depth == 0 {
            return self.leaf(class, scope);
        }
        let d = depth - 1;
        match class {
            Class::Int | Class::Float => match self.c.below(12) {
                0..=2 => self.leaf(class, scope),
                3..=5 if self.pure => self.leaf(class, scope),
                3 | 4 => {
                    let op = *self.c.pick(&["+", "-", "*", "/", "%"]);
                    let (l, r) = if class == Class::Float {
                        match self.c.below(3) {
                            0 => (Class::Float, Class::Int),
                            1 => (Class::Int, Class::Float),
                            _ => (Class::Float, Class::Float),
                        }
                    } else {
                        (Class::Int, Class::Int)
                    };
                    format!(
                        "({} {op} {})",
                        self.expr(l, scope, d),
                        self.expr(r, scope, d)
                    )
                }
                5 => format!("(-{})", self.expr(class, scope, d)),
                6 => format!(
                    "if({}, {}, {})",
                    self.expr(Class::Bool, scope, d),
                    self.expr(class, scope, d),
                    self.expr(class, scope, d)
                ),
                7 => format!(
                    "coalesce({}, {})",
                    self.expr(class, scope, d),
                    self.expr(class, scope, d)
                ),
                8 => match self.options.nesting && self.options.aggregates {
                    true => self
                        .block(class, scope, depth)
                        .unwrap_or_else(|| self.leaf(class, scope)),
                    false => self.leaf(class, scope),
                },
                9 | 10 if !self.pure => self.numeric_row(class, scope, d),
                11 if !self.pure => self.cast(class, scope, d),
                _ => self.leaf(class, scope),
            },
            Class::Str => match self.c.below(10) {
                0..=2 => self.leaf(class, scope),
                3 => format!(
                    "concat({}, {})",
                    self.expr(Class::Str, scope, d),
                    self.expr(Class::Str, scope, d)
                ),
                4 => format!(
                    "if({}, {}, {})",
                    self.expr(Class::Bool, scope, d),
                    self.expr(class, scope, d),
                    self.expr(class, scope, d)
                ),
                5 => format!(
                    "coalesce({}, {})",
                    self.expr(class, scope, d),
                    self.expr(class, scope, d)
                ),
                6 | 7 if !self.pure => self.string_row(scope, d),
                8 if !self.pure => self.substring(scope, d),
                // A cast to String converts every value.
                9 => self.cast(class, scope, d),
                _ => self.leaf(class, scope),
            },
            Class::Date | Class::DateTime => match self.c.below(5) {
                0..=2 => self.leaf(class, scope),
                3 => format!(
                    "coalesce({}, {})",
                    self.expr(class, scope, d),
                    self.expr(class, scope, d)
                ),
                _ => self.cast(class, scope, d),
            },
            Class::Bool => match self.c.below(13) {
                0 => self.leaf(class, scope),
                1..=3 => {
                    let (l, r) = *self.c.pick(&[
                        (Class::Int, Class::Int),
                        (Class::Int, Class::Float),
                        (Class::Float, Class::Float),
                        (Class::Str, Class::Str),
                        (Class::Date, Class::Date),
                        (Class::DateTime, Class::DateTime),
                        (Class::Bool, Class::Bool),
                    ]);
                    let op = *self.c.pick(&["=", "!=", "<", "<=", ">", ">="]);
                    format!(
                        "({} {op} {})",
                        self.expr(l, scope, d),
                        self.expr(r, scope, d)
                    )
                }
                4 => {
                    let class = *self.c.pick(&[Class::Int, Class::Str, Class::Float]);
                    format!(
                        "({} is {}null)",
                        self.expr(class, scope, d),
                        if self.c.below(2) == 0 { "" } else { "not " }
                    )
                }
                5 => {
                    let class = *self.c.pick(&[Class::Int, Class::Str]);
                    // Mostly a non-empty list; `in []` is false on every row.
                    let n = match self.c.flag(90) {
                        true => 1 + self.c.below(3),
                        false => 0,
                    };
                    let items: Vec<String> = (0..n)
                        .map(|_| match class {
                            Class::Int => INTS[self.c.below(8)].to_string(),
                            _ => json_string(STRS[self.c.below(STRS.len())]),
                        })
                        .collect();
                    format!("({} in [{}])", self.expr(class, scope, d), items.join(", "))
                }
                6 => format!("(not {})", self.expr(Class::Bool, scope, d)),
                7 => format!(
                    "({} and {})",
                    self.expr(Class::Bool, scope, d),
                    self.expr(Class::Bool, scope, d)
                ),
                8 => format!(
                    "({} or {})",
                    self.expr(Class::Bool, scope, d),
                    self.expr(Class::Bool, scope, d)
                ),
                9 => format!(
                    "if({}, {}, {})",
                    self.expr(Class::Bool, scope, d),
                    self.expr(Class::Bool, scope, d),
                    self.expr(Class::Bool, scope, d)
                ),
                10 => self
                    .membership(scope)
                    .unwrap_or_else(|| self.leaf(class, scope)),
                11 => match self.options.nesting {
                    true => self
                        .block(Class::Bool, scope, depth)
                        .unwrap_or_else(|| self.leaf(class, scope)),
                    false => self.leaf(class, scope),
                },
                _ => self.cast(class, scope, d),
            },
        }
    }

    /// A numeric registry row (the GQ 3 RFC, section 2): `abs`, `floor`,
    /// `ceil` and `round` (with and without `digits:`) keep their operand's
    /// type; an integer is also a String's `length` or a list's `size`, a
    /// float also `sqrt`, `exp`, `ln`, `log10` or `power` of any number.
    /// `round`'s digits, the roots and logarithms of negatives and zero and
    /// an `exp` or `power` that overflows can fail; most operands do not.
    fn numeric_row(&mut self, class: Class, scope: &[Var], d: u32) -> String {
        match self.c.below(4) {
            0 => {
                let f = *self.c.pick(&["abs", "floor", "ceil"]);
                format!("{f}({})", self.expr(class, scope, d))
            }
            1 => {
                let x = self.expr(class, scope, d);
                let digits = match self.c.below(7) {
                    0 | 1 => String::new(),
                    2 => ", digits: 0".to_string(),
                    3 => ", digits: 1".to_string(),
                    4 => ", digits: 2".to_string(),
                    5 => ", digits: (-1)".to_string(),
                    _ => format!(", digits: {}", self.int_arg(scope)),
                };
                format!("round({x}{digits})")
            }
            2 if class == Class::Int => {
                let lists = self.list_leaves(scope);
                if !lists.is_empty() && self.c.flag(50) {
                    format!("size({})", self.c.pick(&lists))
                } else {
                    format!("length({})", self.expr(Class::Str, scope, d))
                }
            }
            3 if class == Class::Int => format!("length({})", self.expr(Class::Str, scope, d)),
            2 => {
                let f = *self.c.pick(&["sqrt", "exp", "ln", "log10"]);
                let operand = *self.c.pick(&[Class::Int, Class::Float]);
                let x = self.expr(operand, scope, d);
                // Mostly an operand in the row's domain.
                match self.c.flag(50) {
                    true => format!("{f}(abs({x}))"),
                    false => format!("{f}({x})"),
                }
            }
            _ => {
                let (b, e) = *self.c.pick(&[
                    (Class::Float, Class::Int),
                    (Class::Int, Class::Int),
                    (Class::Float, Class::Float),
                ]);
                let base = self.expr(b, scope, d);
                let exponent = match self.c.flag(60) {
                    true => (*self.c.pick(&["0", "1", "2", "0.5", "(-1)"])).to_string(),
                    false => self.expr(e, scope, d),
                };
                format!("power({base}, {exponent})")
            }
        }
    }

    /// A String registry row: `lower`, `upper` (Unicode case mapping),
    /// `trim`, `ltrim`, `rtrim` (Unicode whitespace), `left` and `right`
    /// (a negative count fails).
    fn string_row(&mut self, scope: &[Var], d: u32) -> String {
        match self.c.below(2) {
            0 => {
                let f = *self.c.pick(&["lower", "upper", "trim", "ltrim", "rtrim"]);
                format!("{f}({})", self.expr(Class::Str, scope, d))
            }
            _ => {
                let f = *self.c.pick(&["left", "right"]);
                let s = self.expr(Class::Str, scope, d);
                let n = self.count(scope);
                format!("{f}({s}, {n})")
            }
        }
    }

    /// `substring(s, start, len)`: a start before, at or past the String's
    /// ends (clipped), a length that is mostly not negative.
    fn substring(&mut self, scope: &[Var], d: u32) -> String {
        let s = self.expr(Class::Str, scope, d);
        let start = match self.c.flag(20) {
            true => self.int_arg(scope),
            false => (*self.c.pick(&["1", "2", "0", "4", "(-1)"])).to_string(),
        };
        let len = self.count(scope);
        format!("substring({s}, {start}, {len})")
    }

    /// A count of characters: mostly a small literal, sometimes negative
    /// (`E5`), sometimes an integer in scope.
    fn count(&mut self, scope: &[Var]) -> String {
        match self.c.flag(20) {
            true => self.int_arg(scope),
            false => (*self.c.pick(&["1", "2", "0", "3", "5", "(-1)"])).to_string(),
        }
    }

    /// An operand an integer argument accepts: an integer property, an
    /// integer parameter or literal. An integer-class expression may be an
    /// `F64` (a block's `sum`, a `let` of one), which `left`, `substring`,
    /// `round`'s digits and a `cast` to `Bool` refuse.
    fn int_arg(&mut self, scope: &[Var]) -> String {
        let props: Vec<String> = self
            .leaves(Class::Int, scope)
            .into_iter()
            .filter(|l| l.contains('.'))
            .collect();
        match self.c.below(3) {
            0 if !props.is_empty() => self.c.pick(&props).clone(),
            1 => self.param(Class::Int),
            _ => self.literal(Class::Int),
        }
    }

    /// `cast(x, T)` to a type of `class`, from a class `cast_converts`
    /// admits: numbers between each other, a Bool and an integer, a String
    /// and anything, a Date and a DateTime. A String read as another type is
    /// mostly a text `cast` reads or refuses ([`NUMBER_TEXTS`],
    /// [`BOOL_TEXTS`], a date literal or a date that does not exist).
    fn cast(&mut self, class: Class, scope: &[Var], d: u32) -> String {
        use Class::*;
        let (target, sources): (&str, &[Class]) = match class {
            Int => (*self.c.pick(&["I64", "I32"]), &[Int, Float, Bool, Str]),
            Float => (*self.c.pick(&["F64", "F32"]), &[Int, Float, Str]),
            Str => ("String", &[Int, Float, Bool, Str, Date, DateTime]),
            Bool => ("Bool", &[Int, Bool, Str]),
            Date => ("Date", &[Date, DateTime, Str]),
            DateTime => ("DateTime", &[DateTime, Date, Str]),
        };
        let mut from = *self.c.pick(sources);
        // A `Bool` from an integer, a `Date` from a `DateTime` and the
        // reverse convert every value.
        if self.pure && from == Str && class != Str {
            from = sources[0];
        }
        let x = if from == Int && class == Bool {
            self.int_arg(scope)
        } else if from == Str && class != Str && self.c.flag(60) {
            let text = match class {
                Int | Float => *self.c.pick(&NUMBER_TEXTS),
                Bool => *self.c.pick(&BOOL_TEXTS),
                Date => *self
                    .c
                    .pick(&[DATES[0], DATES[2], DATETIMES[0], "2023-02-29"]),
                _ => *self
                    .c
                    .pick(&[DATETIMES[0], DATETIMES[1], DATES[0], "2024-01-01T25:00:00Z"]),
            };
            json_string(text)
        } else {
            self.expr(from, scope, d)
        };
        format!("cast({x}, {target})")
    }

    /// The list properties of the nodes in scope.
    fn list_leaves(&self, scope: &[Var]) -> Vec<String> {
        let mut out = Vec::new();
        for v in scope {
            if let VarKind::Node(t) = v.kind {
                for p in &self.schema.nodes[t].props {
                    if p.ty == PropTy::ListStr {
                        out.push(format!("{}.{}", v.name, p.name));
                    }
                }
            }
        }
        out
    }

    /// A string or enum membership test against a list or enum property.
    fn membership(&mut self, scope: &[Var]) -> Option<String> {
        let mut options = Vec::new();
        for v in scope {
            if let VarKind::Node(t) = v.kind {
                for p in &self.schema.nodes[t].props {
                    match p.ty {
                        PropTy::ListStr => options.push((format!("{}.{}", v.name, p.name), true)),
                        PropTy::Enum => options.push((format!("{}.{}", v.name, p.name), false)),
                        _ => {}
                    }
                }
            }
        }
        if options.is_empty() {
            return None;
        }
        let (prop, list) = self.c.pick(&options).clone();
        Some(if list {
            let needle = self.expr(Class::Str, scope, 0);
            format!("({needle} in {prop})")
        } else {
            let m = json_string(ENUM_MEMBERS[self.c.below(3)]);
            match self.c.below(2) {
                0 => format!("({prop} = {m})"),
                // The list sometimes holds declared non-null String
                // parameters beside the literal, drawn after it, so a read
                // that does not take this branch keeps its choices.
                _ if self.c.flag(40) => {
                    let mut elements = vec![m];
                    for _ in 0..1 + self.c.below(2) {
                        let p = self.list_param();
                        elements.insert(self.c.below(elements.len() + 1), p);
                    }
                    format!("({prop} in [{}])", elements.join(", "))
                }
                _ => format!("({prop} in [{m}])"),
            }
        })
    }

    /// A list literal's parameter element: a non-null `String` parameter,
    /// declared now, bound to an enum member or to a value outside the enum.
    fn list_param(&mut self) -> String {
        let name = self.var("p");
        let value = *self
            .c
            .pick(&[ENUM_MEMBERS[0], ENUM_MEMBERS[1], ENUM_MEMBERS[2], "purple"]);
        self.params.push((
            name.clone(),
            format!("{name}: String"),
            Some(Literal::String(value.to_string())),
        ));
        name
    }
}

/// The text of a scalar expression of `class` over one node variable `$x`
/// of `schema`'s type `t`, for the scalar laws.
pub fn scalar_expr(
    c: &mut Choices,
    schema: &GenSchema,
    t: usize,
    class: Class,
    depth: u32,
) -> (String, Vec<(String, String, Option<Literal>)>) {
    let mut g = ReadGen {
        schema,
        c,
        options: ReadOptions {
            nesting: false,
            ..ReadOptions::default()
        },
        fresh: 0,
        params: Vec::new(),
        nullable_nodes: Vec::new(),
        pure: false,
        pure_blocks: false,
    };
    let scope = vec![Var {
        name: "$x".into(),
        kind: VarKind::Node(t),
    }];
    let e = g.expr(class, &scope, depth);
    (e, g.params)
}

// ---------------------------------------------------------------- spellings

/// A non-canonical spelling the GQ 3 RFC admits (section 3, "Added:
/// arithmetic"), each of which lowers to its canonical form's tree and is
/// on lint's canonical-forms list (section 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Spelling {
    /// `case when c then a else b end` for `if(c, a, b)`, the simple
    /// `case x when v then a else b end` for `if(x = v, a, b)`, and no
    /// `else` for `if(c, a, null)` (`L212`).
    Case,
    /// `(a) || (b) || …` for `concat(a, b, …)` (`L213`).
    Bars,
    /// `<>` for `!=` (`L214`).
    NotEqual,
    /// A single-quoted string for a double-quoted one (`L215`).
    SingleQuote,
    /// A keyword or registry name in upper or mixed case (`L216`).
    LetterCase,
}

impl Spelling {
    pub const ALL: [Spelling; 5] = [
        Spelling::Case,
        Spelling::Bars,
        Spelling::NotEqual,
        Spelling::SingleQuote,
        Spelling::LetterCase,
    ];

    /// Lint's code for the spelling.
    pub fn code(self) -> &'static str {
        match self {
            Spelling::Case => "L212",
            Spelling::Bars => "L213",
            Spelling::NotEqual => "L214",
            Spelling::SingleQuote => "L215",
            Spelling::LetterCase => "L216",
        }
    }
}

/// `text` written with the non-canonical spellings of `forms` at places a
/// seeded stream picks, each of which parses to the tree its canonical
/// form does: some keywords and registry names in upper or mixed case,
/// `<>` for `!=`, single-quoted strings, `||` for a `concat` of two or more
/// arguments, and `case` for a three-argument `if` (the simple form for an
/// equality condition, no `else` for a `null` one). Driven by the parse
/// tree, so a property, edge or alias name that is a keyword keeps its
/// spelling. Returns the text and the forms it wrote; `text` itself when it
/// does not parse.
pub fn respell_spellings(text: &str, seed: u64, forms: &[Spelling]) -> (String, Vec<Spelling>) {
    use crate::query::syntax::{QueryParser, Rule};
    use pest::Parser as _;
    // SplitMix64, so a small seed draws as evenly as a large one.
    let mut state = seed;
    let mut next = move || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let Ok(pairs) = QueryParser::parse(Rule::query_file, text) else {
        return (text.to_string(), Vec::new());
    };
    let on = |form: Spelling| forms.contains(&form);
    let positionals = |call: &pest::iterators::Pair<Rule>| -> Option<Vec<String>> {
        let mut out = Vec::new();
        for args in call
            .clone()
            .into_inner()
            .filter(|p| p.as_rule() == Rule::call_args)
        {
            for a in args.into_inner() {
                match a.as_rule() {
                    Rule::positional => {
                        let v = a.into_inner().next()?;
                        if v.as_rule() != Rule::expr {
                            return None;
                        }
                        out.push(v.as_str().trim().to_string());
                    }
                    Rule::missing_comma_rec => {}
                    _ => return None,
                }
            }
        }
        Some(out)
    };
    // Every `=` comparison, by its text: the condition a simple `case`
    // writes as its operand and its `when` value.
    let equalities: Vec<(String, String, String)> = pairs
        .clone()
        .flatten()
        .filter(|p| p.as_rule() == Rule::comparison)
        .filter_map(|p| {
            let parts: Vec<_> = p.clone().into_inner().collect();
            match parts.as_slice() {
                [l, op, r] if op.as_rule() == Rule::comp_op && op.as_str() == "=" => Some((
                    p.as_str().trim().to_string(),
                    l.as_str().trim().to_string(),
                    r.as_str().trim().to_string(),
                )),
                _ => None,
            }
        })
        .collect();
    let equality = |condition: &str| {
        let mut c = condition.trim();
        loop {
            if let Some((_, l, r)) = equalities.iter().find(|(whole, _, _)| whole == c) {
                return Some((l.clone(), r.clone()));
            }
            match c.strip_prefix('(').and_then(|c| c.strip_suffix(')')) {
                Some(inner) => c = inner.trim(),
                None => return None,
            }
        }
    };
    // A call that is a `from`'s relation is no expression to respell.
    let relations: Vec<usize> = pairs
        .clone()
        .flatten()
        .filter(|p| p.as_rule() == Rule::relation_expr)
        .map(|p| p.as_span().start())
        .collect();
    let mut edits: Vec<(usize, usize, String, Spelling)> = Vec::new();
    for p in pairs.flatten() {
        let (start, end) = (p.as_span().start(), p.as_span().end());
        let word = p.as_str();
        let rule = p.as_rule();
        let replacement = match rule {
            Rule::call if relations.contains(&start) => None,
            Rule::call => {
                let name = p
                    .clone()
                    .into_inner()
                    .find(|n| n.as_rule() == Rule::call_name)
                    .map(|n| n.as_str().to_string());
                match (name.as_deref(), positionals(&p)) {
                    (Some("concat"), Some(args))
                        if on(Spelling::Bars) && args.len() >= 2 && next() % 2 == 0 =>
                    {
                        Some((
                            format!(
                                "({})",
                                args.iter()
                                    .map(|a| format!("({a})"))
                                    .collect::<Vec<_>>()
                                    .join(" || ")
                            ),
                            Spelling::Bars,
                        ))
                    }
                    (Some("if"), Some(args))
                        if on(Spelling::Case) && args.len() == 3 && next() % 2 == 0 =>
                    {
                        let otherwise = if args[2] == "null" && next() % 2 == 0 {
                            String::new()
                        } else {
                            format!(" else ({})", args[2])
                        };
                        Some((
                            match equality(&args[0]) {
                                Some((x, v)) if next() % 2 == 0 => format!(
                                    "(case ({x}) when ({v}) then ({}){otherwise} end)",
                                    args[1]
                                ),
                                _ => format!(
                                    "(case when ({}) then ({}){otherwise} end)",
                                    args[0], args[1]
                                ),
                            },
                            Spelling::Case,
                        ))
                    }
                    _ => None,
                }
            }
            Rule::comp_op if on(Spelling::NotEqual) && word == "!=" && next() % 2 == 0 => {
                Some(("<>".to_string(), Spelling::NotEqual))
            }
            Rule::string_lit
                if on(Spelling::SingleQuote) && word.starts_with('"') && next() % 3 == 0 =>
            {
                crate::error::decode_string_literal(word).ok().map(|value| {
                    let mut out = String::from("'");
                    for ch in value.chars() {
                        match ch {
                            '\'' => out.push_str("\\'"),
                            '\\' => out.push_str("\\\\"),
                            '\n' => out.push_str("\\n"),
                            '\r' => out.push_str("\\r"),
                            '\t' => out.push_str("\\t"),
                            other => out.push(other),
                        }
                    }
                    out.push('\'');
                    (out, Spelling::SingleQuote)
                })
            }
            _ if on(Spelling::LetterCase) => {
                let keyword = matches!(
                    rule,
                    Rule::agg_name
                        | Rule::order_dir
                        | Rule::bool_lit
                        | Rule::stmt_keyword
                        | Rule::load_kind
                        | Rule::load_mode_name
                ) || format!("{rule:?}").starts_with("kw_")
                    || (rule == Rule::call_name && crate::query::check::is_registry_name(word));
                (keyword && next() % 4 == 0).then(|| {
                    let respelled = if next() % 2 == 0 {
                        word.to_ascii_uppercase()
                    } else {
                        let mut w = word.to_string();
                        w[..1].make_ascii_uppercase();
                        w
                    };
                    (respelled, Spelling::LetterCase)
                })
            }
            _ => None,
        };
        if let Some((r, form)) = replacement
            && r != word
            && edits.last().is_none_or(|(_, e, _, _)| *e <= start)
        {
            edits.push((start, end, r, form));
        }
    }
    let mut out = text.to_string();
    let mut written = Vec::new();
    for (start, end, r, form) in edits.into_iter().rev() {
        out.replace_range(start..end, &r);
        if !written.contains(&form) {
            written.push(form);
        }
    }
    (out, written)
}

// ---------------------------------------------------------------- scalar laws

/// The schema of the scalar laws: one node type with a nullable property
/// of every scalar type, `p_<type>`.
pub const SCALAR_SCHEMA: &str = "node X {
    k: String @key
    p_i32: I32?
    p_i64: I64?
    p_u32: U32?
    p_f32: F32?
    p_f64: F64?
    p_bool: Bool?
    p_str: String?
}
";

/// The scalar types of the scalar laws, as their property and parameter
/// names spell them.
pub const SCALAR_INPUTS: [Sc; 7] = [
    Sc::I32,
    Sc::I64,
    Sc::U32,
    Sc::F32,
    Sc::F64,
    Sc::Bool,
    Sc::Str,
];

fn input_name(sc: Sc) -> &'static str {
    match sc {
        Sc::I32 => "i32",
        Sc::I64 => "i64",
        Sc::U32 => "u32",
        Sc::F32 => "f32",
        Sc::F64 => "f64",
        Sc::Bool => "bool",
        Sc::Str => "str",
        Sc::Date | Sc::DateTime => unreachable!("no temporal input"),
    }
}

/// One scalar expression of the scalar laws, as a tree, so its RFC type can
/// be computed independently of the checker.
#[derive(Debug, Clone)]
pub enum ScalarExpr {
    /// The input of type `Sc`: `$x.p_<t>` or `$<t>`.
    Input(Sc),
    /// An integer literal (never negative; [`ScalarExpr::Neg`] negates).
    Int(i64),
    /// A float literal, as written.
    Float(&'static str),
    Bool(bool),
    Str(&'static str),
    Arith(&'static str, Box<ScalarExpr>, Box<ScalarExpr>),
    Neg(Box<ScalarExpr>),
    If(Box<ScalarExpr>, Box<ScalarExpr>, Box<ScalarExpr>),
    Coalesce(Box<ScalarExpr>, Box<ScalarExpr>),
    Cmp(&'static str, Box<ScalarExpr>, Box<ScalarExpr>),
    And(Box<ScalarExpr>, Box<ScalarExpr>),
    Or(Box<ScalarExpr>, Box<ScalarExpr>),
    Not(Box<ScalarExpr>),
    IsNull(Box<ScalarExpr>, bool),
    /// A one-argument registry row: `lower`, `upper`, `trim`, `ltrim`,
    /// `rtrim`, `length`, `abs`, `floor`, `ceil`, `sqrt`, `exp`, `ln`,
    /// `log10`.
    Row(&'static str, Box<ScalarExpr>),
    /// `left(s, n)` or `right(s, n)`.
    Count(&'static str, Box<ScalarExpr>, Box<ScalarExpr>),
    /// `substring(s, start, len)`.
    Substring(Box<ScalarExpr>, Box<ScalarExpr>, Box<ScalarExpr>),
    /// `round(x)` or `round(x, digits: n)`.
    Round(Box<ScalarExpr>, Option<Box<ScalarExpr>>),
    /// `power(base, exponent)`.
    Power(Box<ScalarExpr>, Box<ScalarExpr>),
    /// `cast(x, T)`.
    Cast(Box<ScalarExpr>, Sc),
}

/// The String literals and inputs of the scalar laws: plain values and
/// [`TEXTS`].
const SCALAR_STRS: [&str; 16] = [
    "",
    "a",
    "ab",
    "b",
    " a ",
    "\u{a0}ab\u{2003}",
    "\tb",
    "  ",
    "ß",
    "İ",
    "ΑΣ",
    "12",
    "+3",
    "1e3",
    " 1",
    "1.5",
];
const SCALAR_STR_TEXTS: [&str; 6] = ["inf", "NaN", "true", "TRUE", "yes", "-0.0"];

/// The types a `cast` of the scalar laws reaches from each class:
/// `cast_converts`' pairs over the laws' inputs.
fn cast_targets(class: Class) -> &'static [Sc] {
    match class {
        Class::Int => &[
            Sc::I32,
            Sc::I64,
            Sc::U32,
            Sc::F32,
            Sc::F64,
            Sc::Str,
            Sc::Bool,
        ],
        Class::Float => &[Sc::I32, Sc::I64, Sc::U32, Sc::F32, Sc::F64, Sc::Str],
        Class::Bool => &[Sc::I32, Sc::I64, Sc::U32, Sc::Str, Sc::Bool],
        _ => &[
            Sc::I32,
            Sc::I64,
            Sc::U32,
            Sc::F32,
            Sc::F64,
            Sc::Str,
            Sc::Bool,
        ],
    }
}

/// How an expression's inputs are spelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inputs {
    /// `$x.p_<t>`: read from the row, evaluated per row.
    Props,
    /// `$<t>`: parameters, bound before execution.
    Params,
}

/// A scalar expression's RFC type: a scalar and whether it is nullable;
/// `None` for an integer literal, whose type the other operand decides.
pub type RfcType = (Option<Sc>, bool);

impl ScalarExpr {
    /// A well-typed expression of `class` (`Int`, `Float`, `Bool`, `Str`).
    pub fn generate(c: &mut Choices, class: Class, depth: u32) -> Self {
        let leaf = |c: &mut Choices, class: Class| -> ScalarExpr {
            match class {
                Class::Int => match c.below(3) {
                    0 => ScalarExpr::Int(*c.pick(&[0, 1, 2, 3, 7, 10, 100, 2_147_483_647])),
                    _ => ScalarExpr::Input(*c.pick(&[Sc::I32, Sc::I64, Sc::U32])),
                },
                Class::Float => match c.below(3) {
                    0 => ScalarExpr::Float(c.pick(&["0.0", "0.5", "1.5", "3.0", "1.5e3"])),
                    _ => ScalarExpr::Input(*c.pick(&[Sc::F32, Sc::F64])),
                },
                Class::Bool => match c.below(3) {
                    0 => ScalarExpr::Bool(c.below(2) == 1),
                    _ => ScalarExpr::Input(Sc::Bool),
                },
                _ => match c.below(3) {
                    0 => ScalarExpr::Str(match c.flag(20) {
                        true => c.pick(&SCALAR_STR_TEXTS),
                        false => c.pick(&SCALAR_STRS),
                    }),
                    _ => ScalarExpr::Input(Sc::Str),
                },
            }
        };
        if depth == 0 {
            return leaf(c, class);
        }
        let d = depth - 1;
        let sub = |c: &mut Choices, class: Class| Box::new(Self::generate(c, class, d));
        // A cast to a type of `class` from any class that converts to it.
        let cast = |c: &mut Choices, class: Class| {
            let targets: Vec<Sc> = SCALAR_INPUTS
                .iter()
                .copied()
                .filter(|t| t.class() == class)
                .collect();
            let target = *c.pick(&targets);
            let from = *c.pick(&[Class::Int, Class::Float, Class::Bool, Class::Str]);
            if cast_targets(from).contains(&target) {
                ScalarExpr::Cast(sub(c, from), target)
            } else {
                ScalarExpr::Cast(sub(c, Class::Str), target)
            }
        };
        match class {
            Class::Int | Class::Float => match c.below(12) {
                0 | 1 => leaf(c, class),
                2 | 3 => {
                    let op = *c.pick(&["+", "-", "*", "/", "%"]);
                    let (l, r) = match class {
                        Class::Float => *c.pick(&[
                            (Class::Float, Class::Float),
                            (Class::Int, Class::Float),
                            (Class::Float, Class::Int),
                        ]),
                        _ => (Class::Int, Class::Int),
                    };
                    ScalarExpr::Arith(op, sub(c, l), sub(c, r))
                }
                4 => ScalarExpr::Neg(sub(c, class)),
                5 => ScalarExpr::If(sub(c, Class::Bool), sub(c, class), sub(c, class)),
                6 => ScalarExpr::Coalesce(sub(c, class), sub(c, class)),
                7 => ScalarExpr::Row(c.pick(&["abs", "floor", "ceil"]), sub(c, class)),
                8 => {
                    let x = sub(c, class);
                    let digits = match c.below(3) {
                        0 => None,
                        _ => Some(Box::new(match c.flag(70) {
                            true => match c.below(5) {
                                4 => ScalarExpr::Neg(Box::new(ScalarExpr::Int(*c.pick(&[1, 2])))),
                                n => ScalarExpr::Int(n as i64),
                            },
                            false => Self::generate(c, Class::Int, d),
                        })),
                    };
                    ScalarExpr::Round(x, digits)
                }
                9 if class == Class::Int => ScalarExpr::Row("length", sub(c, Class::Str)),
                9 => match c.below(5) {
                    4 => {
                        let b = *c.pick(&[Class::Float, Class::Int]);
                        let e = *c.pick(&[Class::Int, Class::Float]);
                        ScalarExpr::Power(sub(c, b), sub(c, e))
                    }
                    _ => {
                        let f = *c.pick(&["sqrt", "exp", "ln", "log10"]);
                        let operand = *c.pick(&[Class::Int, Class::Float]);
                        ScalarExpr::Row(f, sub(c, operand))
                    }
                },
                _ => cast(c, class),
            },
            Class::Bool => match c.below(9) {
                0 => leaf(c, class),
                1 | 2 => {
                    let (l, r) = *c.pick(&[
                        (Class::Int, Class::Int),
                        (Class::Int, Class::Float),
                        (Class::Float, Class::Float),
                        (Class::Str, Class::Str),
                        (Class::Bool, Class::Bool),
                    ]);
                    let op = *c.pick(&["=", "!=", "<", "<=", ">", ">="]);
                    ScalarExpr::Cmp(op, sub(c, l), sub(c, r))
                }
                3 => ScalarExpr::And(sub(c, Class::Bool), sub(c, Class::Bool)),
                4 => ScalarExpr::Or(sub(c, Class::Bool), sub(c, Class::Bool)),
                5 => ScalarExpr::Not(sub(c, Class::Bool)),
                6 => {
                    let class = *c.pick(&[Class::Int, Class::Float, Class::Str]);
                    ScalarExpr::IsNull(sub(c, class), c.below(2) == 1)
                }
                7 => ScalarExpr::If(
                    sub(c, Class::Bool),
                    sub(c, Class::Bool),
                    sub(c, Class::Bool),
                ),
                _ => cast(c, class),
            },
            _ => match c.below(8) {
                0 | 1 => leaf(c, class),
                2 => ScalarExpr::If(sub(c, Class::Bool), sub(c, class), sub(c, class)),
                3 => ScalarExpr::Coalesce(sub(c, class), sub(c, class)),
                4 => ScalarExpr::Row(
                    c.pick(&["lower", "upper", "trim", "ltrim", "rtrim"]),
                    sub(c, class),
                ),
                5 => ScalarExpr::Count(
                    c.pick(&["left", "right"]),
                    sub(c, class),
                    sub(c, Class::Int),
                ),
                6 => ScalarExpr::Substring(sub(c, class), sub(c, Class::Int), sub(c, Class::Int)),
                _ => cast(c, class),
            },
        }
    }

    /// Whether the tree reads an unsigned value: a `U32` input or a cast to
    /// `U32`.
    fn reads_unsigned(&self) -> bool {
        let mut inputs = Vec::new();
        self.inputs(&mut inputs);
        inputs.contains(&Sc::U32) || self.casts_to_unsigned()
    }

    fn casts_to_unsigned(&self) -> bool {
        match self {
            ScalarExpr::Cast(_, Sc::U32) => true,
            _ => self.children().iter().any(|e| e.casts_to_unsigned()),
        }
    }

    /// The direct operands.
    fn children(&self) -> Vec<&ScalarExpr> {
        match self {
            ScalarExpr::Arith(_, l, r)
            | ScalarExpr::Cmp(_, l, r)
            | ScalarExpr::And(l, r)
            | ScalarExpr::Or(l, r)
            | ScalarExpr::Coalesce(l, r)
            | ScalarExpr::Count(_, l, r)
            | ScalarExpr::Power(l, r) => vec![l, r],
            ScalarExpr::If(a, b, c) | ScalarExpr::Substring(a, b, c) => vec![a, b, c],
            ScalarExpr::Neg(e)
            | ScalarExpr::Not(e)
            | ScalarExpr::IsNull(e, _)
            | ScalarExpr::Row(_, e)
            | ScalarExpr::Cast(e, _) => vec![e],
            ScalarExpr::Round(x, digits) => {
                let mut out: Vec<&ScalarExpr> = vec![x];
                out.extend(digits.as_deref());
                out
            }
            ScalarExpr::Input(_)
            | ScalarExpr::Int(_)
            | ScalarExpr::Float(_)
            | ScalarExpr::Bool(_)
            | ScalarExpr::Str(_) => Vec::new(),
        }
    }

    /// Whether the tree holds an unsigned operand under a unary minus, or
    /// reads an unsigned input and holds a negated literal: the first is a
    /// refusal of the checker the generator does not aim for, the second a
    /// literal rule the RFC leaves open for `-0` beside `U32`.
    pub fn negates_unsigned(&self) -> bool {
        if self.reads_unsigned() && self.text(Inputs::Props).contains("(-") {
            return true;
        }
        match self {
            ScalarExpr::Neg(inner) => {
                inner.rfc_type().0 == Some(Sc::U32) || inner.negates_unsigned()
            }
            ScalarExpr::Arith(_, l, r)
            | ScalarExpr::Cmp(_, l, r)
            | ScalarExpr::And(l, r)
            | ScalarExpr::Or(l, r)
            | ScalarExpr::Coalesce(l, r) => {
                let (lt, rt) = (l.rfc_type().0, r.rfc_type().0);
                let negative_literal = |e: &ScalarExpr| matches!(e, ScalarExpr::Neg(i) if matches!(**i, ScalarExpr::Int(_)));
                (lt == Some(Sc::U32) && negative_literal(r))
                    || (rt == Some(Sc::U32) && negative_literal(l))
                    || l.negates_unsigned()
                    || r.negates_unsigned()
            }
            other => other.children().iter().any(|e| e.negates_unsigned()),
        }
    }

    /// The text of the expression with its inputs spelled per `inputs`.
    pub fn text(&self, inputs: Inputs) -> String {
        let t = |e: &ScalarExpr| e.text(inputs);
        match self {
            ScalarExpr::Input(sc) => match inputs {
                Inputs::Props => format!("$x.p_{}", input_name(*sc)),
                Inputs::Params => format!("${}", input_name(*sc)),
            },
            ScalarExpr::Int(v) => v.to_string(),
            ScalarExpr::Float(f) => (*f).to_string(),
            ScalarExpr::Bool(b) => b.to_string(),
            ScalarExpr::Str(s) => json_string(s),
            ScalarExpr::Arith(op, l, r) => format!("({} {op} {})", t(l), t(r)),
            ScalarExpr::Neg(e) => format!("(-{})", t(e)),
            ScalarExpr::If(c, a, b) => format!("if({}, {}, {})", t(c), t(a), t(b)),
            ScalarExpr::Coalesce(a, b) => format!("coalesce({}, {})", t(a), t(b)),
            ScalarExpr::Cmp(op, l, r) => format!("({} {op} {})", t(l), t(r)),
            ScalarExpr::And(l, r) => format!("({} and {})", t(l), t(r)),
            ScalarExpr::Or(l, r) => format!("({} or {})", t(l), t(r)),
            ScalarExpr::Not(e) => format!("(not {})", t(e)),
            ScalarExpr::IsNull(e, negated) => {
                format!("({} is {}null)", t(e), if *negated { "not " } else { "" })
            }
            ScalarExpr::Row(f, e) => format!("{f}({})", t(e)),
            ScalarExpr::Count(f, s, n) => format!("{f}({}, {})", t(s), t(n)),
            ScalarExpr::Substring(s, start, len) => {
                format!("substring({}, {}, {})", t(s), t(start), t(len))
            }
            ScalarExpr::Round(x, None) => format!("round({})", t(x)),
            ScalarExpr::Round(x, Some(d)) => format!("round({}, digits: {})", t(x), t(d)),
            ScalarExpr::Power(b, e) => format!("power({}, {})", t(b), t(e)),
            ScalarExpr::Cast(x, to) => format!("cast({}, {})", t(x), to.spelling()),
        }
    }

    /// The inputs the expression reads.
    pub fn inputs(&self, out: &mut Vec<Sc>) {
        match self {
            ScalarExpr::Input(sc) => {
                if !out.contains(sc) {
                    out.push(*sc);
                }
            }
            other => {
                for e in other.children() {
                    e.inputs(out);
                }
            }
        }
    }

    /// The type the RFC's rules give the expression (section 2,
    /// arithmetic): a narrower integer widens to the wider, `U32` with a
    /// signed integer is `I64`, an integer with a float is `F64`, `F32`
    /// beside `F64` is `F64`, a float literal is `F64`, an integer literal
    /// takes the other operand's type, two integer literals are `I64`; the
    /// operands of `if` and `coalesce` meet by the same rules; the result
    /// is nullable when an operand is (every input is nullable), and a
    /// `coalesce` is nullable only when both arguments are.
    pub fn rfc_type(&self) -> RfcType {
        match self {
            ScalarExpr::Input(sc) => (Some(*sc), true),
            ScalarExpr::Int(_) => (None, false),
            ScalarExpr::Float(_) => (Some(Sc::F64), false),
            ScalarExpr::Bool(_) => (Some(Sc::Bool), false),
            ScalarExpr::Str(_) => (Some(Sc::Str), false),
            // A negated literal is still a literal; any other negation of a
            // literal-typed term is an `I64` operand (`-(-0)`, `0 - 3`).
            ScalarExpr::Neg(e) => match (&**e, e.rfc_type()) {
                (ScalarExpr::Int(_), t) => t,
                (_, (None, n)) => (Some(Sc::I64), n),
                (_, t) => t,
            },
            ScalarExpr::Arith(_, l, r) => {
                let (lt, ln) = l.rfc_type();
                let (rt, rn) = r.rfc_type();
                (join(lt, rt), ln || rn)
            }
            ScalarExpr::If(_, a, b) => {
                let (at, an) = a.rfc_type();
                let (bt, bn) = b.rfc_type();
                (join(at, bt), an || bn)
            }
            ScalarExpr::Coalesce(a, b) => {
                let (at, an) = a.rfc_type();
                let (bt, bn) = b.rfc_type();
                (join(at, bt), an && bn)
            }
            ScalarExpr::Cmp(_, l, r) | ScalarExpr::And(l, r) | ScalarExpr::Or(l, r) => {
                (Some(Sc::Bool), l.rfc_type().1 || r.rfc_type().1)
            }
            ScalarExpr::Not(e) => (Some(Sc::Bool), e.rfc_type().1),
            ScalarExpr::IsNull(_, _) => (Some(Sc::Bool), false),
            // The registry rows (section 2): a String row is a String, or
            // an `I64` for `length`; `abs`, `floor`, `ceil` and `round`
            // keep their operand's type, an integer literal's being `I64`;
            // the roots, logarithms, `exp` and `power` are `F64`; a `cast`
            // is its target. Each is null where an argument is.
            ScalarExpr::Row(f, e) => {
                let (t, n) = e.rfc_type();
                let t = match *f {
                    "length" => Sc::I64,
                    "lower" | "upper" | "trim" | "ltrim" | "rtrim" => Sc::Str,
                    "sqrt" | "exp" | "ln" | "log10" => Sc::F64,
                    _ => t.unwrap_or(Sc::I64),
                };
                (Some(t), n)
            }
            ScalarExpr::Count(_, s, k) => (Some(Sc::Str), s.rfc_type().1 || k.rfc_type().1),
            ScalarExpr::Substring(s, a, b) => (
                Some(Sc::Str),
                s.rfc_type().1 || a.rfc_type().1 || b.rfc_type().1,
            ),
            ScalarExpr::Round(x, digits) => {
                let (t, n) = x.rfc_type();
                let digits_null = digits.as_ref().is_some_and(|d| d.rfc_type().1);
                (Some(t.unwrap_or(Sc::I64)), n || digits_null)
            }
            ScalarExpr::Power(b, e) => (Some(Sc::F64), b.rfc_type().1 || e.rfc_type().1),
            ScalarExpr::Cast(x, to) => (Some(*to), x.rfc_type().1),
        }
    }
}

/// The RFC's join of two operand types; `None` is an integer literal.
fn join(l: Option<Sc>, r: Option<Sc>) -> Option<Sc> {
    use Sc::*;
    match (l, r) {
        (None, None) => Some(I64),
        (None, Some(t)) | (Some(t), None) => Some(match t {
            // An integer literal beside a float is an integer with a float.
            F32 => F64,
            other => other,
        }),
        (Some(a), Some(b)) if a == b => Some(a),
        (Some(a), Some(b)) => Some(match (a, b) {
            (F32 | F64, F32 | F64) => F64,
            (F32 | F64, _) | (_, F32 | F64) => F64,
            (I32, I64) | (I64, I32) => I64,
            (U32, I32) | (I32, U32) | (U32, I64) | (I64, U32) => I64,
            (x, _) => x,
        }),
    }
}

/// The JSON value and the parameter literal of one input of the row.
pub fn scalar_input_value(c: &mut Choices, sc: Sc) -> Option<(String, Literal)> {
    if c.flag(20) {
        return None;
    }
    Some(match sc {
        Sc::I32 => {
            let v = *c.pick(&[
                0i64,
                1,
                -1,
                2,
                -7,
                100,
                i64::from(i32::MAX),
                i64::from(i32::MIN),
            ]);
            (v.to_string(), Literal::Integer(i128::from(v)))
        }
        Sc::I64 => {
            let v = *c.pick(&[0i64, 1, -1, 3, -8, 1000, i64::MAX, i64::MIN]);
            (v.to_string(), Literal::Integer(i128::from(v)))
        }
        Sc::U32 => {
            let v = *c.pick(&[0i64, 1, 2, 9, 1000, i64::from(u32::MAX)]);
            (v.to_string(), Literal::Integer(i128::from(v)))
        }
        Sc::F32 | Sc::F64 => {
            let v = *c.pick(&["0.0", "-0.0", "0.5", "-1.5", "2.25", "1000.0", "1e30"]);
            (v.to_string(), Literal::Float(v.parse().unwrap()))
        }
        Sc::Bool => {
            let v = c.below(2) == 1;
            (v.to_string(), Literal::Bool(v))
        }
        Sc::Str => {
            let v = match c.flag(25) {
                true => *c.pick(&SCALAR_STR_TEXTS),
                false => *c.pick(&SCALAR_STRS),
            };
            (json_string(v), Literal::String(v.to_string()))
        }
        Sc::Date | Sc::DateTime => unreachable!("no temporal input"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::build_catalog;
    use crate::query::check::check_source;
    use crate::schema::parser::parse_schema;
    use proptest::test_runner::{Config, TestRunner};

    /// Generator soundness: every generated schema builds, and every
    /// generated read type-checks against it. A refusal is a generator bug
    /// or a checker defect, never a sample to skip.
    fn generated_reads_type_check(cases: u32) {
        let mut runner = TestRunner::new_with_rng(
            Config {
                cases,
                failure_persistence: None,
                ..Config::default()
            },
            proptest::test_runner::TestRng::deterministic_rng(
                proptest::test_runner::RngAlgorithm::ChaCha,
            ),
        );
        let refusals: std::cell::RefCell<std::collections::BTreeMap<String, (usize, String)>> =
            Default::default();
        let total = std::cell::Cell::new(0usize);
        let drawn: std::cell::RefCell<std::collections::BTreeMap<&str, usize>> = Default::default();
        runner
            .run(&(choices(64), choices(512)), |(s, q)| {
                let schema = GenSchema::generate(&mut Choices::new(&s), SchemaOptions::default());
                let catalog = build_catalog(&parse_schema(&schema.text()).unwrap()).unwrap();
                let read =
                    GenRead::generate(&mut Choices::new(&q), &schema, ReadOptions::default());
                total.set(total.get() + 1);
                for spelling in DRAWN {
                    if read.source.contains(spelling) {
                        *drawn.borrow_mut().entry(spelling).or_default() += 1;
                    }
                }
                let outcome = check_source(&catalog, &read.source)
                    .and_then(|mut decls| decls.pop().unwrap().1.map(|_| ()));
                if let Err(d) = outcome {
                    let mut refusals = refusals.borrow_mut();
                    let entry = refusals
                        .entry(d.code.as_str().to_string())
                        .or_insert((0, String::new()));
                    entry.0 += 1;
                    if entry.1.is_empty() {
                        entry.1 = format!("{}\n{}\n{}", d.message, schema.text(), read.source);
                    }
                }
                Ok(())
            })
            .unwrap();
        let refusals = refusals.into_inner();
        for (code, (n, example)) in &refusals {
            eprintln!("{code}: {n}/{}\n{example}\n", total.get());
        }
        assert!(refusals.is_empty(), "{} codes refused", refusals.len());
        let drawn = drawn.into_inner();
        eprintln!("reads drawing each row of {}: {drawn:?}", total.get());
        let missing: Vec<&&str> = DRAWN.iter().filter(|s| !drawn.contains_key(*s)).collect();
        assert!(missing.is_empty(), "no generated read draws {missing:?}");
    }

    /// The registry rows and order placements the generator must reach.
    const DRAWN: [&str; 26] = [
        "lower(",
        "upper(",
        "trim(",
        "ltrim(",
        "rtrim(",
        "length(",
        "left(",
        "right(",
        "substring(",
        "abs(",
        "floor(",
        "ceil(",
        "round(",
        "digits: ",
        "sqrt(",
        "power(",
        "exp(",
        "ln(",
        "log10(",
        "size(",
        "cast(",
        "stddev(",
        "percentile(",
        " nulls first",
        " nulls last",
        "\u{a0}",
    ];

    #[test]
    fn generated_reads_type_check_at_the_gate() {
        generated_reads_type_check(2_000);
    }

    #[test]
    #[ignore = "nightly: 100,000 generated reads; the gate runs 2,000"]
    fn generated_reads_type_check_at_nightly_breadth() {
        generated_reads_type_check(100_000);
    }
}
