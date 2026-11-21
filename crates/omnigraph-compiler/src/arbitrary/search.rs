//! Search cases for the search law (`docs/dev/test-ownership.md`, "Generated
//! laws", law 6): one node type declaring every index kind a read can take
//! (a BTREE on an `I64`, a BITMAP on an enum, a LABEL_LIST on a list, a
//! BTREE and an NGRAM on a String, a full-text index under a drawn analyzer
//! profile, a vector index under a drawn distance), its rows in three
//! batches (the rows loaded first, rows added later and rows later replaced
//! whole), and reads over it: filters on each indexed column, `contains`
//! over the column and over its `lower`, `match_terms` with every mode and
//! edit budget, and the rankings `distance`, `nearest`, `bm25` and `rrf` of
//! two to four arms with or without `weights`.
//!
//! Terms are drawn from letters every analyzer profile keeps as one token's
//! letters (alphanumeric, already in NFC), and query terms are often a
//! document term with a few random edits, so edit-tolerant matching meets
//! the boundary of its budget; [`term`] and [`edited`] are the edit
//! generators the edit-distance laws share.

use std::fmt::Write as _;

use super::Choices;
use crate::query::value::Literal;

/// The analyzer profiles a full-text field draws.
pub const PROFILES: [&str; 3] = ["standard_v1", "standard_folded_v1", "english_v1"];
/// The distances a vector field draws.
pub const DISTANCES: [&str; 3] = ["l2", "cosine", "dot"];
const MEMBERS: [&str; 3] = ["red", "green", "blue"];
const TAGS: [&str; 4] = ["rust", "db", "misc", "x"];
/// The letters of a term: lower-case ASCII, one upper-case letter, a
/// precomposed accented letter, a sharp s, a CJK ideograph and a digit.
/// Each is alphanumeric and already NFC, so every profile splits a text of
/// them only at its separators.
pub const TERM_LETTERS: [char; 8] = ['a', 'b', 'c', 'A', 'é', 'ß', '中', '1'];
/// The letters of a `name`: case pairs whose lower case is longer than
/// their upper case (`İ`) or not ASCII (`É`), beside ASCII and a space.
const NAME_LETTERS: [char; 9] = ['a', 'b', 'c', 'A', 'B', 'É', 'é', 'İ', ' '];
const SEPARATORS: [&str; 3] = [" ", "-", ", "];
const COMPONENTS: [f64; 5] = [-1.0, 0.0, 0.5, 1.0, 2.0];

/// A term of one to five letters of [`TERM_LETTERS`].
pub fn term(c: &mut Choices) -> String {
    term_of(c, &TERM_LETTERS)
}

fn term_of(c: &mut Choices, letters: &[char]) -> String {
    let n = 1 + c.below(5);
    (0..n).map(|_| *c.pick(letters)).collect()
}

/// The letters a profile's texts draw: a folding profile's reference
/// interpreter folds the Latin letters alone, since the RFC fixes no
/// folding table beyond them, so its texts leave out the ideograph.
fn letters_of(profile: &str) -> &'static [char] {
    if profile == "standard_v1" {
        &TERM_LETTERS
    } else {
        &TERM_LETTERS[..6]
    }
}

/// `base` after `edits` random single-letter edits (an insertion, a
/// deletion, a substitution, or a transposition of two neighbors, which is
/// two edits under the RFC's relation), so its distance from `base` is at
/// most `edits` plus the transpositions drawn.
pub fn edited(c: &mut Choices, base: &str, edits: usize) -> String {
    edited_of(c, base, edits, &TERM_LETTERS)
}

fn edited_of(c: &mut Choices, base: &str, edits: usize, alphabet: &[char]) -> String {
    let mut letters: Vec<char> = base.chars().collect();
    for _ in 0..edits {
        let at = c.below(letters.len() + 1);
        match c.below(4) {
            0 => letters.insert(at, *c.pick(alphabet)),
            1 if at < letters.len() => {
                letters.remove(at);
            }
            2 if at < letters.len() => letters[at] = *c.pick(alphabet),
            3 if at + 1 < letters.len() => letters.swap(at, at + 1),
            _ => letters.insert(at, *c.pick(alphabet)),
        }
    }
    letters.into_iter().collect()
}

/// One row of the case's node type.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchRow {
    pub key: String,
    pub n: Option<i64>,
    pub s: Option<&'static str>,
    pub tags: Option<Vec<&'static str>>,
    pub name: Option<String>,
    pub t: Option<String>,
    pub v: Option<[f64; 4]>,
}

impl SearchRow {
    fn generate(c: &mut Choices, key: String, distance: &str, letters: &[char]) -> Self {
        let n = c.flag(80).then(|| c.below(7) as i64 - 3);
        let s = c.flag(80).then(|| *c.pick(&MEMBERS));
        let tags = c.flag(80).then(|| {
            let mut tags: Vec<&'static str> = (0..c.below(3)).map(|_| *c.pick(&TAGS)).collect();
            tags.sort();
            tags.dedup();
            tags
        });
        let name = c.flag(80).then(|| {
            let n = c.below(9);
            (0..n).map(|_| *c.pick(&NAME_LETTERS)).collect()
        });
        let t = c.flag(80).then(|| {
            let n = c.below(5);
            let mut text = String::new();
            for i in 0..n {
                if i > 0 {
                    text.push_str(c.pick(&SEPARATORS));
                }
                text.push_str(&term_of(c, letters));
            }
            text
        });
        let v = c.flag(80).then(|| vector(c, distance));
        Self {
            key,
            n,
            s,
            tags,
            name,
            t,
            v,
        }
    }

    /// The row as a load line.
    pub fn jsonl(&self) -> String {
        let mut data = format!("\"k\":{}", json(&self.key));
        if let Some(n) = self.n {
            let _ = write!(data, ",\"n\":{n}");
        }
        if let Some(s) = self.s {
            let _ = write!(data, ",\"s\":{}", json(s));
        }
        if let Some(tags) = &self.tags {
            let tags: Vec<String> = tags.iter().map(|t| json(t)).collect();
            let _ = write!(data, ",\"tags\":[{}]", tags.join(","));
        }
        if let Some(name) = &self.name {
            let _ = write!(data, ",\"name\":{}", json(name));
        }
        if let Some(t) = &self.t {
            let _ = write!(data, ",\"t\":{}", json(t));
        }
        if let Some(v) = self.v {
            let _ = write!(data, ",\"v\":[{:?},{:?},{:?},{:?}]", v[0], v[1], v[2], v[3]);
        }
        format!("{{\"type\":\"D\",\"data\":{{{data}}}}}")
    }
}

fn json(s: &str) -> String {
    serde_json::to_string(s).expect("a string serializes")
}

/// A `match_terms` predicate the law checks against its edit oracle.
#[derive(Debug, Clone, PartialEq)]
pub struct Lexical {
    pub text: String,
    pub all: bool,
    pub max_edits: usize,
}

/// One `rrf` arm: its call over `$pop`.
#[derive(Debug, Clone, PartialEq)]
pub enum Arm {
    Distance { param: String },
    Nearest { param: String, k: usize },
    Bm25 { text: String },
}

impl Arm {
    pub fn call(&self) -> String {
        match self {
            Arm::Distance { param } => format!("distance($pop, $x.v, ${param})"),
            Arm::Nearest { param, k } => format!("nearest($pop, $x.v, ${param}, k: {k})"),
            Arm::Bm25 { text } => format!("bm25($pop, $x.t, {})", json(text)),
        }
    }

    /// Whether a larger score ranks first.
    pub fn descending(&self) -> bool {
        matches!(self, Arm::Bm25 { .. })
    }
}

/// An `rrf` read's parts, so a law can write it with its arms permuted.
#[derive(Debug, Clone, PartialEq)]
pub struct Rrf {
    pub arms: Vec<Arm>,
    pub k: usize,
    pub weights: Option<Vec<usize>>,
}

/// One read of the case.
#[derive(Debug, Clone)]
pub struct SearchRead {
    /// The read with its parameters declared, named `q`.
    pub source: String,
    pub params: Vec<(String, Literal)>,
    /// Whether the rows are ordered (an `order` with `limit`).
    pub ordered: bool,
    /// Set when the whole `where` is one `match_terms`.
    pub lexical: Option<Lexical>,
    /// Set for an `rrf` read: the parts its source is written from.
    pub rrf: Option<Rrf>,
    /// Set for a read of one ranking call: the call, and the `limit` of its
    /// `order` when it has one.
    pub ranking: Option<(Arm, Option<usize>)>,
    /// The population's `where`, written inside its `let`.
    pub population: String,
}

impl SearchRead {
    /// The `rrf` read with its arms, and their weights, in `order`.
    pub fn rrf_source(&self, order: &[usize]) -> Option<String> {
        let rrf = self.rrf.as_ref()?;
        let arms: Vec<Arm> = order.iter().map(|i| rrf.arms[*i].clone()).collect();
        let weights = rrf
            .weights
            .as_ref()
            .map(|w| order.iter().map(|i| w[*i]).collect::<Vec<_>>());
        Some(ranking_source(
            &self.population,
            &rrf_call(&arms, rrf.k, weights.as_deref()),
            &self.params,
            "",
        ))
    }

    /// One arm of the `rrf` read alone, its score named `$s`.
    pub fn arm_source(&self, arm: usize) -> Option<String> {
        let rrf = self.rrf.as_ref()?;
        Some(ranking_source(
            &self.population,
            &rrf.arms[arm].call(),
            &self.params,
            "",
        ))
    }
}

fn rrf_call(arms: &[Arm], k: usize, weights: Option<&[usize]>) -> String {
    let arms: Vec<String> = arms.iter().map(Arm::call).collect();
    match weights {
        Some(w) => {
            let w: Vec<String> = w.iter().map(|w| format!("{w}.0")).collect();
            format!(
                "rrf({}, k: {k}, weights: [{}])",
                arms.join(", "),
                w.join(", ")
            )
        }
        None => format!("rrf({}, k: {k})", arms.join(", ")),
    }
}

fn declarations(params: &[(String, Literal)]) -> String {
    params
        .iter()
        .map(|(name, _)| format!("${name}: Vector(4)"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn ranking_source(
    population: &str,
    call: &str,
    params: &[(String, Literal)],
    tail: &str,
) -> String {
    format!(
        "query q({}) {{\n  match {{\n    let $pop = {{ match {{ $x: D }}{population} return {{ $x }} }},\n    from {call} as $s\n  }}\n  return {{ $x.k, $s }}{tail}\n}}\n",
        declarations(params)
    )
}

/// A generated search case.
#[derive(Debug, Clone)]
pub struct SearchCase {
    pub profile: &'static str,
    pub distance: &'static str,
    /// The rows loaded first.
    pub base: Vec<SearchRow>,
    /// Rows of new keys loaded after the base.
    pub tail: Vec<SearchRow>,
    /// Base keys written again whole, after the tail.
    pub updates: Vec<SearchRow>,
    pub reads: Vec<SearchRead>,
}

impl SearchCase {
    pub fn generate(c: &mut Choices, reads: usize) -> Self {
        let profile = *c.pick(&PROFILES);
        let distance = *c.pick(&DISTANCES);
        let n_base = 1 + c.below(12);
        let base: Vec<SearchRow> = (0..n_base)
            .map(|i| SearchRow::generate(c, format!("d{i:02}"), distance, letters_of(profile)))
            .collect();
        let n_tail = c.below(7);
        let tail: Vec<SearchRow> = (0..n_tail)
            .map(|i| {
                SearchRow::generate(
                    c,
                    format!("d{:02}", n_base + i),
                    distance,
                    letters_of(profile),
                )
            })
            .collect();
        let mut updates = Vec::new();
        for row in &base {
            if c.flag(25) {
                updates.push(SearchRow::generate(
                    c,
                    row.key.clone(),
                    distance,
                    letters_of(profile),
                ));
            }
        }
        let mut case = Self {
            profile,
            distance,
            base,
            tail,
            updates,
            reads: Vec::new(),
        };
        for _ in 0..reads {
            let read = case.read(c);
            case.reads.push(read);
        }
        case
    }

    /// The schema as `.pg` text.
    pub fn schema(&self) -> String {
        format!(
            "node D {{\n    k: String @key\n    n: I64? @index\n    s: enum({})? @index\n    tags: [String]? @index\n    name: String? @index\n    t: String? @index(fulltext, analyzer = \"{}\")\n    v: Vector(4, distance = \"{}\")? @index\n}}\n",
            MEMBERS.join(", "),
            self.profile,
            self.distance
        )
    }

    /// The rows every state holds once its loads are done: the base with
    /// its updates applied, then the tail.
    pub fn rows(&self) -> Vec<SearchRow> {
        let mut rows: Vec<SearchRow> = self
            .base
            .iter()
            .map(|row| {
                self.updates
                    .iter()
                    .rev()
                    .find(|u| u.key == row.key)
                    .unwrap_or(row)
                    .clone()
            })
            .collect();
        rows.extend(self.tail.iter().cloned());
        rows
    }

    /// The terms of every stored text, for query terms near them.
    fn stored_terms(&self) -> Vec<String> {
        self.base
            .iter()
            .chain(&self.tail)
            .chain(&self.updates)
            .filter_map(|row| row.t.as_deref())
            .flat_map(|t| {
                t.split(|ch: char| !ch.is_alphanumeric())
                    .filter(|w| !w.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// A lexical query text of one to three terms, each a stored term
    /// with zero to three edits or a fresh term.
    fn text(&self, c: &mut Choices) -> String {
        let stored = self.stored_terms();
        let n = 1 + c.below(3);
        let terms: Vec<String> = (0..n)
            .map(|_| {
                let letters = letters_of(self.profile);
                if stored.is_empty() || c.flag(20) {
                    term_of(c, letters)
                } else {
                    let base = stored[c.below(stored.len())].clone();
                    let edits = c.below(4);
                    let near = edited_of(c, &base, edits, letters);
                    if near.is_empty() { base } else { near }
                }
            })
            .collect();
        terms.join(" ")
    }

    /// A `contains` needle of one to five letters: most often a piece of a
    /// stored name, so the NGRAM probe meets rows it keeps and its recheck.
    fn needle(&self, c: &mut Choices) -> String {
        let names: Vec<Vec<char>> = self
            .base
            .iter()
            .chain(&self.tail)
            .chain(&self.updates)
            .filter_map(|row| row.name.as_ref())
            .map(|name| name.chars().collect())
            .filter(|name: &Vec<char>| !name.is_empty())
            .collect();
        if names.is_empty() || c.flag(25) {
            return (0..1 + c.below(5))
                .map(|_| *c.pick(&NAME_LETTERS[..8]))
                .collect();
        }
        // Three letters or more, which the NGRAM index answers, when the
        // name holds them.
        let name = &names[c.below(names.len())];
        let len = (1 + c.below(5)).max(3).min(name.len());
        let start = c.below(name.len() - len + 1);
        name[start..start + len].iter().collect()
    }

    fn atom(&self, c: &mut Choices) -> (String, Option<Lexical>) {
        let int = |c: &mut Choices| c.below(7) as i64 - 3;
        let atom = match c.below(16) {
            0 => format!("$x.n = {}", int(c)),
            1 => format!("$x.n < {}", int(c)),
            2 => format!("$x.n >= {}", int(c)),
            3 => format!("$x.n in [{}, {}]", int(c), int(c)),
            4 => format!("$x.s = {}", json(c.pick(&MEMBERS))),
            5 => format!(
                "$x.s in [{}, {}]",
                json(c.pick(&MEMBERS)),
                json(c.pick(&MEMBERS))
            ),
            6 => format!("$x.s != {}", json(c.pick(&MEMBERS))),
            7 => format!("{} in $x.tags", json(c.pick(&TAGS))),
            8..=10 => format!("contains($x.name, {})", json(&self.needle(c))),
            11 | 12 => format!(
                "contains(lower($x.name), {})",
                json(&self.needle(c).to_lowercase())
            ),
            13 => {
                let name: String = (0..1 + c.below(3))
                    .map(|_| *c.pick(&NAME_LETTERS))
                    .collect();
                format!("$x.name = {}", json(&name))
            }
            _ => return self.lexical(c),
        };
        (atom, None)
    }

    /// A `match_terms` in either mode with an edit budget of zero to two.
    fn lexical(&self, c: &mut Choices) -> (String, Option<Lexical>) {
        let lexical = Lexical {
            text: self.text(c),
            all: c.flag(50),
            max_edits: c.below(3),
        };
        let mode = if lexical.all { "all" } else { "any" };
        let atom = format!(
            "match_terms($x.t, terms({}, mode: {mode}, max_edits: {}))",
            json(&lexical.text),
            lexical.max_edits
        );
        (atom, Some(lexical))
    }

    /// A predicate of one or two atoms under `and`, `or` and `not`; its
    /// lexical part when it is one `match_terms` alone.
    fn predicate(&self, c: &mut Choices) -> (String, Option<Lexical>) {
        let (first, lexical) = self.atom(c);
        match c.below(5) {
            0 => {
                let (second, _) = self.atom(c);
                (format!("({first} and {second})"), None)
            }
            1 => {
                let (second, _) = self.atom(c);
                (format!("({first} or {second})"), None)
            }
            2 => (format!("not ({first})"), None),
            _ => (first, lexical),
        }
    }

    fn vector_param(&self, c: &mut Choices, params: &mut Vec<(String, Literal)>) -> String {
        let name = format!("q{}", params.len());
        let v = vector(c, self.distance);
        params.push((
            name.clone(),
            Literal::List(v.into_iter().map(Literal::Float).collect()),
        ));
        name
    }

    fn arm(&self, c: &mut Choices, params: &mut Vec<(String, Literal)>) -> Arm {
        match c.below(3) {
            0 => Arm::Distance {
                param: self.vector_param(c, params),
            },
            1 => Arm::Nearest {
                param: self.vector_param(c, params),
                k: 1 + c.below(5),
            },
            _ => Arm::Bm25 { text: self.text(c) },
        }
    }

    fn read(&self, c: &mut Choices) -> SearchRead {
        let population = if c.flag(40) {
            format!(" where {}", self.predicate(c).0)
        } else {
            String::new()
        };
        let mut params = Vec::new();
        match c.below(7) {
            0..=2 => {
                let (pred, lexical) = if c.flag(40) {
                    self.lexical(c)
                } else {
                    self.predicate(c)
                };
                SearchRead {
                    source: format!(
                        "query q() {{\n  match {{ $x: D }}\n  where {pred}\n  return {{ $x.k }}\n}}\n"
                    ),
                    params,
                    ordered: false,
                    lexical,
                    rrf: None,
                    ranking: None,
                    population: String::new(),
                }
            }
            3..=5 => {
                let arm = self.arm(c, &mut params);
                let limit = c.flag(40).then(|| 1 + c.below(5));
                let tail = match limit {
                    Some(limit) => {
                        let dir = if arm.descending() { "desc" } else { "asc" };
                        format!("\n  order {{ $s {dir} }}\n  limit {limit}")
                    }
                    None => String::new(),
                };
                SearchRead {
                    source: ranking_source(&population, &arm.call(), &params, &tail),
                    ordered: limit.is_some(),
                    params,
                    lexical: None,
                    rrf: None,
                    ranking: Some((arm, limit)),
                    population,
                }
            }
            _ => {
                let n = 2 + c.below(3);
                let mut arms: Vec<Arm> = Vec::new();
                while arms.len() < n {
                    let arm = self.arm(c, &mut params);
                    // Two arms over one field and query are a type error.
                    // A distance or nearest arm reads a fresh parameter,
                    // so the loop ends however the stream runs out.
                    if !arms.contains(&arm) {
                        arms.push(arm);
                    }
                }
                let k = *c.pick(&[1usize, 2, 60]);
                let weights = c
                    .flag(50)
                    .then(|| (0..arms.len()).map(|_| 1 + c.below(3)).collect::<Vec<_>>());
                let rrf = Rrf { arms, k, weights };
                let call = rrf_call(&rrf.arms, rrf.k, rrf.weights.as_deref());
                SearchRead {
                    source: ranking_source(&population, &call, &params, ""),
                    params,
                    ordered: false,
                    lexical: None,
                    rrf: Some(rrf),
                    ranking: None,
                    population,
                }
            }
        }
    }
}

/// A vector under `distance`. A `cosine` vector has a power-of-two norm (an
/// axis vector or a vector of equal magnitudes, scaled), so every cosine
/// distance between two of them is a dyadic fraction, exact in `F32` on
/// every path; the RFC bounds an indexed cosine distance only up to `F32`
/// rounding, which an exact comparison cannot hold. A zero vector, which has
/// no cosine distance, is never drawn for `cosine`.
fn vector(c: &mut Choices, distance: &str) -> [f64; 4] {
    if distance != "cosine" {
        return [
            *c.pick(&COMPONENTS),
            *c.pick(&COMPONENTS),
            *c.pick(&COMPONENTS),
            *c.pick(&COMPONENTS),
        ];
    }
    let sign = |c: &mut Choices| if c.flag(50) { -1.0 } else { 1.0 };
    let mut v = [0.0; 4];
    if c.flag(50) {
        let scale = *c.pick(&[1.0, 2.0, 4.0]);
        v[c.below(4)] = scale * sign(c);
    } else {
        let scale = *c.pick(&[1.0, 2.0]);
        for x in &mut v {
            *x = scale * sign(c);
        }
    }
    v
}
