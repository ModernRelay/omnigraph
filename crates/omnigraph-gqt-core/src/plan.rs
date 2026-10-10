//! The `--- expect plan` section: assertions over the plan a query step runs
//! under, checked against the engine's explain document (RFC 0068) without
//! executing the query. Each line names one fact of one node; nothing is
//! compared as rendered text.
//!
//! ```text
//! scan Doc as $d: columns [__id, slug]
//! scan Doc: not columns [embedding]
//! scan Doc as $d: filter reads [d.state]
//! scan Doc as $b: no filter
//! scan Doc as $d: access id_lookup
//! hash join $d
//! hash join $d ran id_lookup
//! scan Doc as $d: ranked bm25
//! scan Doc as $d: ranked nearest fetch 10
//! scan Doc as $d: ranked nearest fetch 10 nprobes 20
//! expand $d Knows $e: mode indexed_scan
//! expand $d Knows $e: mode indexed_scan ran csr
//! contains join $p.text contains $m.number
//! cross join $p.text contains $m.number
//! scan Passage as $p: runtime filter text
//! scan Passage as $p: no runtime filter
//! filter reads [a.state, b.state]
//! hydrate $d: columns [body, title]
//! pass projection_pushdown
//! not pass aggregate_pushdown
//! ```
//!
//! A `ran` claim names the side of the node's declared switch that ran,
//! read from the execution report of the same run (the last attempt of the
//! node's row), joined to the explain row by the node's `id`.

use omnigraph_compiler::catalog::Catalog;
use omnigraph_planner::optimizer::{
    PASS_ACCESS_PATH, PASS_ADDRESS_SHORT_CIRCUIT, PASS_AGGREGATE_PUSHDOWN, PASS_EXPAND_MODE,
    PASS_FRAGMENT_SCOPE, PASS_JOIN_ALGORITHM, PASS_LATE_MATERIALIZATION, PASS_PREDICATE_PUSHDOWN,
    PASS_PROJECTION_PUSHDOWN, PASS_RESOLVE,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::report::Row;

const FORMS: &str = "forms: `scan <Type>[ as $var]: columns [a, b]`, `scan <Type>[ as $var]: not columns [a, b]`, `scan <Type>[ as $var]: filter reads [v.a]`, `scan <Type>[ as $var]: no filter`, `scan <Type>[ as $var]: access id_lookup`, `scan <Type>[ as $var]: access sequential`, `scan <Type>[ as $var]: access index_probe <column>`, `hash join $var[ ran <hash_join|id_lookup>]`, `scan <Type>[ as $var]: ranked <nearest|bm25>[ fetch <n>][ nprobes <n>]`, `scan <Type>[ as $var]: runtime filter <column>`, `scan <Type>[ as $var]: no runtime filter`, `contains join $h.x contains $n.y`, `cross join $h.x contains $n.y`, `expand $src <Edge> $dst: mode <csr|indexed_scan>[ ran <csr|indexed_scan>]`, `filter reads [a.x, b.y]`, `sort tiebreak [$a.@id, $e.@type]`, `rank fuse row tiebreak [$e.@type, $e.@id]`, `expand $a $b: selection alternation [Knows out, Likes in]`, `sort no tiebreak`, `aggregate <column>: <func>(<Type>) <accumulator> <overflow> -> <Type>`, `block aggregate <gq>: <func>(<Type>) <accumulator> <overflow> -> <Type>`, `result columns [<name>: <Type>, ...]`, `type <gq>: <Type>`, `cast <gq>: <Type> -> <Type>`, `no cast <gq>`, `hydrate $var: columns [a, b]`, `pass <name>`, `not pass <name>`";

const ID_LOOKUP: &str = "id_lookup";
const JOIN_SIDES: [&str; 2] = ["hash_join", "id_lookup"];
const EXPAND_MODES: [&str; 2] = ["csr", "indexed_scan"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AggregateClaim {
    column: String,
    func: String,
    input: String,
    accumulator: omnigraph_planner::Accumulator,
    overflow: omnigraph_planner::Overflow,
    result: String,
}

/// What one `scan` line claims of the selected scans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ScanClaim {
    /// The scans project exactly `columns`, or none of them when `negated`.
    Columns { columns: Vec<String>, negated: bool },
    /// The scans carry a pushed filter reading exactly `reads`.
    FilterReads { reads: Vec<String> },
    /// The scans carry no pushed filter.
    NoFilter,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum PlanLine {
    Type {
        gq: String,
        ty: String,
    },
    Cast {
        gq: String,
        from: String,
        to: String,
    },
    NoCast {
        gq: String,
    },
    ResultColumns {
        columns: Vec<String>,
    },
    Aggregate {
        claim: AggregateClaim,
    },
    BlockAggregate {
        claim: AggregateClaim,
    },
    /// The scans of `type_name` (all of them, or the one bound to `binding`)
    /// satisfy `claim`.
    Scan {
        type_name: String,
        binding: Option<String>,
        claim: ScanClaim,
    },
    /// A physical scan is a traversal's destination reached by the per-slice
    /// id lookup (`id_restriction` `input`).
    ScanAccess {
        type_name: String,
        binding: Option<String>,
    },
    ScanIndex {
        type_name: String,
        binding: Option<String>,
        column: Option<String>,
    },
    /// A physical `HashJoin` reaches `$binding`'s rows through a build of
    /// its table, and when `ran` is claimed, the run took that side of the
    /// join's declared switch.
    HashJoin {
        binding: String,
        ran: Option<String>,
    },
    /// Every selected physical scan is ranked, and one of them by the named
    /// index, asking it for `fetch` candidates and under the probe cap
    /// `nprobes` (`0` no cap) when claimed; an `rrf()` has one scan per arm.
    ScanRanked {
        type_name: String,
        binding: Option<String>,
        index: String,
        fetch: Option<u64>,
        nprobes: Option<u64>,
    },
    /// Every selected physical scan is marked with a runtime filter on
    /// `column`, or with none when `column` is `None`.
    ScanRuntimeFilter {
        type_name: String,
        binding: Option<String>,
        column: Option<String>,
    },
    /// A physical `ContainsJoin` pairs `$haystack contains $needle`, each a
    /// `binding.property`.
    ContainsJoin {
        haystack: String,
        needle: String,
    },
    /// A physical `CrossJoin` holds the conjunct `$haystack contains $needle`
    /// among its `filters`: the plain filtered product, no `ContainsJoin`.
    CrossJoin {
        haystack: String,
        needle: String,
    },
    /// The physical `Expand` from `$src` over `edge_type` to `$dst` runs in
    /// `mode` (`csr` or `indexed_scan`), and when `ran` is claimed, the run
    /// ended on that mode.
    ExpandMode {
        src: String,
        edge_type: Option<String>,
        dst: String,
        mode: String,
        ran: Option<String>,
    },
    /// Exact resolved selection, including member order and direction.
    EdgeSelection {
        src: String,
        dst: String,
        selection_kind: String,
        members: Vec<(String, String)>,
    },
    /// Exact downstream identity keys declared by a physical RankFuse.
    RankFuse {
        tiebreak: Vec<String>,
    },
    /// An in-memory `Filter` node stays in the plan reading exactly `reads`.
    Filter {
        reads: Vec<String>,
    },
    /// A physical `Sort` declares exactly the ordered identity keys after its
    /// keys, none when empty.
    Sort {
        tiebreak: Vec<String>,
    },
    /// A physical `HydrateColumns` fetches exactly `columns` of `$binding` by
    /// row address for the rows that reached the output.
    Hydrate {
        binding: String,
        columns: Vec<String>,
    },
    /// The optimizer pass `name` fired, or did not when `negated`.
    Pass {
        name: String,
        negated: bool,
    },
}

#[derive(Debug, Default)]
pub(crate) struct PlanExpect {
    pub lines: Vec<PlanLine>,
}

pub(crate) fn parse_plan_body(body: &[(usize, &str)]) -> Result<Vec<PlanLine>, String> {
    let mut lines = Vec::new();
    for (index, raw) in body {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let refused = |what: &str| {
            format!(
                "line {}: `--- expect plan` {what}: `{line}` ({FORMS})",
                index + 1
            )
        };
        if let Some(name) = line
            .strip_prefix("not pass ")
            .map(|name| (name, true))
            .or_else(|| line.strip_prefix("pass ").map(|name| (name, false)))
        {
            let (name, negated) = name;
            let name = name.trim();
            if ![
                PASS_RESOLVE,
                PASS_PREDICATE_PUSHDOWN,
                PASS_AGGREGATE_PUSHDOWN,
                PASS_PROJECTION_PUSHDOWN,
                PASS_FRAGMENT_SCOPE,
                PASS_ADDRESS_SHORT_CIRCUIT,
                PASS_LATE_MATERIALIZATION,
                PASS_JOIN_ALGORITHM,
                PASS_EXPAND_MODE,
                PASS_ACCESS_PATH,
                omnigraph_planner::scan_access::PASS_SCAN_ACCESS,
                omnigraph_planner::scan_access::PASS_KEY_TO_ID,
            ]
            .contains(&name)
            {
                return Err(refused("names a known optimizer pass"));
            }
            lines.push(PlanLine::Pass {
                name: name.to_string(),
                negated,
            });
            continue;
        }
        if let Some(gq) = line.strip_prefix("no cast ") {
            let gq = gq.trim();
            if gq.is_empty() {
                return Err(refused("names an expression after `no cast`"));
            }
            lines.push(PlanLine::NoCast { gq: gq.to_string() });
            continue;
        }
        if let Some((claim, cast)) = line
            .strip_prefix("type ")
            .map(|s| (s, false))
            .or_else(|| line.strip_prefix("cast ").map(|s| (s, true)))
        {
            let (gq, types) = claim
                .rsplit_once(':')
                .ok_or_else(|| refused("separates expression and type with `:`"))?;
            let gq = gq.trim();
            if gq.is_empty() {
                return Err(refused("names a nonempty expression"));
            }
            if cast {
                let (from, to) = types
                    .split_once("->")
                    .ok_or_else(|| refused("separates cast types with `->`"))?;
                let from = expression_type(from.trim())
                    .ok_or_else(|| refused("names a valid source type"))?;
                let to = expression_type(to.trim())
                    .ok_or_else(|| refused("names a valid target type"))?;
                lines.push(PlanLine::Cast {
                    gq: gq.to_string(),
                    from,
                    to,
                });
            } else {
                let ty = expression_type(types.trim())
                    .ok_or_else(|| refused("names a valid expression type"))?;
                lines.push(PlanLine::Type {
                    gq: gq.to_string(),
                    ty,
                });
            }
            continue;
        }
        if let Some(list) = line.strip_prefix("filter reads") {
            let reads = column_list(list).ok_or_else(|| refused("lists reads in `[...]`"))?;
            lines.push(PlanLine::Filter { reads });
            continue;
        }
        if let Some(list) = line.strip_prefix("result columns ") {
            let columns = bracket_list(list, |item| {
                let lines = crate::shape::parse_shape_body(&[(0, item)]).ok()?;
                let [line] = lines.as_slice() else {
                    return None;
                };
                Some(crate::shape::spell_shape_line(line))
            })
            .ok_or_else(|| refused("lists result columns as `[name: Type, ...]`"))?;
            lines.push(PlanLine::ResultColumns { columns });
            continue;
        }
        if let Some((claim, block)) = line
            .strip_prefix("aggregate ")
            .map(|claim| (claim, false))
            .or_else(|| {
                line.strip_prefix("block aggregate ")
                    .map(|claim| (claim, true))
            })
        {
            let (column, call) = claim
                .split_once(':')
                .ok_or_else(|| refused("separates the aggregate column with `:`"))?;
            let (func, argument) = call
                .trim()
                .split_once('(')
                .ok_or_else(|| refused("spells an aggregate as `func(Type)`"))?;
            let (argument, result) = argument
                .split_once("->")
                .ok_or_else(|| refused("separates the result type with `->`"))?;
            let (input, arithmetic) = argument
                .rsplit_once(')')
                .ok_or_else(|| refused("closes the aggregate argument with `)`"))?;
            let words: Vec<_> = arithmetic.split_whitespace().collect();
            let [accumulator, overflow] = words.as_slice() else {
                return Err(refused("names an accumulator and an overflow rule"));
            };
            let column = column.trim();
            let valid_column = if block {
                !column.is_empty()
            } else {
                crate::shape::parse_shape_body(&[(0, &format!("{column}: I64"))]).is_ok()
            };
            if !valid_column || !["count", "sum", "avg", "min", "max"].contains(&func) {
                return Err(refused(
                    "names a result column and a known aggregate function",
                ));
            }
            let input = aggregate_type(input.trim())
                .ok_or_else(|| refused("names a declared input type"))?;
            let result = aggregate_type(result.trim())
                .ok_or_else(|| refused("names a declared result type"))?;
            let accumulator = serde_json::from_value(Value::String((*accumulator).to_string()))
                .map_err(|_| refused("names count, exact_integer, float64 or extremum"))?;
            let overflow = serde_json::from_value(Value::String((*overflow).to_string()))
                .map_err(|_| refused("names round_to_nearest or error"))?;
            let claim = AggregateClaim {
                column: column.to_string(),
                func: func.to_string(),
                input,
                accumulator,
                overflow,
                result,
            };
            lines.push(if block {
                PlanLine::BlockAggregate { claim }
            } else {
                PlanLine::Aggregate { claim }
            });
            continue;
        }
        if let Some(claim) = line.strip_prefix("rank fuse ") {
            let tiebreak = if claim.trim() == "no row tiebreak" {
                Vec::new()
            } else {
                claim
                    .trim()
                    .strip_prefix("row tiebreak")
                    .and_then(identity_key_list)
                    .ok_or_else(|| {
                        refused("claims `row tiebreak [$a.@id, $e.@type]` or `no row tiebreak`")
                    })?
            };
            lines.push(PlanLine::RankFuse { tiebreak });
            continue;
        }
        if let Some(claim) = line.strip_prefix("sort ") {
            let claim = claim.trim();
            let tiebreak = if claim == "no tiebreak" {
                Vec::new()
            } else {
                claim
                    .strip_prefix("tiebreak")
                    .and_then(identity_key_list)
                    .ok_or_else(|| refused("claims `tiebreak [$a, $b]` or `no tiebreak`"))?
            };
            lines.push(PlanLine::Sort { tiebreak });
            continue;
        }
        if let Some(rest) = line.strip_prefix("hydrate ") {
            let (binding, claim) = rest
                .split_once(':')
                .ok_or_else(|| refused("claims `hydrate $var: columns [a, b]`"))?;
            let binding = binding
                .trim()
                .strip_prefix('$')
                .filter(|binding| identifier(binding))
                .ok_or_else(|| refused("names the hydrated binding with `$`"))?;
            let columns = claim
                .trim()
                .strip_prefix("columns")
                .and_then(column_list)
                .ok_or_else(|| refused("claims `hydrate $var: columns [a, b]`"))?;
            lines.push(PlanLine::Hydrate {
                binding: binding.to_string(),
                columns,
            });
            continue;
        }
        if let Some(rest) = line.strip_prefix("hash join ") {
            let rest = rest.trim_start();
            let (binding, claim) = rest.split_once(' ').unwrap_or((rest, ""));
            let binding = binding
                .strip_prefix('$')
                .filter(|binding| identifier(binding))
                .ok_or_else(|| refused("names the joined binding with `$`"))?;
            let ran = match choice_and_ran(&format!("{ID_LOOKUP} {claim}"), &JOIN_SIDES) {
                Some((_, ran)) => ran,
                None => {
                    return Err(refused(
                        "claims `hash join $var[ ran <hash_join|id_lookup>]`",
                    ));
                }
            };
            lines.push(PlanLine::HashJoin {
                binding: binding.to_string(),
                ran,
            });
            continue;
        }
        let join = if let Some(rest) = line.strip_prefix("contains join ") {
            Some((rest, "contains join"))
        } else {
            line.strip_prefix("cross join ")
                .map(|rest| (rest, "cross join"))
        };
        if let Some((rest, head)) = join {
            let words: Vec<&str> = rest.split_whitespace().collect();
            let (haystack, needle) = match words[..] {
                [haystack, "contains", needle] => {
                    match (property_ref(haystack), property_ref(needle)) {
                        (Some(haystack), Some(needle)) => (haystack, needle),
                        _ => return Err(refused("names both columns as `$binding.property`")),
                    }
                }
                _ => return Err(refused(&format!("claims `{head} $h.x contains $n.y`"))),
            };
            lines.push(if head == "contains join" {
                PlanLine::ContainsJoin { haystack, needle }
            } else {
                PlanLine::CrossJoin { haystack, needle }
            });
            continue;
        }
        let (rest, expand) = if let Some(rest) = line.strip_prefix("scan ") {
            (rest, false)
        } else if let Some(rest) = line.strip_prefix("expand ") {
            (rest, true)
        } else {
            return Err(refused(
                "knows fourteen line heads: `scan`, `hash join`, `contains join`, `cross join`, `expand`, `filter`, `sort`, `rank fuse`, `aggregate`, `result columns`, `type`, `cast`, `no cast`, `pass`",
            ));
        };
        let Some((selector, claim)) = rest.split_once(':') else {
            return Err(refused("separates the scan from its claim with `:`"));
        };
        if expand {
            let ends: Vec<&str> = selector.split_whitespace().collect();
            let (src, edge_type, dst) = match ends.as_slice() {
                [src, dst] => (*src, None, *dst),
                [src, edge, dst] if identifier(edge) => (*src, Some((*edge).to_string()), *dst),
                _ => return Err(refused("spells `$src [<Edge>] $dst`")),
            };
            let (Some(src), Some(dst)) = (src.strip_prefix('$'), dst.strip_prefix('$')) else {
                return Err(refused("names both bindings with `$`"));
            };
            if !identifier(src) || !identifier(dst) {
                return Err(refused("names nonempty traversal bindings"));
            }
            if let Some(selection) = claim.trim().strip_prefix("selection ") {
                if edge_type.is_some() {
                    return Err(refused("selection names only its endpoint bindings"));
                }
                let (kind, list) = selection
                    .split_once(' ')
                    .ok_or_else(|| refused("lists selection members"))?;
                if !["named", "alternation", "wildcard"].contains(&kind) {
                    return Err(refused("uses named, alternation or wildcard"));
                }
                let inner = list
                    .trim()
                    .strip_prefix('[')
                    .and_then(|list| list.strip_suffix(']'))
                    .ok_or_else(|| refused("lists members in [...]"))?;
                let mut members = Vec::new();
                if !inner.trim().is_empty() {
                    for member in inner.split(',') {
                        let (name, direction) = member
                            .trim()
                            .rsplit_once(' ')
                            .ok_or_else(|| refused("spells each member as `Type out|in|both`"))?;
                        let name = if name.starts_with('"') {
                            serde_json::from_str::<String>(name).ok()
                        } else {
                            identifier(name).then(|| name.to_string())
                        }
                        .ok_or_else(|| refused("names a member type"))?;
                        if !["out", "in", "both"].contains(&direction) {
                            return Err(refused("uses out, in or both"));
                        }
                        members.push((name, direction.to_string()));
                    }
                }
                lines.push(PlanLine::EdgeSelection {
                    src: src.to_string(),
                    dst: dst.to_string(),
                    selection_kind: kind.to_string(),
                    members,
                });
                continue;
            }
            let (mode, ran) = claim
                .trim()
                .strip_prefix("mode ")
                .and_then(|rest| choice_and_ran(rest, &EXPAND_MODES))
                .ok_or_else(|| {
                    refused("claims `mode <csr|indexed_scan>[ ran <csr|indexed_scan>]`")
                })?;
            lines.push(PlanLine::ExpandMode {
                src: src.to_string(),
                edge_type,
                dst: dst.to_string(),
                mode,
                ran,
            });
            continue;
        }
        let (type_name, binding) = match selector.trim().split_once(" as ") {
            Some((type_name, binding)) => {
                let Some(binding) = binding.trim().strip_prefix('$') else {
                    return Err(refused("names the binding with `$`"));
                };
                (type_name.trim().to_string(), Some(binding.to_string()))
            }
            None => (selector.trim().to_string(), None),
        };
        if binding.as_ref().is_some_and(|name| !identifier(name)) {
            return Err(refused("names one nonempty binding"));
        }
        if !identifier(&type_name) {
            return Err(refused("names one node type"));
        }
        let claim = claim.trim();
        let claim = if claim == "no filter" {
            ScanClaim::NoFilter
        } else if claim == "no runtime filter" {
            lines.push(PlanLine::ScanRuntimeFilter {
                type_name,
                binding,
                column: None,
            });
            continue;
        } else if let Some(column) = claim.strip_prefix("runtime filter ") {
            let column = column.trim();
            if !identifier(column) {
                return Err(refused("claims `runtime filter <column>`, one column name"));
            }
            lines.push(PlanLine::ScanRuntimeFilter {
                type_name,
                binding,
                column: Some(column.to_string()),
            });
            continue;
        } else if let Some(ranked) = claim.strip_prefix("ranked ") {
            let mut words = ranked.split_whitespace();
            let kind = words
                .next()
                .filter(|kind| ["nearest", "bm25"].contains(kind))
                .ok_or_else(|| refused("claims `ranked nearest` or `ranked bm25`"))?;
            let mut fetch = None;
            let mut nprobes = None;
            while let Some(key) = words.next() {
                let count = words.next().and_then(|count| count.parse::<u64>().ok());
                match (key, count) {
                    ("fetch", Some(count)) if fetch.is_none() && nprobes.is_none() => {
                        fetch = Some(count);
                    }
                    ("nprobes", Some(count)) if nprobes.is_none() => {
                        nprobes = Some(count);
                    }
                    _ => {
                        return Err(refused(
                            "claims `ranked <kind>[ fetch <n>][ nprobes <n>]`: each key optional, at most once, in that order, followed by a whole number",
                        ));
                    }
                }
            }
            lines.push(PlanLine::ScanRanked {
                type_name,
                binding,
                index: kind.to_string(),
                fetch,
                nprobes,
            });
            continue;
        } else if let Some(access) = claim.strip_prefix("access ") {
            let words: Vec<_> = access.split_whitespace().collect();
            match words.as_slice() {
                [ID_LOOKUP] => lines.push(PlanLine::ScanAccess { type_name, binding }),
                ["sequential"] => lines.push(PlanLine::ScanIndex {
                    type_name,
                    binding,
                    column: None,
                }),
                ["index_probe", column] if identifier(column) => lines.push(PlanLine::ScanIndex {
                    type_name,
                    binding,
                    column: Some((*column).to_string()),
                }),
                _ => {
                    return Err(refused(
                        "claims `access id_lookup`, `access sequential` or `access index_probe <column>`",
                    ));
                }
            }
            continue;
        } else if let Some(list) = claim.strip_prefix("filter reads") {
            ScanClaim::FilterReads {
                reads: column_list(list).ok_or_else(|| refused("lists reads in `[...]`"))?,
            }
        } else {
            let (negated, list) = match claim.strip_prefix("not columns") {
                Some(list) => (true, list),
                None => match claim.strip_prefix("columns") {
                    Some(list) => (false, list),
                    None => {
                        return Err(refused(
                            "claims `columns`, `not columns`, `filter reads`, `no filter`, `access`, `ranked`, `runtime filter` or `no runtime filter`",
                        ));
                    }
                },
            };
            ScanClaim::Columns {
                columns: column_list(list).ok_or_else(|| refused("lists columns in `[...]`"))?,
                negated,
            }
        };
        lines.push(PlanLine::Scan {
            type_name,
            binding,
            claim,
        });
    }
    if lines.is_empty() {
        return Err("`--- expect plan` carries at least one line".to_string());
    }
    Ok(lines)
}

/// `binding.property` of a `$binding.property` reference, `None` for any
/// other spelling.
fn property_ref(text: &str) -> Option<String> {
    let (binding, property) = text.strip_prefix('$')?.split_once('.')?;
    (identifier(binding) && identifier(property)).then(|| format!("{binding}.{property}"))
}

fn expression_type(text: &str) -> Option<String> {
    let base = text.strip_suffix('?').unwrap_or(text);
    if base == "exact_integer" || base == "[exact_integer]" {
        return Some(text.to_string());
    }
    aggregate_type(text)
}

fn aggregate_type(text: &str) -> Option<String> {
    if text == "exact_integer" {
        return Some(text.to_string());
    }
    match crate::shape::parse_type(text) {
        Some(prop)
            if prop.enum_values.is_none()
                && prop.scalar != omnigraph_compiler::ScalarType::Blob =>
        {
            Some(prop.display_name())
        }
        Some(_) => None,
        None => crate::shape::is_type_name(text).then(|| text.to_string()),
    }
}

fn identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

/// A non-empty `[$a, $b]` list of bindings, or `None` when the text is not one.
fn identity_key_list(text: &str) -> Option<Vec<String>> {
    bracket_list(text, |part| {
        let text = part.strip_prefix('$')?;
        let (binding, property) = text.split_once('.').unwrap_or((text, "@id"));
        (identifier(binding) && ["@id", "@type"].contains(&property))
            .then(|| format!("${binding}.{property}"))
    })
}

fn declared_keys(node: &Value, key: &str) -> Result<Vec<String>, String> {
    node.get(key)
        .and_then(Value::as_array)
        .and_then(|keys| {
            keys.iter()
                .map(|key| key.as_str().map(str::to_string))
                .collect()
        })
        .ok_or_else(|| {
            let kind = node
                .get("node")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let id = node
                .get("id")
                .and_then(Value::as_u64)
                .map(|id| format!(" (node {id})"))
                .unwrap_or_default();
            format!("expect plan: the physical `{kind}`{id} carries no `{key}` key")
        })
}

fn column_list(text: &str) -> Option<Vec<String>> {
    bracket_list(text, |column| {
        column
            .split('.')
            .all(identifier)
            .then(|| column.to_string())
    })
}

/// The items of a non-empty `[a, b]` list, each trimmed and read by `item`;
/// `None` when the text is not such a list or `item` refuses one.
fn bracket_list(text: &str, item: impl Fn(&str) -> Option<String>) -> Option<Vec<String>> {
    let inner = text.trim().strip_prefix('[')?.strip_suffix(']')?;
    inner.split(',').map(|part| item(part.trim())).collect()
}

pub(crate) fn validate_plan_columns(lines: &[PlanLine], catalog: &Catalog) -> Result<(), String> {
    for line in lines {
        let types: Vec<&str> = match line {
            PlanLine::Type { ty, .. } => vec![ty],
            PlanLine::Cast { from, to, .. } => vec![from, to],
            _ => vec![],
        };
        for ty in types {
            if crate::shape::is_type_name(ty)
                && crate::shape::parse_type(ty).is_none()
                && !catalog.node_types.contains_key(ty)
            {
                return Err(format!("expect plan: unknown node type `{ty}`"));
            }
        }
        if let PlanLine::ResultColumns { columns } = line {
            for column in columns {
                let parsed = crate::shape::parse_shape_body(&[(0, column)])?;
                for field in parsed {
                    if let crate::shape::ShapeType::Node(name) = field.shape_type
                        && !catalog.node_types.contains_key(&name)
                    {
                        return Err(format!("expect plan: unknown node type `{name}`"));
                    }
                }
            }
        }
        if let PlanLine::Aggregate { claim } | PlanLine::BlockAggregate { claim } = line {
            for ty in [&claim.input, &claim.result] {
                if ty != "exact_integer"
                    && crate::shape::parse_type(ty).is_none()
                    && !catalog.node_types.contains_key(ty)
                {
                    return Err(format!("expect plan: unknown node type `{ty}`"));
                }
            }
        }
        let PlanLine::Scan {
            type_name,
            claim: ScanClaim::Columns { columns, .. },
            ..
        } = line
        else {
            continue;
        };
        let node = catalog
            .node_types
            .get(type_name)
            .ok_or_else(|| format!("expect plan: unknown node type `{type_name}`"))?;
        for column in columns {
            if node.arrow_schema.field_with_name(column).is_err() {
                return Err(format!(
                    "expect plan: unknown column `{column}` on node type `{type_name}`"
                ));
            }
        }
    }
    Ok(())
}

/// One scan of the explain document's logical plan.
#[derive(Debug)]
struct PlannedScan {
    type_key: String,
    binding: Option<String>,
    projection: Option<Vec<String>>,
    /// The reads of the pushed filter; `None` when the scan carries none.
    filter_reads: Option<Vec<String>>,
}

/// One `Expand` of the explain document's physical plan with the mode it
/// recorded.
#[derive(Debug)]
struct PlannedExpandMode {
    id: Option<u64>,
    src: String,
    edge_type: Option<String>,
    dst: String,
    mode: String,
}

/// One `Scan` of the explain document's physical plan with the access path
/// it recorded (`None` on a table scan), its ranking (`None` unranked) and
/// the column of the runtime filter it is marked with (`None` unmarked).
#[derive(Debug)]
struct PlannedAccess {
    type_key: String,
    binding: Option<String>,
    access: Option<String>,
    index_query: Option<omnigraph_planner::IndexQuery>,
    ranked: Option<PlannedRanking>,
    runtime_filter: Option<String>,
}

/// One `HashJoin` of the explain document's physical plan with its node id.
#[derive(Debug)]
struct PlannedJoin {
    id: Option<u64>,
    binding: String,
}

/// One `ContainsJoin` of the explain document's physical plan: the two
/// columns it pairs by, each `binding.property`.
#[derive(Debug)]
struct PlannedContainsJoin {
    haystack: String,
    needle: String,
}

/// The `ranked` object of a physical `Scan` row: the index, its fetch and
/// its probe cap (`None` when the row carries no `nprobes` key, `Some(0)`
/// when it is `null`, no cap).
#[derive(Debug)]
struct PlannedRanking {
    kind: String,
    fetch: Option<u64>,
    nprobes: Option<u64>,
}

/// `<choice>[ ran <choice>]` over the words `allowed`: the planned choice
/// and the side claimed to have run.
fn choice_and_ran(rest: &str, allowed: &[&str]) -> Option<(String, Option<String>)> {
    let mut words = rest.split_whitespace();
    let choice = words.next().filter(|word| allowed.contains(word))?;
    let ran = match (words.next(), words.next(), words.next()) {
        (None, _, _) => None,
        (Some("ran"), Some(side), None) if allowed.contains(&side) => Some(side.to_string()),
        _ => return None,
    };
    Some((choice.to_string(), ran))
}

/// The side of node `id`'s declared switch the run took: the last attempt of
/// the row that recorded one. `Err` names why there is no such side.
fn ran_side(report: Option<&[Row]>, id: Option<u64>, what: &str) -> Result<String, String> {
    let report = report.ok_or_else(|| {
        format!("expect plan: `ran` on {what} needs the execution report of the run")
    })?;
    let id = id.ok_or_else(|| format!("expect plan: the {what} row carries no `id`"))?;
    let sides: Vec<&str> = report
        .iter()
        .filter(|row| row.id as u64 == id)
        .filter_map(|row| row.attempts.last()?.ran.side())
        .collect();
    match sides[..] {
        [side] => Ok(side.to_string()),
        [] => Err(format!(
            "expect plan: the run recorded no side of a declared switch on {what} (node {id})"
        )),
        _ => Err(format!(
            "expect plan: the run recorded {sides:?} on {what} (node {id}), one side expected"
        )),
    }
}

/// The logical plan's scans and in-memory filters, and the physical plan's
/// traversal modes, access paths, hash joins, contains joins, each cross
/// join's `filters` and sort tie-breaks (`Err`: a `Sort` row with no `tiebreak`).
#[derive(Debug, Default)]
struct PlannedNodes {
    typed: Vec<Result<TypedNode, String>>,
    aggregates: Vec<Result<AggregateClaim, String>>,
    block_aggregates: Vec<Result<AggregateClaim, String>>,
    scans: Vec<PlannedScan>,
    filters: Vec<Vec<String>>,
    modes: Vec<PlannedExpandMode>,
    accesses: Vec<PlannedAccess>,
    joins: Vec<PlannedJoin>,
    contains_joins: Vec<PlannedContainsJoin>,
    cross_joins: Vec<Vec<String>>,
    sorts: Vec<Result<Vec<String>, String>>,
    fusions: Vec<Result<Vec<String>, String>>,
    selections: Vec<(String, String, Value)>,
    /// `(binding, sorted columns)` of every `HydrateColumns` binding.
    hydrations: Vec<(String, Vec<String>)>,
}

#[derive(Debug)]
struct TypedNode {
    op: String,
    gq: String,
    ty: String,
    cast_from: Option<(String, String)>,
}

fn typed_nodes(nodes: &[Result<TypedNode, String>]) -> Result<Vec<&TypedNode>, String> {
    nodes
        .iter()
        .map(|node| node.as_ref().map_err(Clone::clone))
        .collect()
}

fn typed_node(value: &Value) -> Result<TypedNode, String> {
    let bad = || "expect plan: malformed typed expression tree".to_string();
    let op = value.get("op").and_then(Value::as_str).ok_or_else(bad)?;
    let gq = value
        .get("gq")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(bad)?;
    let ty = value.get("type").and_then(Value::as_str).ok_or_else(bad)?;
    if expression_type(ty).as_deref() != Some(ty) {
        return Err(bad());
    }
    let args = value
        .get("args")
        .and_then(Value::as_array)
        .ok_or_else(bad)?;
    let valid_arity = match op {
        "property" | "variable" | "param" | "literal" | "alias" | "count_rows" => args.is_empty(),
        "nearest" | "aggregate" | "not" | "is_null" | "cast" => args.len() == 1,
        "search" | "match_text" | "bm25" | "and" | "or" | "compare" => args.len() == 2,
        "fuzzy" | "rrf" => (2..=3).contains(&args.len()),
        _ => false,
    };
    if !valid_arity {
        return Err(bad());
    }
    let cast_from = if op == "cast" {
        let child = &args[0];
        let child_gq = child.get("gq").and_then(Value::as_str).ok_or_else(bad)?;
        if child_gq != gq {
            return Err(bad());
        }
        Some((
            child_gq.to_string(),
            child
                .get("type")
                .and_then(Value::as_str)
                .ok_or_else(bad)?
                .to_string(),
        ))
    } else {
        None
    };
    Ok(TypedNode {
        op: op.to_string(),
        gq: gq.to_string(),
        ty: ty.to_string(),
        cast_from,
    })
}

fn collect_typed_tree(value: &Value, out: &mut Vec<Result<TypedNode, String>>) {
    let mut pending = match value {
        Value::Array(items) => items.iter().rev().collect::<Vec<_>>(),
        _ => vec![value],
    };
    while let Some(value) = pending.pop() {
        out.push(typed_node(value));
        if let Some(args) = value.get("args").and_then(Value::as_array) {
            pending.extend(args.iter().rev());
        }
    }
}

fn collect_typed_fields(node: &Value, out: &mut Vec<Result<TypedNode, String>>) {
    let mut pending = vec![node];
    while let Some(value) = pending.pop() {
        match value {
            Value::Object(fields) => {
                for (key, value) in fields {
                    if key.starts_with("typed_") {
                        if !value.is_null() || !matches!(key.as_str(), "typed_filter" | "typed_k") {
                            collect_typed_tree(value, out);
                        }
                    } else if key != "inputs" {
                        pending.push(value);
                    }
                }
            }
            Value::Array(items) => pending.extend(items),
            _ => {}
        }
    }
}

fn require_typed_fields(node: &Value) -> Result<(), String> {
    let missing = |key: &str| {
        format!(
            "expect plan: physical {} has missing or incomplete `{key}`",
            node.get("node").and_then(Value::as_str).unwrap_or("node")
        )
    };
    for (plain, typed) in [
        ("exprs", "typed_exprs"),
        ("filters", "typed_filters"),
        ("keys", "typed_keys"),
        ("residual", "typed_residual"),
    ] {
        if plain == "residual" && node.get("node").and_then(Value::as_str) == Some("Scan") {
            continue;
        }
        if let Some(values) = node.get(plain) {
            let count = values.as_array().ok_or_else(|| missing(typed))?.len();
            if node.get(typed).and_then(Value::as_array).map(Vec::len) != Some(count) {
                return Err(missing(typed));
            }
        }
    }
    if let Some(filter) = node.get("filter") {
        let typed = node
            .get("typed_filter")
            .ok_or_else(|| missing("typed_filter"))?;
        if filter.is_null() {
            if !typed.is_null() {
                return Err(missing("typed_filter"));
            }
        } else {
            let mut pending = vec![filter];
            let mut count = 0;
            while let Some(filter) = pending.pop() {
                match filter.get("kind").and_then(Value::as_str) {
                    Some("gq") => count += 1,
                    Some("and") => {
                        pending.push(filter.get("left").ok_or_else(|| missing("typed_filter"))?);
                        pending.push(filter.get("right").ok_or_else(|| missing("typed_filter"))?);
                    }
                    Some("id_after" | "version_window") => {}
                    _ => return Err(missing("typed_filter")),
                }
            }
            if typed.as_array().map(Vec::len) != Some(count) {
                return Err(missing("typed_filter"));
            }
        }
    }
    if node.get("node").and_then(Value::as_str) == Some("AntiJoin")
        || node.get("predicate").is_some()
    {
        for key in ["typed_left", "typed_right"] {
            if !node.get(key).is_some_and(Value::is_object) {
                return Err(missing(key));
            }
        }
        if node.get("node").and_then(Value::as_str) == Some("AntiJoin") {
            let mut leaf = &node["typed_left"];
            while leaf.get("op").and_then(Value::as_str) == Some("cast") {
                leaf = leaf
                    .get("args")
                    .and_then(Value::as_array)
                    .and_then(|args| args.first())
                    .ok_or_else(|| missing("typed_left"))?;
            }
            let aggregate = node.get("aggregate").ok_or_else(|| missing("aggregate"))?;
            match leaf.get("op").and_then(Value::as_str) {
                Some("count_rows") if aggregate.is_null() => {}
                Some("aggregate") if aggregate.is_object() => {}
                _ => return Err(missing("aggregate")),
            }
        }
    }
    if node.get("k").is_some_and(|k| !k.is_null())
        && !node.get("typed_k").is_some_and(Value::is_object)
    {
        return Err(missing("typed_k"));
    }
    if node.get("node").and_then(Value::as_str) == Some("ContainsJoin")
        && !node.get("typed_conjunct").is_some_and(Value::is_object)
    {
        return Err(missing("typed_conjunct"));
    }
    if let Some(ranked) = node.get("ranked").filter(|ranked| !ranked.is_null()) {
        for key in ["typed_query", "typed_score"] {
            if !ranked.get(key).is_some_and(Value::is_object) {
                return Err(missing(key));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_typed_plan(plan: &Value) -> Result<(), String> {
    let mut pending = vec![plan];
    while let Some(node) = pending.pop() {
        require_typed_fields(node)?;
        if let Some(inputs) = node.get("inputs").and_then(Value::as_array) {
            pending.extend(inputs);
        }
    }
    let mut nodes = PlannedNodes::default();
    planned_physical(plan, &mut nodes);
    typed_nodes(&nodes.typed)?;
    for entry in nodes.aggregates.iter().chain(&nodes.block_aggregates) {
        entry.as_ref().map_err(Clone::clone)?;
    }
    Ok(())
}

fn planned_physical(node: &Value, out: &mut PlannedNodes) {
    collect_typed_fields(node, &mut out.typed);
    let text = |key: &str| {
        node.get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let id = node.get("id").and_then(Value::as_u64);
    match node.get("node").and_then(Value::as_str) {
        Some("HydrateColumns") => {
            for binding in node
                .get("bindings")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let columns = binding
                    .get("columns")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect();
                out.hydrations.push((
                    binding
                        .get("binding")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    sorted_set(columns),
                ));
            }
        }
        Some("Aggregate") => match node.get("aggregates").and_then(Value::as_array) {
            Some(aggregates) => {
                out.aggregates
                    .extend(
                        aggregates
                            .iter()
                            .filter(|entry| !entry.is_null())
                            .map(|entry| {
                                serde_json::from_value(entry.clone()).map_err(|error| {
                                    format!("expect plan: malformed aggregate entry: {error}")
                                })
                            }),
                    )
            }
            None => out.aggregates.push(Err(
                "expect plan: Aggregate has no aggregates list".to_string()
            )),
        },
        Some("AntiJoin") => match node.get("aggregate") {
            Some(Value::Null) => {}
            Some(entry) => {
                let mut entry = entry.clone();
                if let Some(fields) = entry.as_object_mut()
                    && let Some(gq) = fields.remove("gq")
                {
                    fields.insert("column".into(), gq);
                }
                out.block_aggregates
                    .push(serde_json::from_value(entry).map_err(|error| {
                        format!("expect plan: malformed block aggregate entry: {error}")
                    }));
            }
            None => out.block_aggregates.push(Err(
                "expect plan: AntiJoin has no aggregate declaration".into(),
            )),
        },
        Some("Expand") => {
            out.selections.push((
                text("src"),
                text("dst"),
                node.get("edges").cloned().unwrap_or(Value::Null),
            ));
            out.modes.push(PlannedExpandMode {
                id,
                src: text("src"),
                edge_type: node
                    .get("edges")
                    .filter(|edges| edges.get("kind").and_then(Value::as_str) == Some("named"))
                    .and_then(|edges| edges.get("members"))
                    .and_then(Value::as_array)
                    .and_then(|members| members.first())
                    .and_then(|member| member.get("edge_type"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                dst: text("dst"),
                mode: text("mode"),
            });
        }
        Some("Scan") => out.accesses.push(PlannedAccess {
            type_key: text("table"),
            binding: node
                .get("binding")
                .and_then(Value::as_str)
                .map(str::to_string),
            access: node
                .get("access")
                .and_then(Value::as_str)
                .map(str::to_string),
            index_query: node
                .get("index_query")
                .and_then(|query| serde_json::from_value(query.clone()).ok()),
            ranked: node
                .get("ranked")
                .and_then(Value::as_object)
                .map(|ranked| PlannedRanking {
                    kind: ranked
                        .get("kind")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    fetch: ranked.get("fetch").and_then(Value::as_u64),
                    nprobes: ranked
                        .get("nprobes")
                        .map(|nprobes| nprobes.as_u64().unwrap_or(0)),
                }),
            runtime_filter: node
                .get("runtime_filter")
                .and_then(|filter| filter.get("column"))
                .and_then(Value::as_str)
                .map(str::to_string),
        }),
        Some("HashJoin") => out.joins.push(PlannedJoin {
            id,
            binding: text("binding"),
        }),
        Some("ContainsJoin") => out.contains_joins.push(PlannedContainsJoin {
            haystack: text("haystack").trim_start_matches('$').to_string(),
            needle: text("needle").trim_start_matches('$').to_string(),
        }),
        Some("CrossJoin") => out.cross_joins.push(
            node.get("filters")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
        ),
        Some("Sort") => out.sorts.push(declared_keys(node, "tiebreak")),
        Some("RankFuse") => out.fusions.push(declared_keys(node, "row_tiebreak")),
        _ => {}
    }
    if let Some(inputs) = node.get("inputs").and_then(Value::as_array) {
        for input in inputs {
            planned_physical(input, out);
        }
    }
}

fn planned_nodes(node: &Value, out: &mut PlannedNodes) -> Result<(), String> {
    match node.get("node").and_then(Value::as_str) {
        Some("TableScan") => out.scans.push(PlannedScan {
            type_key: node
                .get("table")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            binding: node
                .get("binding")
                .and_then(Value::as_str)
                .map(str::to_string),
            projection: node
                .get("projection")
                .and_then(Value::as_array)
                .map(|columns| {
                    columns
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                }),
            filter_reads: node
                .get("filter")
                .filter(|filter| !filter.is_null())
                .map(predicate_reads)
                .transpose()?,
        }),
        Some("Filter") => {
            let conjuncts = node
                .get("conjuncts")
                .and_then(Value::as_array)
                .ok_or_else(|| "expect plan: Filter has no conjuncts".to_string())?;
            let mut reads: Vec<String> = Vec::new();
            for conjunct in conjuncts {
                reads.extend(predicate_reads(conjunct)?);
            }
            reads.sort();
            reads.dedup();
            out.filters.push(reads);
        }
        _ => {}
    }
    if let Some(inputs) = node.get("inputs").and_then(Value::as_array) {
        for input in inputs {
            planned_nodes(input, out)?;
        }
    }
    Ok(())
}

/// The columns a serialized `Predicate` reads, rendered `binding.property`
/// (`binding` alone for a whole node object), sorted and deduplicated.
fn predicate_reads(predicate: &Value) -> Result<Vec<String>, String> {
    fn walk(predicate: &Value, out: &mut Vec<String>) -> Result<(), String> {
        let malformed = || format!("expect plan: unsupported or malformed predicate {predicate}");
        match predicate.get("kind").and_then(Value::as_str) {
            Some("and") => {
                for side in ["left", "right"] {
                    walk(predicate.get(side).ok_or_else(malformed)?, out)?;
                }
            }
            Some("gq") => {
                for read in predicate
                    .get("reads")
                    .and_then(Value::as_array)
                    .ok_or_else(malformed)?
                {
                    let binding = read
                        .get("binding")
                        .and_then(Value::as_str)
                        .filter(|name| identifier(name))
                        .ok_or_else(malformed)?;
                    out.push(match read.get("property") {
                        Some(Value::String(property)) if identifier(property) => {
                            format!("{binding}.{property}")
                        }
                        Some(Value::Null) => binding.to_string(),
                        _ => return Err(malformed()),
                    });
                }
            }
            _ => return Err(malformed()),
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(predicate, &mut out)?;
    Ok(sorted_set(out))
}

fn sorted_set(mut columns: Vec<String>) -> Vec<String> {
    columns.sort();
    columns.dedup();
    columns
}

/// Follow only wrappers of the declared result, never an inner branch.
fn result_columns(node: &Value) -> Option<&Vec<Value>> {
    match node.get("node")?.as_str()? {
        "Aggregate" | "Projection" | "MetadataCount" => node.get("columns")?.as_array(),
        "Sort" | "Page" | "Limit" => {
            let [input] = node.get("inputs")?.as_array()?.as_slice() else {
                return None;
            };
            result_columns(input)
        }
        _ => None,
    }
}

/// The first line the explain document contradicts, spelled with what the
/// document holds instead; a `ran` claim reads `report`, the execution
/// report of the run the document belongs to.
pub(crate) fn plan_mismatch(
    lines: &[PlanLine],
    explain: &Value,
    report: Option<&[Row]>,
) -> Option<String> {
    let mut nodes = PlannedNodes::default();
    if let Some(plan) = explain.get("logical_plan") {
        if let Err(error) = planned_nodes(plan, &mut nodes) {
            return Some(error);
        }
    }
    if let Some(plan) = explain.get("physical_plan") {
        planned_physical(plan, &mut nodes);
    }
    let passes: Vec<&str> = explain
        .get("passes")
        .and_then(Value::as_array)
        .map(|passes| passes.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    for line in lines {
        match line {
            PlanLine::Type { gq, ty } => {
                let typed = match typed_nodes(&nodes.typed) {
                    Ok(nodes) => nodes,
                    Err(error) => return Some(error),
                };
                let selected: Vec<_> = typed
                    .into_iter()
                    .filter(|node| node.op != "cast" && node.gq == *gq)
                    .collect();
                if selected.is_empty() || selected.iter().any(|node| node.ty != *ty) {
                    return Some(format!(
                        "expect plan: expression `{gq}` expected type {ty}; found {selected:?}"
                    ));
                }
            }
            PlanLine::Cast { gq, from, to } => {
                let typed = match typed_nodes(&nodes.typed) {
                    Ok(nodes) => nodes,
                    Err(error) => return Some(error),
                };
                if !typed.iter().any(|node| {
                    node.ty == *to
                        && node
                            .cast_from
                            .as_ref()
                            .is_some_and(|(child, ty)| child == gq && ty == from)
                }) {
                    return Some(format!(
                        "expect plan: no cast over `{gq}` from {from} to {to}"
                    ));
                }
            }
            PlanLine::NoCast { gq } => {
                let typed = match typed_nodes(&nodes.typed) {
                    Ok(nodes) => nodes,
                    Err(error) => return Some(error),
                };
                if typed.iter().any(|node| {
                    node.cast_from
                        .as_ref()
                        .is_some_and(|(child, _)| child == gq)
                }) {
                    return Some(format!("expect plan: unexpected cast over `{gq}`"));
                }
            }
            PlanLine::ResultColumns { columns } => {
                let declared = explain.get("physical_plan").and_then(result_columns);
                let expected: Vec<Value> = columns.iter().cloned().map(Value::String).collect();
                if declared != Some(&expected) {
                    return Some(format!(
                        "expect plan: result columns expected {columns:?}; found {declared:?}"
                    ));
                }
            }
            PlanLine::Aggregate { claim } | PlanLine::BlockAggregate { claim } => {
                let entries = if matches!(line, PlanLine::BlockAggregate { .. }) {
                    &nodes.block_aggregates
                } else {
                    &nodes.aggregates
                };
                let declared = match entries
                    .iter()
                    .map(|entry| entry.as_ref())
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(declared) => declared,
                    Err(error) => return Some(error.clone()),
                };
                let selected: Vec<_> = declared
                    .into_iter()
                    .filter(|entry| entry.column == claim.column)
                    .collect();
                if selected.is_empty() || selected.iter().any(|entry| *entry != claim) {
                    return Some(format!(
                        "expect plan: aggregate `{}` expected {claim:?}; found {selected:?}",
                        claim.column
                    ));
                }
            }
            PlanLine::Pass { name, negated } => {
                let fired = passes.iter().any(|pass| pass == name);
                if fired && *negated {
                    return Some(format!(
                        "expect plan: pass `{name}` fired; the document lists {passes:?}"
                    ));
                }
                if !fired && !*negated {
                    return Some(format!(
                        "expect plan: pass `{name}` did not fire; the document lists {passes:?}"
                    ));
                }
            }
            PlanLine::Filter { reads } => {
                let want = sorted_set(reads.clone());
                if !nodes.filters.contains(&want) {
                    return Some(format!(
                        "expect plan: no in-memory filter reads {want:?}; the plan keeps filters reading {:?}",
                        nodes.filters
                    ));
                }
            }
            PlanLine::EdgeSelection {
                src,
                dst,
                selection_kind,
                members,
            } => {
                let want = serde_json::json!({"kind":selection_kind,"members":members.iter().map(|(name,direction)| serde_json::json!({"edge_type":name,"direction":direction})).collect::<Vec<_>>()});
                let selected: Vec<_> = nodes
                    .selections
                    .iter()
                    .filter(|(from, to, _)| from == src && to == dst)
                    .collect();
                if selected.is_empty() || selected.iter().any(|(_, _, edges)| edges != &want) {
                    return Some(format!(
                        "expect plan: selection `${src} -> ${dst}` is not {want}; found {selected:?}"
                    ));
                }
            }
            PlanLine::RankFuse { tiebreak } => {
                let declared = match nodes.fusions.iter().cloned().collect::<Result<Vec<_>, _>>() {
                    Ok(declared) => declared,
                    Err(missing) => return Some(missing),
                };
                if !declared.contains(tiebreak) {
                    return Some(format!(
                        "expect plan: no RankFuse row tie-breaks on {tiebreak:?}; found {declared:?}"
                    ));
                }
            }
            PlanLine::Hydrate { binding, columns } => {
                let want = sorted_set(columns.clone());
                let found: Vec<&Vec<String>> = nodes
                    .hydrations
                    .iter()
                    .filter(|(hydrated, _)| hydrated == binding)
                    .map(|(_, columns)| columns)
                    .collect();
                if !found.contains(&&want) {
                    return Some(format!(
                        "expect plan: no `HydrateColumns` fetches {want:?} of `${binding}`; the plan hydrates {:?}",
                        nodes.hydrations
                    ));
                }
            }
            PlanLine::Sort { tiebreak } => {
                if nodes.sorts.is_empty() {
                    return Some("expect plan: the physical plan has no `Sort`".to_string());
                }
                let declared = match nodes.sorts.iter().cloned().collect::<Result<Vec<_>, _>>() {
                    Ok(declared) => declared,
                    Err(missing) => return Some(missing),
                };
                let want = tiebreak.clone();
                if !declared.contains(&want) {
                    return Some(format!(
                        "expect plan: no sort tie-breaks on {want:?}; the plan's sorts tie-break on {declared:?}"
                    ));
                }
            }
            PlanLine::ExpandMode {
                src,
                edge_type,
                dst,
                mode,
                ran,
            } => {
                let selected: Vec<_> = nodes
                    .modes
                    .iter()
                    .filter(|expand| {
                        expand.src == *src
                            && edge_type
                                .as_ref()
                                .is_none_or(|wanted| expand.edge_type.as_ref() == Some(wanted))
                            && expand.dst == *dst
                    })
                    .collect();
                let edge_type = edge_type.as_deref().unwrap_or("*");
                if selected.is_empty() {
                    return Some(format!(
                        "expect plan: no expand `${src} {edge_type} ${dst}` in the physical plan"
                    ));
                }
                for expand in selected {
                    if expand.mode != *mode {
                        return Some(format!(
                            "expect plan: the expand `${src} {edge_type} ${dst}` runs `{}`, expected `{mode}`",
                            expand.mode
                        ));
                    }
                    let Some(ran) = ran else {
                        continue;
                    };
                    let what = format!("the expand `${src} {edge_type} ${dst}`");
                    match ran_side(report, expand.id, &what) {
                        Err(mismatch) => return Some(mismatch),
                        Ok(side) if side != *ran => {
                            return Some(format!(
                                "expect plan: {what} ran `{side}`, expected `ran {ran}`"
                            ));
                        }
                        Ok(_) => {}
                    }
                }
            }
            PlanLine::ScanRanked {
                type_name,
                binding,
                index: kind,
                fetch,
                nprobes,
            } => {
                let selected = match physical_scans(&nodes, type_name, binding.as_deref()) {
                    Ok(selected) => selected,
                    Err(mismatch) => return Some(mismatch),
                };
                let mut rankings = Vec::with_capacity(selected.len());
                for scan in selected {
                    let Some(ranked) = &scan.ranked else {
                        return Some(format!(
                            "expect plan: the scan of `{type_name}` is not ranked, expected `ranked {kind}`"
                        ));
                    };
                    rankings.push(ranked);
                }
                let of_kind: Vec<&&PlannedRanking> = rankings
                    .iter()
                    .filter(|ranked| ranked.kind == *kind)
                    .collect();
                if of_kind.is_empty() {
                    let kinds: Vec<&str> = rankings.iter().map(|r| r.kind.as_str()).collect();
                    return Some(format!(
                        "expect plan: the scan of `{type_name}` is ranked by {kinds:?}, expected `{kind}`"
                    ));
                }
                if let Some(fetch) = fetch {
                    let fetches: Vec<Option<u64>> = of_kind.iter().map(|r| r.fetch).collect();
                    if !fetches.contains(&Some(*fetch)) {
                        return Some(match fetches[..] {
                            [None] => format!(
                                "expect plan: the ranked scan of `{type_name}` has no fetch, expected {fetch}"
                            ),
                            [Some(have)] => format!(
                                "expect plan: the ranked scan of `{type_name}` fetches {have}, expected {fetch}"
                            ),
                            _ => format!(
                                "expect plan: the `{kind}` scans of `{type_name}` fetch {fetches:?}, expected {fetch}"
                            ),
                        });
                    }
                }
                if let Some(nprobes) = nprobes {
                    let caps: Vec<Option<u64>> = of_kind.iter().map(|r| r.nprobes).collect();
                    if !caps.contains(&Some(*nprobes)) {
                        return Some(match caps[..] {
                            [None] => format!(
                                "expect plan: the ranked scan of `{type_name}` carries no probe cap, expected nprobes {nprobes}"
                            ),
                            [Some(have)] => format!(
                                "expect plan: the ranked scan of `{type_name}` probes {have}, expected nprobes {nprobes}"
                            ),
                            _ => format!(
                                "expect plan: the `{kind}` scans of `{type_name}` probe {caps:?}, expected nprobes {nprobes}"
                            ),
                        });
                    }
                }
            }
            PlanLine::ScanRuntimeFilter {
                type_name,
                binding,
                column,
            } => {
                let selected = match physical_scans(&nodes, type_name, binding.as_deref()) {
                    Ok(selected) => selected,
                    Err(mismatch) => return Some(mismatch),
                };
                for scan in selected {
                    match (&scan.runtime_filter, column) {
                        (None, Some(column)) => {
                            return Some(format!(
                                "expect plan: the scan of `{type_name}` carries no runtime filter, expected one on `{column}`"
                            ));
                        }
                        (Some(have), None) => {
                            return Some(format!(
                                "expect plan: the scan of `{type_name}` carries a runtime filter on `{have}`, expected none"
                            ));
                        }
                        (Some(have), Some(column)) if have != column => {
                            return Some(format!(
                                "expect plan: the scan of `{type_name}` carries a runtime filter on `{have}`, expected `{column}`"
                            ));
                        }
                        _ => {}
                    }
                }
            }
            PlanLine::ContainsJoin { haystack, needle } => {
                let found = nodes
                    .contains_joins
                    .iter()
                    .any(|join| join.haystack == *haystack && join.needle == *needle);
                if !found {
                    let known: Vec<String> = nodes
                        .contains_joins
                        .iter()
                        .map(|join| format!("${} contains ${}", join.haystack, join.needle))
                        .collect();
                    return Some(format!(
                        "expect plan: no contains join `${haystack} contains ${needle}` in the physical plan; it joins {known:?}"
                    ));
                }
            }
            PlanLine::CrossJoin { haystack, needle } => {
                let conjunct = format!("${haystack} contains ${needle}");
                let found = nodes
                    .cross_joins
                    .iter()
                    .any(|filters| filters.contains(&conjunct));
                if !found {
                    return Some(format!(
                        "expect plan: no cross join holding `{conjunct}` in the physical plan; its cross joins hold {:?}",
                        nodes.cross_joins
                    ));
                }
            }
            PlanLine::ScanIndex {
                type_name,
                binding,
                column,
            } => {
                let selected = match physical_scans(&nodes, type_name, binding.as_deref()) {
                    Ok(selected) => selected,
                    Err(mismatch) => return Some(mismatch),
                };
                let want = if column.is_some() {
                    "index_probe"
                } else {
                    "sequential"
                };
                for scan in selected {
                    if scan.access.as_deref() != Some(want) {
                        return Some(format!(
                            "expect plan: the scan of `{type_name}` has access {:?}, expected `{want}`",
                            scan.access
                        ));
                    }
                    if let Some(column) = column {
                        if !scan
                            .index_query
                            .as_ref()
                            .is_some_and(|query| probes_column(query, column))
                        {
                            return Some(format!(
                                "expect plan: the scan of `{type_name}` does not probe column `{column}`"
                            ));
                        }
                    }
                }
            }
            PlanLine::ScanAccess { type_name, binding } => {
                let selected = match physical_scans(&nodes, type_name, binding.as_deref()) {
                    Ok(selected) => selected,
                    Err(mismatch) => return Some(mismatch),
                };
                for scan in selected {
                    match &scan.access {
                        None => {
                            let joined = nodes
                                .joins
                                .iter()
                                .any(|join| Some(&join.binding) == scan.binding.as_ref());
                            return Some(if joined {
                                format!(
                                    "expect plan: the scan of `{type_name}` is the build side of a hash join, expected `access {ID_LOOKUP}`"
                                )
                            } else {
                                format!(
                                    "expect plan: the scan of `{type_name}` is a table scan with no access path, expected `access {ID_LOOKUP}`"
                                )
                            });
                        }
                        Some(have) if have != ID_LOOKUP => {
                            return Some(format!(
                                "expect plan: the scan of `{type_name}` reaches its rows by `{have}`, expected `{ID_LOOKUP}`"
                            ));
                        }
                        Some(_) => {}
                    }
                }
            }
            PlanLine::HashJoin { binding, ran } => {
                let selected: Vec<&PlannedJoin> = nodes
                    .joins
                    .iter()
                    .filter(|join| join.binding == *binding)
                    .collect();
                if selected.is_empty() {
                    let known: Vec<String> = nodes
                        .joins
                        .iter()
                        .map(|join| format!("${}", join.binding))
                        .collect();
                    return Some(format!(
                        "expect plan: no hash join over `${binding}` in the physical plan; it joins {known:?}"
                    ));
                }
                for join in selected {
                    let Some(ran) = ran else {
                        continue;
                    };
                    let what = format!("the hash join over `${binding}`");
                    match ran_side(report, join.id, &what) {
                        Err(mismatch) => return Some(mismatch),
                        Ok(side) if side != *ran => {
                            return Some(format!(
                                "expect plan: {what} ran `{side}`, expected `ran {ran}`"
                            ));
                        }
                        Ok(_) => {}
                    }
                }
            }
            PlanLine::Scan {
                type_name,
                binding,
                claim,
            } => {
                let type_key = format!("node:{type_name}");
                let selected: Vec<&PlannedScan> = nodes
                    .scans
                    .iter()
                    .filter(|scan| scan.type_key == type_key)
                    .filter(|scan| {
                        binding
                            .as_ref()
                            .is_none_or(|b| scan.binding.as_ref() == Some(b))
                    })
                    .collect();
                if selected.is_empty() {
                    let known: Vec<String> = nodes
                        .scans
                        .iter()
                        .map(|scan| {
                            format!(
                                "{}{}",
                                scan.type_key,
                                scan.binding
                                    .as_ref()
                                    .map(|b| format!(" as ${b}"))
                                    .unwrap_or_default()
                            )
                        })
                        .collect();
                    return Some(format!(
                        "expect plan: no scan of `{type_name}`{} in the plan; the plan scans {known:?}",
                        binding
                            .as_ref()
                            .map(|b| format!(" bound to `${b}`"))
                            .unwrap_or_default()
                    ));
                }
                for scan in selected {
                    if let Some(mismatch) = scan_mismatch(type_name, scan, claim) {
                        return Some(mismatch);
                    }
                }
            }
        }
    }
    None
}

/// The physical scans of `type_name` (bound to `binding` when named), or
/// the mismatch naming what the plan scans instead.
fn probes_column(query: &omnigraph_planner::IndexQuery, column: &str) -> bool {
    use omnigraph_planner::IndexQuery;
    match query {
        IndexQuery::Search { column: found, .. } => found == column,
        IndexQuery::And { left, right } | IndexQuery::Or { left, right } => {
            probes_column(left, column) || probes_column(right, column)
        }
        IndexQuery::Not { input } => probes_column(input, column),
    }
}

fn physical_scans<'n>(
    nodes: &'n PlannedNodes,
    type_name: &str,
    binding: Option<&str>,
) -> Result<Vec<&'n PlannedAccess>, String> {
    let type_key = format!("node:{type_name}");
    let selected: Vec<&PlannedAccess> = nodes
        .accesses
        .iter()
        .filter(|scan| scan.type_key == type_key)
        .filter(|scan| binding.is_none_or(|b| scan.binding.as_deref() == Some(b)))
        .collect();
    if selected.is_empty() {
        let known: Vec<String> = nodes
            .accesses
            .iter()
            .map(|scan| {
                format!(
                    "{}{}",
                    scan.type_key,
                    scan.binding
                        .as_ref()
                        .map(|b| format!(" as ${b}"))
                        .unwrap_or_default()
                )
            })
            .collect();
        return Err(format!(
            "expect plan: no scan of `{type_name}`{} in the physical plan; it scans {known:?}",
            binding
                .map(|b| format!(" bound to `${b}`"))
                .unwrap_or_default()
        ));
    }
    Ok(selected)
}

fn scan_mismatch(type_name: &str, scan: &PlannedScan, claim: &ScanClaim) -> Option<String> {
    match claim {
        ScanClaim::Columns { columns, negated } => {
            projection_mismatch("scan", type_name, &scan.projection, columns, *negated)
        }
        ScanClaim::FilterReads { reads } => {
            let want = sorted_set(reads.clone());
            match &scan.filter_reads {
                None => Some(format!(
                    "expect plan: the scan of `{type_name}` carries no filter, expected one reading {want:?}"
                )),
                Some(have) if *have != want => Some(format!(
                    "expect plan: the scan of `{type_name}` filters on {have:?}, expected {want:?}"
                )),
                Some(_) => None,
            }
        }
        ScanClaim::NoFilter => scan.filter_reads.as_ref().map(|have| {
            format!("expect plan: the scan of `{type_name}` carries a filter reading {have:?}")
        }),
    }
}

fn projection_mismatch(
    kind: &str,
    type_name: &str,
    projection: &Option<Vec<String>>,
    columns: &[String],
    negated: bool,
) -> Option<String> {
    let Some(projection) = projection else {
        return Some(format!(
            "expect plan: the {kind} of `{type_name}` carries no projection (every column)"
        ));
    };
    let have = sorted_set(projection.clone());
    if negated {
        columns.iter().find(|column| have.contains(column)).map(|present| {
            format!("expect plan: the {kind} of `{type_name}` reads `{present}`; it projects {have:?}")
        })
    } else {
        let want = sorted_set(columns.to_vec());
        (have != want).then(|| {
            format!("expect plan: the {kind} of `{type_name}` projects {have:?}, expected {want:?}")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The document alone: no `ran` line in these tests reads a report.
    fn check(lines: &[PlanLine], explain: &Value) -> Option<String> {
        plan_mismatch(lines, explain, None)
    }

    #[test]
    fn typed_claims_parse_lists_vectors_exact_types_and_colons_in_gq() {
        for line in [
            "type $p.age: I64?",
            "type [1, 2.5]: [F64]",
            "type $q: Vector(4)?",
            "type $p: Person",
            "type $n: [exact_integer]?",
            "type \"a:b\": String",
            "cast $p.age: I64? -> F64?",
            "cast $items: [I64]? -> [exact_integer]?",
            "no cast $p.age",
        ] {
            assert!(parse_plan_body(&[(0, line)]).is_ok(), "{line}");
        }
        for line in [
            "type : I64",
            "type $p.age I64",
            "type $p.age: I64??",
            "cast $p.age: I64 F64",
            "cast $p.age: I64 ->",
            "no cast ",
            "type $n: [[exact_integer]]",
        ] {
            assert!(parse_plan_body(&[(0, line)]).is_err(), "{line}");
        }
    }

    fn typed_property() -> Value {
        json!({"op":"property","gq":"$p.age","type":"I64?","args":[]})
    }

    #[test]
    fn typed_claims_walk_all_physical_fields_and_distinguish_transparent_casts() {
        let cast = json!({"op":"cast","gq":"$p.age","type":"F64?","args":[typed_property()]});
        let explain = json!({"physical_plan":{"node":"Projection","typed_exprs":[cast.clone()],"inputs":[
            {"node":"Scan", "ranked":{"typed_query":typed_property()}, "typed_filter":[typed_property()]},
            {"node":"ContainsJoin", "typed_conjunct":typed_property(), "typed_residual":[typed_property()]},
            {"node":"AntiJoin", "typed_left":typed_property(), "typed_right":typed_property()},
            {"node":"Sort", "typed_keys":[typed_property()], "tiebreak":[]},
            {"node":"RankFuse", "typed_k":typed_property(), "row_tiebreak":[]}
        ]}});
        let claims = parse_plan_body(&[
            (0, "type $p.age: I64?"),
            (1, "cast $p.age: I64? -> F64?"),
            (2, "no cast $other"),
        ])
        .unwrap();
        assert_eq!(check(&claims, &explain), None);
        for line in [
            "type $p.age: F64?",
            "type $missing: I64?",
            "cast $p.age: I64 -> F64?",
            "cast $missing: I64? -> F64?",
            "no cast $p.age",
        ] {
            let claims = parse_plan_body(&[(0, line)]).unwrap();
            assert!(check(&claims, &explain).is_some(), "{line}");
        }
        let mut wrong = explain.clone();
        wrong["physical_plan"]["inputs"][0]["ranked"]["typed_query"]["type"] = json!("F64?");
        assert!(check(&claims, &wrong).is_some());
    }

    #[test]
    fn round_trip_guard_requires_every_expression_bearing_root() {
        let tree = typed_property();
        let examples = [
            (
                json!({"node":"Projection", "exprs":["$p.age"], "typed_exprs":[tree.clone()]}),
                "typed_exprs",
            ),
            (
                json!({"node":"Filter", "filters":["$p.age"], "typed_filters":[tree.clone()]}),
                "typed_filters",
            ),
            (
                json!({"node":"Sort", "keys":["$p.age asc"], "typed_keys":[tree.clone()]}),
                "typed_keys",
            ),
            (
                json!({"node":"Scan", "filter":{"kind":"and", "left":{"kind":"gq"}, "right":{"kind":"id_after"}}, "typed_filter":[tree.clone()]}),
                "typed_filter",
            ),
            (
                json!({"node":"ContainsJoin", "residual":["$p.age"], "typed_residual":[tree.clone()], "typed_conjunct":tree.clone()}),
                "typed_conjunct",
            ),
            (
                json!({"node":"AntiJoin", "predicate":"count > 0", "typed_left":{"op":"count_rows","gq":"count","type":"I64","args":[]}, "typed_right":tree.clone(), "aggregate":null}),
                "typed_left",
            ),
            (
                json!({"node":"AntiJoin", "predicate":"count > 0", "typed_left":{"op":"count_rows","gq":"count","type":"I64","args":[]}, "typed_right":tree.clone(), "aggregate":null}),
                "typed_right",
            ),
            (
                json!({"node":"AntiJoin", "predicate":"count > 0", "typed_left":{"op":"count_rows","gq":"count","type":"I64","args":[]}, "typed_right":tree.clone(), "aggregate":null}),
                "aggregate",
            ),
            (
                json!({"node":"RankFuse", "k":"$p.age", "typed_k":tree.clone()}),
                "typed_k",
            ),
            (
                json!({"node":"Scan", "ranked":{"query":"$p.age", "typed_query":tree.clone(), "typed_score":tree.clone()}}),
                "ranked",
            ),
        ];
        for (valid, key) in examples {
            assert_eq!(validate_typed_plan(&valid), Ok(()), "{valid}");
            let mut missing = valid.clone();
            if key == "ranked" {
                missing[key].as_object_mut().unwrap().remove("typed_score");
            } else {
                missing.as_object_mut().unwrap().remove(key);
            }
            assert!(validate_typed_plan(&missing).is_err(), "{missing}");
            let mut empty = valid;
            if key == "ranked" {
                empty[key]["typed_query"] = Value::Null;
            } else {
                empty[key] = json!([]);
            }
            assert!(validate_typed_plan(&empty).is_err(), "{empty}");
        }
        assert_eq!(
            validate_typed_plan(&json!({"node":"RankFuse","k":null})),
            Ok(())
        );
        assert_eq!(
            validate_typed_plan(&json!({"node":"Scan","filter":null,"typed_filter":null})),
            Ok(())
        );
    }

    #[test]
    fn typed_claims_and_round_trip_guard_refuse_malformed_trees() {
        let claims = parse_plan_body(&[(0, "no cast $other")]).unwrap();
        for tree in [
            json!(null),
            json!([]),
            json!({"op":"property","gq":"$p.age","args":[]}),
            json!({"op":"cast","gq":"$p.age","type":"F64?","args":[]}),
            json!({"op":"unknown","gq":"$p.age","type":"I64?","args":[]}),
            json!({"op":"cast","gq":"$p.age","type":"F64?","args":[null]}),
        ] {
            let plan = json!({"node":"Projection","typed_exprs":[tree]});
            assert!(validate_typed_plan(&plan).is_err());
            assert!(check(&claims, &json!({"physical_plan":plan})).is_some());
        }
    }

    #[test]
    fn result_columns_parse_nested_type_brackets_and_vector_parentheses() {
        for line in [
            "result columns [xs: [String]?, embedding: Vector(4), n: I64?]",
            "result columns [ xs:[String]?, embedding: Vector(4) , n: I64? ]",
        ] {
            let lines = parse_plan_body(&[(0, line)]).expect("result columns parse");
            let explain = json!({"physical_plan": {
                "node":"Projection",
                "columns":["xs: [String]?", "embedding: Vector(4)", "n: I64?"]
            }});
            assert_eq!(check(&lines, &explain), None, "{line}");
        }
        for line in [
            "result columns [xs: [String]?,, n: I64?]",
            "result columns [xs: [String]?, n: I64?,]",
            "result columns [xs: [String], n: I64?",
            "result columns [embedding: Vector(4, n: I64?]",
            "result columns [n I64?]",
            "result columns [n: I64?] trailing",
        ] {
            assert!(parse_plan_body(&[(0, line)]).is_err(), "{line}");
        }
    }

    #[test]
    fn result_columns_match_root_order_types_and_declared_nullability() {
        let lines = parse_plan_body(&[(0, "result columns [n: I64?, p: Person]")])
            .expect("result columns parse");
        let explain = json!({"physical_plan": {
            "node":"Page","inputs":[{"node":"Sort","inputs":[{
                "node":"Aggregate","columns":["n: I64?", "p: Person"],
                "inputs":[{"node":"Projection","columns":["inner: String"]}]
            }]}]
        }});
        assert_eq!(check(&lines, &explain), None);
        for wrong_columns in [
            json!(["p: Person", "n: I64?"]),
            json!(["n: I64", "p: Person"]),
            json!(["n: F64?", "p: Person"]),
            json!(["other: I64?", "p: Person"]),
            json!(["n: I64?"]),
        ] {
            let mut wrong = explain.clone();
            wrong["physical_plan"]["inputs"][0]["inputs"][0]["columns"] = wrong_columns;
            assert!(check(&lines, &wrong).is_some());
        }
        let absent = json!({"physical_plan": {"node":"Projection"}});
        assert!(check(&lines, &absent).is_some());
        let hidden = json!({"physical_plan": {
            "node":"CrossJoin","inputs":[{
                "node":"Projection","columns":["n: I64?", "p: Person"]
            }]
        }});
        assert!(check(&lines, &hidden).is_some());
    }

    #[test]
    fn aggregate_claims_check_every_declared_fact_and_require_the_column() {
        let lines = parse_plan_body(&[(
            0,
            "aggregate total: sum(I64?) exact_integer round_to_nearest -> F64?",
        )])
        .unwrap();
        let explain = json!({"physical_plan": {"node":"Limit","inputs":[{
            "node":"Aggregate","aggregates":[null,{
                "column":"total","func":"sum","input":"I64?",
                "accumulator":"exact_integer","overflow":"round_to_nearest","result":"F64?"
            }]
        }]}});
        assert_eq!(check(&lines, &explain), None);
        for (field, replacement) in [
            ("column", "other"),
            ("func", "avg"),
            ("input", "I64"),
            ("accumulator", "float64"),
            ("overflow", "error"),
            ("result", "F64"),
        ] {
            let mut wrong = explain.clone();
            wrong["physical_plan"]["inputs"][0]["aggregates"][1][field] = json!(replacement);
            assert!(check(&lines, &wrong).is_some(), "{field}");
            let mut missing = explain.clone();
            missing["physical_plan"]["inputs"][0]["aggregates"][1]
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert!(check(&lines, &missing).is_some(), "missing {field}");
        }
        for node in [json!({"node":"Aggregate"}), json!({"node":"MetadataCount"})] {
            assert!(check(&lines, &json!({"physical_plan":node})).is_some());
        }
    }

    #[test]
    fn block_aggregate_claims_read_the_leaf_spec_beneath_left_casts() {
        let lines = parse_plan_body(&[
            (
                0,
                "block aggregate max($c.large): max(U64?) extremum error -> U64?",
            ),
            (1, "type max($c.large): U64?"),
            (2, "cast max($c.large): U64? -> exact_integer?"),
        ])
        .unwrap();
        let leaf = json!({"op":"aggregate","gq":"max($c.large)","type":"U64?","args":[{"op":"property","gq":"$c.large","type":"U64?","args":[]}]});
        let explain = json!({"physical_plan":{"node":"AntiJoin","predicate":"max($c.large) > $bound",
            "aggregate":{"gq":"max($c.large)","func":"max","input":"U64?","accumulator":"extremum","overflow":"error","result":"U64?"},
            "typed_left":{"op":"cast","gq":"max($c.large)","type":"exact_integer?","args":[leaf]},
            "typed_right":{"op":"param","gq":"$bound","type":"exact_integer","args":[]}}});
        assert_eq!(check(&lines, &explain), None);
        assert_eq!(validate_typed_plan(&explain["physical_plan"]), Ok(()));
        for key in ["gq", "func", "input", "accumulator", "overflow", "result"] {
            let mut wrong = explain.clone();
            wrong["physical_plan"]["aggregate"]
                .as_object_mut()
                .unwrap()
                .remove(key);
            assert!(check(&lines, &wrong).is_some(), "{key}");
        }
        for key in ["typed_left", "typed_right", "aggregate"] {
            let mut wrong = explain.clone();
            wrong["physical_plan"].as_object_mut().unwrap().remove(key);
            assert!(
                validate_typed_plan(&wrong["physical_plan"]).is_err(),
                "{key}"
            );
        }
        for line in [
            "block aggregate : max(U64?) extremum error -> U64?",
            "block aggregate max($c.large): max(U64?) bogus error -> U64?",
        ] {
            assert!(parse_plan_body(&[(0, line)]).is_err(), "{line}");
        }
    }

    #[test]
    fn aggregate_claims_parse_node_and_value_types_and_refuse_malformed_lines() {
        for line in [
            "aggregate n: count(Person) count error -> I64?",
            "aggregate p.age: sum(I32) exact_integer round_to_nearest -> F64?",
            "aggregate first: min(Date?) extremum error -> Date?",
            "aggregate n: count([String]?) count error -> I64?",
            "aggregate n: count(Vector(4)) count error -> I64?",
            "aggregate n: count(Vector(4) ) count error -> I64?",
            "aggregate n: count(Vector(4))\tcount error -> I64?",
            "aggregate total: sum(exact_integer) exact_integer round_to_nearest -> F64?",
        ] {
            assert!(parse_plan_body(&[(0, line)]).is_ok(), "{line}");
        }
        for line in [
            "aggregate total sum(I64) exact_integer round_to_nearest -> F64?",
            "aggregate total: total(I64) exact_integer round_to_nearest -> F64?",
            "aggregate total: sum(I64 exact_integer round_to_nearest -> F64?",
            "aggregate total: sum(I64) integer round_to_nearest -> F64?",
            "aggregate total: sum(I64) exact_integer wrap -> F64?",
            "aggregate total: sum(I64) exact_integer -> F64?",
            "aggregate total: sum(I64) exact_integer round_to_nearest F64?",
            "aggregate total: sum(I64) exact_integer round_to_nearest -> F64? extra",
            "aggregate total: sum(Person?) exact_integer round_to_nearest -> F64?",
            "aggregate total: sum(Blob) exact_integer round_to_nearest -> F64?",
            "aggregate total: sum(enum(a, b)) exact_integer round_to_nearest -> F64?",
            "aggregate : sum(I64) exact_integer round_to_nearest -> F64?",
        ] {
            assert!(parse_plan_body(&[(0, line)]).is_err(), "{line}");
        }
        let lines =
            parse_plan_body(&[(0, "aggregate n: count(Missing) count error -> I64?")]).unwrap();
        let schema =
            omnigraph_compiler::schema::parser::parse_schema("node Person { name: String @key }")
                .unwrap();
        let catalog = omnigraph_compiler::catalog::build_catalog(&schema).unwrap();
        assert!(
            validate_plan_columns(&lines, &catalog)
                .unwrap_err()
                .contains("Missing")
        );
    }

    #[test]
    fn selected_members_and_identity_claims_detect_missing_or_reordered_keys() {
        let claims = parse_plan_body(&[
            (
                0,
                "expand $a $b: selection alternation [Knows out, Likes in]",
            ),
            (1, "expand $a $b: mode indexed_scan"),
            (2, "sort tiebreak [$e.@type, $e.@id]"),
            (3, "rank fuse row tiebreak [$e.@type, $e.@id]"),
        ])
        .unwrap();
        let doc = json!({"physical_plan":{"node":"Sort","tiebreak":["$e.@type","$e.@id"],"inputs":[
            {"node":"RankFuse","row_tiebreak":["$e.@type","$e.@id"],"inputs":[
                {"node":"Expand","src":"a","dst":"b","mode":"indexed_scan","edges":{"kind":"alternation","members":[{"edge_type":"Knows","direction":"out"},{"edge_type":"Likes","direction":"in"}]}}
            ]}
        ]}});
        assert_eq!(check(&claims, &doc), None);
        for replacement in [json!(["$e.@id"]), json!(["$e.@id", "$e.@type"])] {
            let mut wrong = doc.clone();
            wrong["physical_plan"]["tiebreak"] = replacement.clone();
            assert!(check(&claims, &wrong).is_some());
            let mut wrong = doc.clone();
            wrong["physical_plan"]["inputs"][0]["row_tiebreak"] = replacement;
            assert!(check(&claims, &wrong).is_some());
        }
        for replacement in [json!("out"), json!("both")] {
            let mut wrong = doc.clone();
            wrong["physical_plan"]["inputs"][0]["inputs"][0]["edges"]["members"][1]["direction"] =
                replacement;
            assert!(check(&claims, &wrong).is_some());
        }
        let empty = parse_plan_body(&[(0, "expand $a $b: selection wildcard []")]).unwrap();
        let empty_doc = json!({"physical_plan":{"node":"Expand","src":"a","dst":"b","edges":{"kind":"wildcard","members":[]}}});
        assert_eq!(check(&empty, &empty_doc), None);
        let mut wrong = empty_doc;
        wrong["physical_plan"]["edges"]["kind"] = json!("alternation");
        assert!(check(&empty, &wrong).is_some());
    }

    fn explain() -> Value {
        json!({
            "logical_plan": {
                "node": "Aggregate",
                "inputs": [{
                    "node": "Filter",
                    "conjuncts": [{
                        "kind": "gq",
                        "reads": [
                            {"binding": "d", "property": "rank"},
                            {"binding": "e", "property": "rank"},
                        ],
                        "text": "d.rank = e.rank",
                    }],
                    "inputs": [{
                        "node": "Join",
                        "kind": "Cross",
                        "inputs": [
                            {
                                "node": "TableScan",
                                "table": "node:Doc",
                                "binding": "d",
                                "projection": ["__id", "slug", "rank", "state"],
                                "filter": {
                                    "kind": "and",
                                    "left": {
                                        "kind": "gq",
                                        "reads": [{"binding": "d", "property": "state"}],
                                        "text": "d.state = 'open'",
                                    },
                                    "right": {
                                        "kind": "gq",
                                        "reads": [{"binding": "d", "property": "slug"}],
                                        "text": "d.slug != 'x'",
                                    },
                                },
                            },
                            {
                                "node": "TableScan",
                                "table": "node:Doc",
                                "binding": "e",
                                "projection": ["__id", "slug", "rank"],
                                "filter": null,
                            },
                        ],
                    }],
                }],
            },
            "passes": ["resolve", "predicate_pushdown", "projection_pushdown"],
        })
    }

    #[test]
    fn parses_every_form() {
        let body = [
            (0, "scan Doc as $d: columns [__id, slug, rank, state]"),
            (1, "scan Doc: not columns [embedding]"),
            (2, "scan Doc as $d: filter reads [d.state, d.slug]"),
            (3, "scan Doc as $e: no filter"),
            (4, "filter reads [e.rank, d.rank]"),
            (5, "pass projection_pushdown"),
            (6, "not pass aggregate_pushdown"),
        ];
        let lines = parse_plan_body(&body).unwrap();
        assert_eq!(lines.len(), 7);
        assert_eq!(check(&lines, &explain()), None);
    }

    #[test]
    fn a_read_column_fails_the_negative_form() {
        let lines = parse_plan_body(&[(0, "scan Doc: not columns [slug]")]).unwrap();
        let mismatch = check(&lines, &explain()).unwrap();
        assert!(mismatch.contains("reads `slug`"), "{mismatch}");
    }

    #[test]
    fn filter_claims_read_the_predicates() {
        let lines = parse_plan_body(&[(0, "scan Doc as $e: filter reads [e.rank]")]).unwrap();
        let mismatch = check(&lines, &explain()).unwrap();
        assert!(mismatch.contains("carries no filter"), "{mismatch}");
        let lines = parse_plan_body(&[(0, "scan Doc as $d: no filter")]).unwrap();
        let mismatch = check(&lines, &explain()).unwrap();
        assert!(mismatch.contains("carries a filter reading"), "{mismatch}");
        let lines = parse_plan_body(&[(0, "scan Doc as $d: filter reads [d.state]")]).unwrap();
        let mismatch = check(&lines, &explain()).unwrap();
        assert!(mismatch.contains("filters on"), "{mismatch}");
        let lines = parse_plan_body(&[(0, "filter reads [d.rank]")]).unwrap();
        let mismatch = check(&lines, &explain()).unwrap();
        assert!(mismatch.contains("no in-memory filter reads"), "{mismatch}");
    }

    #[test]
    fn a_fired_pass_fails_the_negated_form() {
        let lines = parse_plan_body(&[(0, "not pass predicate_pushdown")]).unwrap();
        let mismatch = check(&lines, &explain()).unwrap();
        assert!(mismatch.contains("fired"), "{mismatch}");
    }

    #[test]
    fn an_unknown_scan_or_pass_is_named() {
        let lines = parse_plan_body(&[(0, "scan Other: columns [__id]")]).unwrap();
        assert!(
            check(&lines, &explain())
                .unwrap()
                .contains("no scan of `Other`")
        );
        let lines = parse_plan_body(&[(0, "pass late_materialization")]).unwrap();
        assert!(check(&lines, &explain()).unwrap().contains("did not fire"));
    }

    #[test]
    fn refuses_a_line_outside_the_grammar() {
        assert!(parse_plan_body(&[(0, "scan Doc columns [a]")]).is_err());
        assert!(parse_plan_body(&[(0, "scan Doc: filter reads []")]).is_err());
        assert!(parse_plan_body(&[(0, "filter reads d.slug")]).is_err());
        assert!(parse_plan_body(&[(0, "expand knows: mode csr")]).is_err());
        assert!(parse_plan_body(&[]).is_err());
    }

    /// A `sort` line claims the ordered identity keys a physical `Sort` declares after its
    /// keys; a `Sort` row without a `tiebreak` key declares nothing to compare.
    #[test]
    fn sort_claims_read_the_declared_tiebreak() {
        let declared = json!({"physical_plan": {"node": "Sort", "id": 1, "tiebreak": ["$p.@id"]}});
        let bare = json!({"physical_plan": {"node": "Sort", "id": 1, "tiebreak": []}});
        let lines = parse_plan_body(&[(0, "sort tiebreak [$p]")]).unwrap();
        assert_eq!(
            lines[0],
            PlanLine::Sort {
                tiebreak: vec!["$p.@id".to_string()]
            }
        );
        assert_eq!(check(&lines, &declared), None);
        let mismatch = check(&lines, &bare).unwrap();
        assert!(
            mismatch
                .contains("no sort tie-breaks on [\"$p.@id\"]; the plan's sorts tie-break on [[]]"),
            "{mismatch}"
        );
        let lines = parse_plan_body(&[(0, "sort no tiebreak")]).unwrap();
        assert_eq!(check(&lines, &bare), None);
        let mismatch = check(&lines, &declared).unwrap();
        assert!(
            mismatch
                .contains("no sort tie-breaks on []; the plan's sorts tie-break on [[\"$p.@id\"]]"),
            "{mismatch}"
        );
        let missing = json!({"physical_plan": {"node": "Sort", "id": 1}});
        let mismatch = check(&lines, &missing).unwrap();
        assert!(
            mismatch.contains("the physical `Sort` (node 1) carries no `tiebreak` key"),
            "{mismatch}"
        );
        for refused in [
            "sort tiebreak $p",
            "sort tiebreak []",
            "sort tiebreaks [$p]",
            "sort",
        ] {
            assert!(parse_plan_body(&[(0, refused)]).is_err(), "{refused}");
        }
    }

    #[test]
    fn expand_mode_claims_read_the_physical_plan() {
        let explain = json!({
            "logical_plan": {"node": "Expand", "dst_type": "Doc", "dst": "e", "src": "d", "edges": {"kind":"named", "members":[{"edge_type": "Knows", "direction": "out"}]}},
            "physical_plan": {"node": "Projection", "inputs": [
                {"node": "Expand", "src": "d", "edges": {"kind":"named", "members":[{"edge_type": "Knows", "direction": "out"}]}, "dst": "e", "mode": "indexed_scan",
                 "frontier_estimate": 3, "inputs": [{"node": "Scan"}]}
            ]},
            "passes": ["resolve", "expand_mode"],
        });
        let lines = parse_plan_body(&[
            (0, "expand $d Knows $e: mode indexed_scan"),
            (1, "pass expand_mode"),
        ])
        .unwrap();
        assert_eq!(
            lines[0],
            PlanLine::ExpandMode {
                src: "d".to_string(),
                edge_type: Some("Knows".to_string()),
                dst: "e".to_string(),
                mode: "indexed_scan".to_string(),
                ran: None,
            }
        );
        assert_eq!(check(&lines, &explain), None);
        for (claim, message) in [
            ("expand $d Knows $e: mode csr", "runs `indexed_scan`"),
            ("expand $d Likes $e: mode csr", "no expand `$d Likes $e`"),
            ("expand $e Knows $d: mode csr", "no expand"),
        ] {
            let lines = parse_plan_body(&[(0, claim)]).unwrap();
            let mismatch = check(&lines, &explain).unwrap();
            assert!(mismatch.contains(message), "{mismatch}");
        }
        for refused in [
            "expand $d Knows $e: mode fast",
            "expand $d Knows $e: columns [__id]",
            "expand $d Knows: mode csr",
            "expand $d Knows e: mode csr",
            "expand $d Knows $e: mode csr ran",
            "expand $d Knows $e: mode csr ran fast",
            "expand $d Knows $e: mode csr took csr",
        ] {
            assert!(parse_plan_body(&[(0, refused)]).is_err(), "{refused}");
        }
    }

    /// A `ran` claim reads the last attempt of the node's row, joined by the
    /// explain row's `id`.
    #[test]
    fn ran_claims_read_the_report_by_node_id() {
        let explain = json!({
            "physical_plan": {"node": "Projection", "id": 4, "inputs": [
                {"node": "HashJoin", "id": 3, "binding": "d", "fallback": "id_lookup", "inputs": [
                    {"node": "Expand", "id": 1, "src": "s", "edges": {"kind":"named", "members":[{"edge_type": "Links", "direction": "out"}]}, "dst": "d", "mode": "indexed_scan",
                     "inputs": [{"node": "Scan", "id": 0, "table": "node:Source", "binding": "s"}]},
                    {"node": "Scan", "id": 2, "table": "node:Doc", "binding": "d"}
                ]}
            ]},
        });
        let report: Vec<Row> = serde_json::from_value(json!([
            {"id": 3, "operator": "HashJoinExec", "status": "executed",
             "attempts": [{"rung": 0, "ran": "hash_join", "actual_rows": 3}, {"rung": 1, "ran": "id_lookup", "actual_rows": 3}]},
            {"id": 2, "operator": "ScanExec", "status": "executed",
             "attempts": [{"rung": 0, "ran": true, "actual_rows": 2}]},
            {"id": 1, "operator": "ExpandExec", "status": "executed",
             "attempts": [{"rung": 0, "ran": "csr", "actual_rows": 3}]},
            {"id": 0, "operator": "ScanExec", "status": "executed",
             "attempts": [{"rung": 0, "ran": true, "actual_rows": 1}]}
        ]))
        .unwrap();
        let lines = parse_plan_body(&[
            (0, "hash join $d ran id_lookup"),
            (1, "expand $s Links $d: mode indexed_scan ran csr"),
        ])
        .unwrap();
        assert_eq!(
            lines[0],
            PlanLine::HashJoin {
                binding: "d".to_string(),
                ran: Some("id_lookup".to_string()),
            }
        );
        assert_eq!(plan_mismatch(&lines, &explain, Some(&report)), None);
        for (claim, message) in [
            (
                "hash join $d ran hash_join",
                "ran `id_lookup`, expected `ran hash_join`",
            ),
            (
                "expand $s Links $d: mode indexed_scan ran indexed_scan",
                "ran `csr`, expected `ran indexed_scan`",
            ),
        ] {
            let lines = parse_plan_body(&[(0, claim)]).unwrap();
            let mismatch = plan_mismatch(&lines, &explain, Some(&report)).unwrap();
            assert!(mismatch.contains(message), "{claim}: {mismatch}");
        }
        let lines = parse_plan_body(&[(0, "hash join $d ran hash_join")]).unwrap();
        assert!(
            plan_mismatch(&lines, &explain, None)
                .unwrap()
                .contains("needs the execution report")
        );
        let no_switch: Vec<Row> = report.iter().filter(|row| row.id != 3).cloned().collect();
        assert!(
            plan_mismatch(&lines, &explain, Some(&no_switch))
                .unwrap()
                .contains("recorded no side of a declared switch")
        );
    }

    #[test]
    fn access_and_hash_join_claims_read_the_physical_plan() {
        let explain = json!({
            "logical_plan": {"node": "TableScan", "table": "node:Doc", "binding": "d"},
            "physical_plan": {"node": "Projection", "inputs": [
                {"node": "HashJoin", "binding": "d", "fallback": "id_lookup", "inputs": [
                    {"node": "Expand", "src": "s", "edges": {"kind":"named", "members":[{"edge_type": "Links", "direction": "out"}]}, "dst": "d", "mode": "csr",
                     "inputs": [{"node": "Scan", "table": "node:Source", "binding": "s"}]},
                    {"node": "Scan", "table": "node:Doc", "binding": "d"}
                ]}
            ]},
            "passes": ["resolve", "access_path"],
        });
        let lines = parse_plan_body(&[(0, "hash join $d"), (1, "pass access_path")]).unwrap();
        assert_eq!(
            lines[0],
            PlanLine::HashJoin {
                binding: "d".to_string(),
                ran: None,
            }
        );
        assert_eq!(check(&lines, &explain), None);
        for (claim, message) in [
            (
                "scan Doc as $d: access id_lookup",
                "is the build side of a hash join",
            ),
            (
                "scan Source as $s: access id_lookup",
                "table scan with no access path",
            ),
            ("hash join $e", "no hash join over `$e`"),
            ("scan Other: access id_lookup", "no scan of `Other`"),
        ] {
            let lines = parse_plan_body(&[(0, claim)]).unwrap();
            let mismatch = check(&lines, &explain).unwrap();
            assert!(mismatch.contains(message), "{mismatch}");
        }
        let looked_up = json!({
            "physical_plan": {"node": "Projection", "inputs": [
                {"node": "Scan", "table": "node:Doc", "binding": "d", "id_restriction": "input",
                 "access": "id_lookup", "inputs": [
                    {"node": "Expand", "src": "s", "edges": {"kind":"named", "members":[{"edge_type": "Links", "direction": "out"}]}, "dst": "d", "mode": "csr",
                     "inputs": [{"node": "Scan", "table": "node:Source", "binding": "s"}]}
                ]}
            ]},
        });
        let lines = parse_plan_body(&[(0, "scan Doc as $d: access id_lookup")]).unwrap();
        assert_eq!(
            lines[0],
            PlanLine::ScanAccess {
                type_name: "Doc".to_string(),
                binding: Some("d".to_string()),
            }
        );
        assert_eq!(check(&lines, &looked_up), None);
        let lines = parse_plan_body(&[(0, "hash join $d")]).unwrap();
        assert!(
            check(&lines, &looked_up)
                .unwrap()
                .contains("no hash join over `$d`")
        );
        for refused in [
            "scan Doc as $d: access fast",
            "scan Doc as $d: access",
            "scan Doc as $d: access hash_join",
            "scan Doc as $d: access id_lookup ran id_lookup",
            "hash join d",
            "hash join $d ran",
            "hash join $d ran csr",
            "hash join $d took hash_join",
        ] {
            assert!(parse_plan_body(&[(0, refused)]).is_err(), "{refused}");
        }
    }

    #[test]
    fn scalar_access_claims_require_matching_typed_leaves_on_every_scan() {
        let leaf = json!({"kind":"search","index":"by_age","column":"age","search":"age = 1"});
        let scan = json!({"node":"Scan","table":"node:Doc","binding":"d","access":"index_probe",
            "index_query":{"kind":"not","input":{"kind":"or","left":leaf.clone(),"right":leaf}},
            "residual":"(active = true)"});
        let claims = parse_plan_body(&[(0, "scan Doc as $d: access index_probe age")]).unwrap();
        let explain = json!({"physical_plan":scan});
        assert_eq!(check(&claims, &explain), None);
        for bad in [
            json!(null),
            json!({"kind":"unknown"}),
            json!({"kind":"search","index":"by_title","column":"title","search":"title = age"}),
        ] {
            let mut explain = explain.clone();
            explain["physical_plan"]["index_query"] = bad;
            assert!(check(&claims, &explain).is_some());
        }
        let mut sequential = explain["physical_plan"].clone();
        sequential["access"] = json!("sequential");
        sequential.as_object_mut().unwrap().remove("index_query");
        let both = json!({"physical_plan":{"node":"CrossJoin","inputs":[explain["physical_plan"],sequential.clone()]}});
        assert!(check(&claims, &both).is_some());
        let claims = parse_plan_body(&[(0, "scan Doc as $d: access sequential")]).unwrap();
        assert_eq!(check(&claims, &json!({"physical_plan":sequential})), None);
        for refused in [
            "scan Doc: access index_probe",
            "scan Doc: access sequential age",
            "scan Doc: access index_probe age extra",
        ] {
            assert!(parse_plan_body(&[(0, refused)]).is_err());
        }
    }

    /// A `hydrate` line reads the physical `HydrateColumns`: the exact set
    /// of one binding's hydrated columns, in any order.
    #[test]
    fn hydrate_claims_read_the_physical_hydration() {
        let explain = json!({
            "physical_plan": {"node": "HydrateColumns", "id": 4, "bindings": [
                {"binding": "d", "table": "node:Doc", "columns": ["title", "body"]}
            ], "inputs": [{"node": "Page", "id": 3, "inputs": []}]},
            "passes": ["resolve", "late_materialization"],
        });
        let lines = parse_plan_body(&[(0, "hydrate $d: columns [body, title]")]).unwrap();
        assert_eq!(check(&lines, &explain), None);
        for (claim, want) in [
            ("hydrate $d: columns [body]", "no `HydrateColumns` fetches"),
            ("hydrate $e: columns [body, title]", "of `$e`"),
        ] {
            let lines = parse_plan_body(&[(0, claim)]).unwrap();
            let mismatch = check(&lines, &explain).unwrap();
            assert!(mismatch.contains(want), "{claim}: {mismatch}");
        }
        for refused in [
            "hydrate d: columns [body]",
            "hydrate $d columns [body]",
            "hydrate $d: [body]",
            "hydrate $d: columns []",
        ] {
            assert!(parse_plan_body(&[(0, refused)]).is_err(), "{refused}");
        }
    }

    /// A `contains join` line reads the physical `ContainsJoin`'s columns
    /// and a `runtime filter` claim the marked scan's column; the collected
    /// side is unmarked, and a `cross join` line the plain product's filters.
    #[test]
    fn contains_join_and_runtime_filter_claims_read_the_physical_plan() {
        let explain = json!({
            "physical_plan": {"node": "Projection", "inputs": [
                {"node": "ContainsJoin", "id": 3, "haystack": "$p.text", "needle": "$m.number",
                 "residual": [], "inputs": [
                    {"node": "Scan", "id": 1, "table": "node:Matter", "binding": "m"},
                    {"node": "Scan", "id": 2, "table": "node:Passage", "binding": "p",
                     "runtime_filter": {"column": "text", "needle": ["m", "number"],
                                        "kind": "text_contains_any"}}
                ]}
            ]},
            "passes": ["resolve", "join_algorithm"],
        });
        let lines = parse_plan_body(&[
            (0, "contains join $p.text contains $m.number"),
            (1, "scan Passage as $p: runtime filter text"),
            (2, "scan Matter as $m: no runtime filter"),
            (3, "pass join_algorithm"),
        ])
        .unwrap();
        assert_eq!(
            lines[0],
            PlanLine::ContainsJoin {
                haystack: "p.text".to_string(),
                needle: "m.number".to_string(),
            }
        );
        assert_eq!(
            lines[1],
            PlanLine::ScanRuntimeFilter {
                type_name: "Passage".to_string(),
                binding: Some("p".to_string()),
                column: Some("text".to_string()),
            }
        );
        assert_eq!(
            lines[2],
            PlanLine::ScanRuntimeFilter {
                type_name: "Matter".to_string(),
                binding: Some("m".to_string()),
                column: None,
            }
        );
        assert_eq!(check(&lines, &explain), None);
        for (claim, message) in [
            (
                "contains join $m.number contains $p.text",
                "no contains join `$m.number contains $p.text`",
            ),
            (
                "scan Matter as $m: runtime filter number",
                "carries no runtime filter, expected one on `number`",
            ),
            (
                "scan Passage as $p: no runtime filter",
                "carries a runtime filter on `text`, expected none",
            ),
            (
                "scan Passage as $p: runtime filter pid",
                "carries a runtime filter on `text`, expected `pid`",
            ),
            ("scan Other: runtime filter text", "no scan of `Other`"),
            (
                "cross join $p.text contains $m.number",
                "no cross join holding `$p.text contains $m.number` in the physical plan; its cross joins hold []",
            ),
        ] {
            let lines = parse_plan_body(&[(0, claim)]).unwrap();
            let mismatch = check(&lines, &explain).unwrap();
            assert!(mismatch.contains(message), "{claim}: {mismatch}");
        }
        let plain = json!({
            "physical_plan": {"node": "CrossJoin", "filters": ["$p.text contains $m.number"], "inputs": [
                {"node": "Scan", "table": "node:Matter", "binding": "m"},
                {"node": "Scan", "table": "node:Passage", "binding": "p"}
            ]},
        });
        let lines = parse_plan_body(&[(0, "contains join $p.text contains $m.number")]).unwrap();
        assert!(
            check(&lines, &plain)
                .unwrap()
                .contains("in the physical plan; it joins []")
        );
        let lines = parse_plan_body(&[
            (0, "cross join $p.text contains $m.number"),
            (1, "scan Passage as $p: no runtime filter"),
        ])
        .unwrap();
        assert_eq!(
            lines[0],
            PlanLine::CrossJoin {
                haystack: "p.text".to_string(),
                needle: "m.number".to_string(),
            }
        );
        assert_eq!(check(&lines, &plain), None);
        let lines = parse_plan_body(&[(0, "cross join $m.number contains $p.text")]).unwrap();
        assert!(
            check(&lines, &plain)
                .unwrap()
                .contains(r#"its cross joins hold [["$p.text contains $m.number"]]"#)
        );
        for refused in [
            "contains join p.text contains $m.number",
            "contains join $p.text $m.number",
            "contains join $p.text contains $m",
            "contains join $p.text contains $m.number extra",
            "cross join $p.text $m.number",
            "cross join p.text contains $m.number",
            "scan Passage as $p: runtime filter",
            "scan Passage as $p: runtime filter p.text",
            "scan Passage as $p: runtime filter text more",
        ] {
            assert!(parse_plan_body(&[(0, refused)]).is_err(), "{refused}");
        }
    }

    #[test]
    fn scan_ranked_claims_read_the_physical_plan() {
        let explain = json!({
            "physical_plan": {"node": "Page", "inputs": [{"node": "Sort", "inputs": [
                {"node": "Projection", "inputs": [
                    {"node": "Scan", "table": "node:Doc", "binding": "d",
                     "ranked": {"kind": "nearest", "property": "embedding", "query": "$q",
                                "fetch": 10, "nprobes": 20, "scope": "order"}}
                ]}
            ]}]},
        });
        let lines = parse_plan_body(&[
            (0, "scan Doc as $d: ranked nearest fetch 10"),
            (1, "scan Doc: ranked nearest"),
            (2, "scan Doc: ranked nearest fetch 10 nprobes 20"),
        ])
        .unwrap();
        assert_eq!(
            lines[0],
            PlanLine::ScanRanked {
                type_name: "Doc".to_string(),
                binding: Some("d".to_string()),
                index: "nearest".to_string(),
                fetch: Some(10),
                nprobes: None,
            }
        );
        assert_eq!(lines[2].clone(), {
            let mut capped = lines[0].clone();
            if let PlanLine::ScanRanked {
                binding, nprobes, ..
            } = &mut capped
            {
                *binding = None;
                *nprobes = Some(20);
            }
            capped
        });
        assert_eq!(check(&lines, &explain), None);
        for (claim, message) in [
            ("scan Doc as $d: ranked bm25", "is ranked by [\"nearest\"]"),
            (
                "scan Doc as $d: ranked nearest fetch 3",
                "fetches 10, expected 3",
            ),
            (
                "scan Doc as $d: ranked nearest fetch 10 nprobes 1",
                "probes 20, expected nprobes 1",
            ),
            (
                "scan Doc as $e: ranked nearest",
                "no scan of `Doc` bound to `$e`",
            ),
        ] {
            let lines = parse_plan_body(&[(0, claim)]).unwrap();
            let mismatch = check(&lines, &explain).unwrap();
            assert!(mismatch.contains(message), "{claim}: {mismatch}");
        }
        let unranked =
            json!({"physical_plan": {"node": "Scan", "table": "node:Doc", "binding": "d"}});
        let lines = parse_plan_body(&[(0, "scan Doc as $d: ranked bm25")]).unwrap();
        assert!(check(&lines, &unranked).unwrap().contains("is not ranked"));
        let uncapped = json!({"physical_plan": {"node": "Scan", "table": "node:Doc", "binding": "d",
            "ranked": {"kind": "bm25", "fetch": null}}});
        let lines = parse_plan_body(&[(0, "scan Doc as $d: ranked bm25 fetch 10")]).unwrap();
        assert!(check(&lines, &uncapped).unwrap().contains("has no fetch"));
        let no_cap = json!({"physical_plan": {"node": "Scan", "table": "node:Doc", "binding": "d",
            "ranked": {"kind": "nearest", "fetch": 10, "nprobes": null}}});
        let lines =
            parse_plan_body(&[(0, "scan Doc as $d: ranked nearest fetch 10 nprobes 0")]).unwrap();
        assert_eq!(check(&lines, &no_cap), None, "0 spells no cap");
        let fused = json!({"physical_plan": {"node": "RankFuse", "inputs": [
            {"node": "Scan", "table": "node:Doc", "binding": "d",
             "ranked": {"kind": "nearest", "fetch": 3, "scope": "primary"}},
            {"node": "Scan", "table": "node:Doc", "binding": "d",
             "ranked": {"kind": "bm25", "fetch": null, "scope": "secondary"}}
        ]}});
        let lines = parse_plan_body(&[
            (0, "scan Doc as $d: ranked nearest fetch 3"),
            (1, "scan Doc as $d: ranked bm25"),
        ])
        .unwrap();
        assert_eq!(check(&lines, &fused), None, "one scan per arm");
        let lines = parse_plan_body(&[(0, "scan Doc as $d: ranked nearest fetch 4")]).unwrap();
        assert!(
            check(&lines, &fused)
                .unwrap()
                .contains("fetches 3, expected 4")
        );
        let lines = parse_plan_body(&[(0, "scan Doc as $d: ranked nearest nprobes 20")]).unwrap();
        assert_eq!(
            lines[0],
            PlanLine::ScanRanked {
                type_name: "Doc".to_string(),
                binding: Some("d".to_string()),
                index: "nearest".to_string(),
                fetch: None,
                nprobes: Some(20),
            }
        );
        assert_eq!(
            check(&lines, &explain),
            None,
            "`fetch` is optional beside `nprobes`"
        );
        for refused in [
            "scan Doc as $d: ranked",
            "scan Doc as $d: ranked fuzzy",
            "scan Doc as $d: ranked bm25 fetch",
            "scan Doc as $d: ranked bm25 fetch ten",
            "scan Doc as $d: ranked bm25 limit 10",
            "scan Doc as $d: ranked nearest nprobes 4 fetch 10",
            "scan Doc as $d: ranked nearest nprobes 4 nprobes 4",
            "scan Doc as $d: ranked nearest fetch 10 nprobes",
        ] {
            assert!(parse_plan_body(&[(0, refused)]).is_err(), "{refused}");
        }
    }

    #[test]
    fn refuses_unknown_passes_and_malformed_selectors() {
        for line in [
            "not pass misspelled",
            "pass misspelled",
            "not pass limit_pushdown",
            "scan Doc as $: columns [slug]",
            "scan Doc: columns [a b]",
            "scan Doc: columns [a,,b]",
            "scan Doc: columns [a,]",
            "expand Doc: not columns [embedding]",
            "expand $ Knows $e: mode csr",
        ] {
            assert!(parse_plan_body(&[(0, line)]).is_err(), "{line}");
        }
    }

    #[test]
    fn negative_columns_require_a_catalog_field() {
        let schema = omnigraph_compiler::schema::parser::parse_schema(
            "node Doc { slug: String @key embedding: Vector(4) }",
        )
        .unwrap();
        let catalog = omnigraph_compiler::catalog::build_catalog(&schema).unwrap();
        for column in ["missing", "Embedding", "d.embedding"] {
            let line = format!("scan Doc: not columns [{column}]");
            let lines = parse_plan_body(&[(0, &line)]).unwrap();
            assert!(validate_plan_columns(&lines, &catalog).is_err(), "{line}");
        }
        let lines = parse_plan_body(&[(0, "scan Doc: not columns [embedding]")]).unwrap();
        assert!(validate_plan_columns(&lines, &catalog).is_ok());
    }

    #[test]
    fn duplicate_expand_selectors_check_every_mode() {
        let lines = parse_plan_body(&[(0, "expand $d Knows $e: mode csr")]).unwrap();
        let explain = json!({"physical_plan": {"node": "AntiJoin", "inputs": [
            {"node":"Expand", "src":"d", "edges":{"kind":"named","members":[{"edge_type":"Knows","direction":"out"}]}, "dst":"e", "mode":"csr"},
            {"node":"Expand", "src":"d", "edges":{"kind":"named","members":[{"edge_type":"Knows","direction":"out"}]}, "dst":"e", "mode":"indexed_scan"}
        ]}});
        assert!(check(&lines, &explain).unwrap().contains("indexed_scan"));
    }

    #[test]
    fn predicates_fail_closed_on_unknown_or_malformed_shapes() {
        let lines = parse_plan_body(&[(0, "scan Doc: no filter")]).unwrap();
        for filter in [
            json!({"kind":"future"}),
            json!({"kind":"gq"}),
            json!({"kind":"and", "left":{"kind":"gq", "reads":[]}}),
            json!({"kind":"gq", "reads":[{"binding":"d"}]}),
        ] {
            let explain =
                json!({"logical_plan": {"node":"TableScan", "table":"node:Doc", "filter":filter}});
            assert!(
                check(&lines, &explain)
                    .unwrap()
                    .contains("malformed predicate")
            );
        }
    }
}
