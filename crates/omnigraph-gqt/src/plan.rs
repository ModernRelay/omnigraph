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
//! filter reads [a.state, b.state]
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
use serde::Serialize;
use serde_json::Value;

use crate::report::Row;

const FORMS: &str = "forms: `scan <Type>[ as $var]: columns [a, b]`, `scan <Type>[ as $var]: not columns [a, b]`, `scan <Type>[ as $var]: filter reads [v.a]`, `scan <Type>[ as $var]: no filter`, `scan <Type>[ as $var]: access id_lookup`, `hash join $var[ ran <hash_join|id_lookup>]`, `scan <Type>[ as $var]: ranked <nearest|bm25>[ fetch <n>][ nprobes <n>]`, `expand $src <Edge> $dst: mode <csr|indexed_scan>[ ran <csr|indexed_scan>]`, `filter reads [a.x, b.y]`, `sort tiebreak [$a, $b]`, `sort no tiebreak`, `pass <name>`, `not pass <name>`";

const ID_LOOKUP: &str = "id_lookup";
const JOIN_SIDES: [&str; 2] = ["hash_join", "id_lookup"];
const EXPAND_MODES: [&str; 2] = ["csr", "indexed_scan"];

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
    /// The physical `Expand` from `$src` over `edge_type` to `$dst` runs in
    /// `mode` (`csr` or `indexed_scan`), and when `ran` is claimed, the run
    /// ended on that mode.
    ExpandMode {
        src: String,
        edge_type: String,
        dst: String,
        mode: String,
        ran: Option<String>,
    },
    /// An in-memory `Filter` node stays in the plan reading exactly `reads`.
    Filter { reads: Vec<String> },
    /// A physical `Sort` declares exactly the ids of `tiebreak` after its
    /// keys, none when empty.
    Sort { tiebreak: Vec<String> },
    /// The optimizer pass `name` fired, or did not when `negated`.
    Pass { name: String, negated: bool },
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
        if let Some(list) = line.strip_prefix("filter reads") {
            let reads = column_list(list).ok_or_else(|| refused("lists reads in `[...]`"))?;
            lines.push(PlanLine::Filter { reads });
            continue;
        }
        if let Some(claim) = line.strip_prefix("sort ") {
            let claim = claim.trim();
            let tiebreak = if claim == "no tiebreak" {
                Vec::new()
            } else {
                claim
                    .strip_prefix("tiebreak")
                    .and_then(binding_list)
                    .ok_or_else(|| refused("claims `tiebreak [$a, $b]` or `no tiebreak`"))?
            };
            lines.push(PlanLine::Sort { tiebreak });
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
        let (rest, expand) = if let Some(rest) = line.strip_prefix("scan ") {
            (rest, false)
        } else if let Some(rest) = line.strip_prefix("expand ") {
            (rest, true)
        } else {
            return Err(refused(
                "knows six line heads: `scan`, `hash join`, `expand`, `filter`, `sort`, `pass`",
            ));
        };
        let Some((selector, claim)) = rest.split_once(':') else {
            return Err(refused("separates the scan from its claim with `:`"));
        };
        if expand {
            let ends: Vec<&str> = selector.split_whitespace().collect();
            let [src, edge_type, dst] = ends[..] else {
                return Err(refused("spells the traversal `$src <Edge> $dst`"));
            };
            let (Some(src), Some(dst)) = (src.strip_prefix('$'), dst.strip_prefix('$')) else {
                return Err(refused("names both bindings with `$`"));
            };
            if !identifier(src) || !identifier(dst) || !identifier(edge_type) {
                return Err(refused("names nonempty traversal bindings and edge type"));
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
                edge_type: edge_type.to_string(),
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
            if access.trim() != ID_LOOKUP {
                return Err(refused(
                    "claims `access id_lookup`; a destination read once as a build side is `hash join $var`",
                ));
            }
            lines.push(PlanLine::ScanAccess { type_name, binding });
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
                            "claims `columns`, `not columns`, `filter reads`, `no filter`, `access` or `ranked`",
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

/// A non-empty `[a, b]` list, or `None` when the text is not one.
fn identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

/// A non-empty `[$a, $b]` list of bindings, or `None` when the text is not one.
fn binding_list(text: &str) -> Option<Vec<String>> {
    bracket_list(text, |part| {
        let binding = part.strip_prefix('$')?;
        identifier(binding).then(|| binding.to_string())
    })
}

/// The binding of a `Sort` row's tie-break key: `$p.@id` names `p`.
fn tiebreak_binding(key: &str) -> String {
    key.strip_prefix('$')
        .unwrap_or(key)
        .split('.')
        .next()
        .unwrap_or_default()
        .to_string()
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
    edge_type: String,
    dst: String,
    mode: String,
}

/// One `Scan` of the explain document's physical plan with the access path
/// it recorded (`None` on a table scan) and its ranking (`None` unranked).
#[derive(Debug)]
struct PlannedAccess {
    type_key: String,
    binding: Option<String>,
    access: Option<String>,
    ranked: Option<PlannedRanking>,
}

/// One `HashJoin` of the explain document's physical plan with its node id.
#[derive(Debug)]
struct PlannedJoin {
    id: Option<u64>,
    binding: String,
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
/// traversal modes, access paths, hash joins and sort tie-breaks (`Err` names
/// a `Sort` row with no `tiebreak` key).
#[derive(Debug, Default)]
struct PlannedNodes {
    scans: Vec<PlannedScan>,
    filters: Vec<Vec<String>>,
    modes: Vec<PlannedExpandMode>,
    accesses: Vec<PlannedAccess>,
    joins: Vec<PlannedJoin>,
    sorts: Vec<Result<Vec<String>, String>>,
}

fn planned_physical(node: &Value, out: &mut PlannedNodes) {
    let text = |key: &str| {
        node.get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let id = node.get("id").and_then(Value::as_u64);
    match node.get("node").and_then(Value::as_str) {
        Some("Expand") => out.modes.push(PlannedExpandMode {
            id,
            src: text("src"),
            edge_type: text("edge_type"),
            dst: text("dst"),
            mode: text("mode"),
        }),
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
        }),
        Some("HashJoin") => out.joins.push(PlannedJoin {
            id,
            binding: text("binding"),
        }),
        Some("Sort") => out.sorts.push(
            node.get("tiebreak")
                .and_then(Value::as_array)
                .map(|keys| {
                    sorted_set(
                        keys.iter()
                            .filter_map(Value::as_str)
                            .map(tiebreak_binding)
                            .collect(),
                    )
                })
                .ok_or_else(|| {
                    let node = id.map(|id| format!(" (node {id})")).unwrap_or_default();
                    format!("expect plan: the physical `Sort`{node} carries no `tiebreak` key")
                }),
        ),
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
            PlanLine::Sort { tiebreak } => {
                if nodes.sorts.is_empty() {
                    return Some("expect plan: the physical plan has no `Sort`".to_string());
                }
                let declared = match nodes.sorts.iter().cloned().collect::<Result<Vec<_>, _>>() {
                    Ok(declared) => declared,
                    Err(missing) => return Some(missing),
                };
                let want = sorted_set(tiebreak.clone());
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
                        expand.src == *src && expand.edge_type == *edge_type && expand.dst == *dst
                    })
                    .collect();
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

    /// A `sort` line claims the ids a physical `Sort` declares after its
    /// keys; a `Sort` row without a `tiebreak` key declares nothing to compare.
    #[test]
    fn sort_claims_read_the_declared_tiebreak() {
        let declared = json!({"physical_plan": {"node": "Sort", "id": 1, "tiebreak": ["$p.@id"]}});
        let bare = json!({"physical_plan": {"node": "Sort", "id": 1, "tiebreak": []}});
        let lines = parse_plan_body(&[(0, "sort tiebreak [$p]")]).unwrap();
        assert_eq!(
            lines[0],
            PlanLine::Sort {
                tiebreak: vec!["p".to_string()]
            }
        );
        assert_eq!(check(&lines, &declared), None);
        let mismatch = check(&lines, &bare).unwrap();
        assert!(
            mismatch.contains("no sort tie-breaks on [\"p\"]; the plan's sorts tie-break on [[]]"),
            "{mismatch}"
        );
        let lines = parse_plan_body(&[(0, "sort no tiebreak")]).unwrap();
        assert_eq!(check(&lines, &bare), None);
        let mismatch = check(&lines, &declared).unwrap();
        assert!(
            mismatch.contains("no sort tie-breaks on []; the plan's sorts tie-break on [[\"p\"]]"),
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
            "logical_plan": {"node": "Expand", "dst_type": "Doc", "dst": "e", "src": "d", "edge_type": "Knows"},
            "physical_plan": {"node": "Projection", "inputs": [
                {"node": "Expand", "src": "d", "edge_type": "Knows", "dst": "e", "mode": "indexed_scan",
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
                edge_type: "Knows".to_string(),
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
                    {"node": "Expand", "id": 1, "src": "s", "edge_type": "Links", "dst": "d", "mode": "indexed_scan",
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
                    {"node": "Expand", "src": "s", "edge_type": "Links", "dst": "d", "mode": "csr",
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
                    {"node": "Expand", "src": "s", "edge_type": "Links", "dst": "d", "mode": "csr",
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
            {"node":"Expand", "src":"d", "edge_type":"Knows", "dst":"e", "mode":"csr"},
            {"node":"Expand", "src":"d", "edge_type":"Knows", "dst":"e", "mode":"indexed_scan"}
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
