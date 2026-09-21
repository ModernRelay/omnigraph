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
//! scan Doc as $d: access hash_join
//! expand $d Knows $e: mode indexed_scan
//! filter reads [a.state, b.state]
//! pass projection_pushdown
//! not pass aggregate_pushdown
//! ```

use omnigraph_compiler::catalog::Catalog;
use omnigraph_planner::optimizer::{
    PASS_ACCESS_PATH, PASS_ADDRESS_SHORT_CIRCUIT, PASS_AGGREGATE_PUSHDOWN, PASS_EXPAND_MODE,
    PASS_FRAGMENT_SCOPE, PASS_JOIN_ALGORITHM, PASS_LATE_MATERIALIZATION, PASS_PREDICATE_PUSHDOWN,
    PASS_PROJECTION_PUSHDOWN, PASS_RESOLVE,
};
use serde::Serialize;
use serde_json::Value;

const FORMS: &str = "forms: `scan <Type>[ as $var]: columns [a, b]`, `scan <Type>[ as $var]: not columns [a, b]`, `scan <Type>[ as $var]: filter reads [v.a]`, `scan <Type>[ as $var]: no filter`, `scan <Type>[ as $var]: access <id_lookup|hash_join>`, `expand $src <Edge> $dst: mode <csr|indexed_scan>`, `filter reads [a.x, b.y]`, `pass <name>`, `not pass <name>`";

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
    /// A physical scan reaches destination rows through the named access path.
    ScanAccess {
        type_name: String,
        binding: Option<String>,
        access: String,
    },
    /// The physical `Expand` from `$src` over `edge_type` to `$dst` runs in
    /// `mode` (`csr` or `indexed_scan`).
    ExpandMode {
        src: String,
        edge_type: String,
        dst: String,
        mode: String,
    },
    /// An in-memory `Filter` node stays in the plan reading exactly `reads`.
    Filter { reads: Vec<String> },
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
        let (rest, expand) = if let Some(rest) = line.strip_prefix("scan ") {
            (rest, false)
        } else if let Some(rest) = line.strip_prefix("expand ") {
            (rest, true)
        } else {
            return Err(refused(
                "knows four line heads: `scan`, `expand`, `filter`, `pass`",
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
            let mode = claim
                .trim()
                .strip_prefix("mode ")
                .map(str::trim)
                .filter(|mode| ["csr", "indexed_scan"].contains(mode))
                .ok_or_else(|| refused("claims `mode csr` or `mode indexed_scan`"))?;
            lines.push(PlanLine::ExpandMode {
                src: src.to_string(),
                edge_type: edge_type.to_string(),
                dst: dst.to_string(),
                mode: mode.to_string(),
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
        } else if let Some(access) = claim.strip_prefix("access ") {
            let access = access.trim();
            if !["id_lookup", "hash_join"].contains(&access) {
                return Err(refused("claims `access id_lookup` or `access hash_join`"));
            }
            lines.push(PlanLine::ScanAccess {
                type_name,
                binding,
                access: access.to_string(),
            });
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
                            "claims `columns`, `not columns`, `filter reads`, `no filter` or `access`",
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

fn column_list(text: &str) -> Option<Vec<String>> {
    let inner = text.trim().strip_prefix('[')?.strip_suffix(']')?;
    inner
        .split(',')
        .map(|part| {
            let column = part.trim();
            column
                .split('.')
                .all(identifier)
                .then(|| column.to_string())
        })
        .collect()
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
    src: String,
    edge_type: String,
    dst: String,
    mode: String,
}

/// One `Scan` of the explain document's physical plan with the access path
/// it recorded (`None` on a table scan).
#[derive(Debug)]
struct PlannedAccess {
    type_key: String,
    binding: Option<String>,
    access: Option<String>,
}

/// The scans and the in-memory filters (each as its sorted reads) of the
/// logical plan, and the traversal modes and scan access paths of the
/// physical plan.
#[derive(Debug, Default)]
struct PlannedNodes {
    scans: Vec<PlannedScan>,
    filters: Vec<Vec<String>>,
    modes: Vec<PlannedExpandMode>,
    accesses: Vec<PlannedAccess>,
}

fn planned_physical(node: &Value, out: &mut PlannedNodes) {
    let text = |key: &str| {
        node.get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    match node.get("node").and_then(Value::as_str) {
        Some("Expand") => out.modes.push(PlannedExpandMode {
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
        }),
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
            let predicate = node
                .get("predicate")
                .ok_or_else(|| "expect plan: Filter has no predicate".to_string())?;
            out.filters.push(predicate_reads(predicate)?);
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
/// document holds instead.
pub(crate) fn plan_mismatch(lines: &[PlanLine], explain: &Value) -> Option<String> {
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
            PlanLine::ExpandMode {
                src,
                edge_type,
                dst,
                mode,
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
                }
            }
            PlanLine::ScanAccess {
                type_name,
                binding,
                access,
            } => {
                let type_key = format!("node:{type_name}");
                let selected: Vec<&PlannedAccess> = nodes
                    .accesses
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
                    return Some(format!(
                        "expect plan: no scan of `{type_name}`{} in the physical plan; it scans {known:?}",
                        binding
                            .as_ref()
                            .map(|b| format!(" bound to `${b}`"))
                            .unwrap_or_default()
                    ));
                }
                for scan in selected {
                    match &scan.access {
                        None => {
                            return Some(format!(
                                "expect plan: the scan of `{type_name}` is a table scan with no access path, expected `{access}`"
                            ));
                        }
                        Some(have) if have != access => {
                            return Some(format!(
                                "expect plan: the scan of `{type_name}` reaches its rows by `{have}`, expected `{access}`"
                            ));
                        }
                        Some(_) => {}
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

    fn explain() -> Value {
        json!({
            "logical_plan": {
                "node": "Aggregate",
                "inputs": [{
                    "node": "Filter",
                    "predicate": {
                        "kind": "gq",
                        "reads": [
                            {"binding": "d", "property": "rank"},
                            {"binding": "e", "property": "rank"},
                        ],
                        "text": "d.rank = e.rank",
                    },
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
        assert_eq!(plan_mismatch(&lines, &explain()), None);
    }

    #[test]
    fn a_read_column_fails_the_negative_form() {
        let lines = parse_plan_body(&[(0, "scan Doc: not columns [slug]")]).unwrap();
        let mismatch = plan_mismatch(&lines, &explain()).unwrap();
        assert!(mismatch.contains("reads `slug`"), "{mismatch}");
    }

    #[test]
    fn filter_claims_read_the_predicates() {
        let lines = parse_plan_body(&[(0, "scan Doc as $e: filter reads [e.rank]")]).unwrap();
        let mismatch = plan_mismatch(&lines, &explain()).unwrap();
        assert!(mismatch.contains("carries no filter"), "{mismatch}");
        let lines = parse_plan_body(&[(0, "scan Doc as $d: no filter")]).unwrap();
        let mismatch = plan_mismatch(&lines, &explain()).unwrap();
        assert!(mismatch.contains("carries a filter reading"), "{mismatch}");
        let lines = parse_plan_body(&[(0, "scan Doc as $d: filter reads [d.state]")]).unwrap();
        let mismatch = plan_mismatch(&lines, &explain()).unwrap();
        assert!(mismatch.contains("filters on"), "{mismatch}");
        let lines = parse_plan_body(&[(0, "filter reads [d.rank]")]).unwrap();
        let mismatch = plan_mismatch(&lines, &explain()).unwrap();
        assert!(mismatch.contains("no in-memory filter reads"), "{mismatch}");
    }

    #[test]
    fn a_fired_pass_fails_the_negated_form() {
        let lines = parse_plan_body(&[(0, "not pass predicate_pushdown")]).unwrap();
        let mismatch = plan_mismatch(&lines, &explain()).unwrap();
        assert!(mismatch.contains("fired"), "{mismatch}");
    }

    #[test]
    fn an_unknown_scan_or_pass_is_named() {
        let lines = parse_plan_body(&[(0, "scan Other: columns [__id]")]).unwrap();
        assert!(
            plan_mismatch(&lines, &explain())
                .unwrap()
                .contains("no scan of `Other`")
        );
        let lines = parse_plan_body(&[(0, "pass late_materialization")]).unwrap();
        assert!(
            plan_mismatch(&lines, &explain())
                .unwrap()
                .contains("did not fire")
        );
    }

    #[test]
    fn refuses_a_line_outside_the_grammar() {
        assert!(parse_plan_body(&[(0, "scan Doc columns [a]")]).is_err());
        assert!(parse_plan_body(&[(0, "scan Doc: filter reads []")]).is_err());
        assert!(parse_plan_body(&[(0, "filter reads d.slug")]).is_err());
        assert!(parse_plan_body(&[(0, "expand knows: mode csr")]).is_err());
        assert!(parse_plan_body(&[]).is_err());
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
            }
        );
        assert_eq!(plan_mismatch(&lines, &explain), None);
        for (claim, message) in [
            ("expand $d Knows $e: mode csr", "runs `indexed_scan`"),
            ("expand $d Likes $e: mode csr", "no expand `$d Likes $e`"),
            ("expand $e Knows $d: mode csr", "no expand"),
        ] {
            let lines = parse_plan_body(&[(0, claim)]).unwrap();
            let mismatch = plan_mismatch(&lines, &explain).unwrap();
            assert!(mismatch.contains(message), "{mismatch}");
        }
        for refused in [
            "expand $d Knows $e: mode fast",
            "expand $d Knows $e: columns [__id]",
            "expand $d Knows: mode csr",
            "expand $d Knows e: mode csr",
        ] {
            assert!(parse_plan_body(&[(0, refused)]).is_err(), "{refused}");
        }
    }

    #[test]
    fn scan_access_claims_read_the_physical_plan() {
        let explain = json!({
            "logical_plan": {"node": "TableScan", "table": "node:Doc", "binding": "d"},
            "physical_plan": {"node": "Projection", "inputs": [
                {"node": "Scan", "table": "node:Doc", "binding": "d", "id_restriction": "input",
                 "access": "hash_join", "inputs": [
                    {"node": "Expand", "src": "s", "edge_type": "Links", "dst": "d", "mode": "csr",
                     "inputs": [{"node": "Scan", "table": "node:Source", "binding": "s"}]}
                ]}
            ]},
            "passes": ["resolve", "access_path"],
        });
        let lines = parse_plan_body(&[
            (0, "scan Doc as $d: access hash_join"),
            (1, "scan Doc: access hash_join"),
            (2, "pass access_path"),
        ])
        .unwrap();
        assert_eq!(
            lines[0],
            PlanLine::ScanAccess {
                type_name: "Doc".to_string(),
                binding: Some("d".to_string()),
                access: "hash_join".to_string(),
            }
        );
        assert_eq!(plan_mismatch(&lines, &explain), None);
        for (claim, message) in [
            (
                "scan Doc as $d: access id_lookup",
                "reaches its rows by `hash_join`",
            ),
            (
                "scan Source as $s: access id_lookup",
                "table scan with no access path",
            ),
            (
                "scan Doc as $e: access hash_join",
                "no scan of `Doc` bound to `$e`",
            ),
            ("scan Other: access hash_join", "no scan of `Other`"),
        ] {
            let lines = parse_plan_body(&[(0, claim)]).unwrap();
            let mismatch = plan_mismatch(&lines, &explain).unwrap();
            assert!(mismatch.contains(message), "{mismatch}");
        }
        for refused in ["scan Doc as $d: access fast", "scan Doc as $d: access"] {
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
        assert!(
            plan_mismatch(&lines, &explain)
                .unwrap()
                .contains("indexed_scan")
        );
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
                plan_mismatch(&lines, &explain)
                    .unwrap()
                    .contains("malformed predicate")
            );
        }
    }
}
