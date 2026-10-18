//! Decision instrument for a correlated block's strategy (`key_set` against
//! `correlated`): the rows every operator emits, those of the block's inner
//! tree, and the wall time of one channel lookalike read with and without its
//! `not { }` exclusion. The block is
//! forced correlated by a conjunct that reads `$x` and always holds, so one
//! binary measures both strategies over the same snapshot. Each read opens
//! the graph cold.
//!
//! By default the graph is synthetic and shaped like a CRM channel: a channel
//! watching 140 sources, 2,000 identities that each star two of them and
//! touch 15 other sources, and an excluded source starred by 1,600 of them,
//! loaded in 48 commits with no indexes, so every table has about 48
//! fragments and every dependent scan reads them all. With
//! `OMNIGRAPH_ANTI_JOIN_STORE` set to a graph URI of the same types, the
//! three reads run there instead, with `OMNIGRAPH_ANTI_JOIN_CHANNEL`,
//! `OMNIGRAPH_ANTI_JOIN_EXCLUDE` and `OMNIGRAPH_ANTI_JOIN_SINCE`.

use std::time::Instant;

use omnigraph::db::Omnigraph;
use omnigraph::loader::LoadMode;
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;
use omnigraph_planner::{AntiJoinStrategy, PhysicalNode};

use crate::helpers::*;

const SCHEMA: &str = r#"
node Channel { slug: String @key }
node Source { slug: String @key }
node Touchpoint {
    slug: String @key
    action: String
    observed_at: DateTime
}
node ExternalID { slug: String @key }
edge ChannelWatchesSource: Channel -> Source
edge TouchpointOnSource: Touchpoint -> Source
edge TouchpointByIdentity: Touchpoint -> ExternalID
"#;

const OUTER: &str = r#"
        $c: Channel { slug: $channel }
        $s: Source
        $c channelWatchesSource $s
        $t: Touchpoint { action: "star" }
        $t touchpointOnSource $s
        $t.observed_at >= $since
        $x: ExternalID
        $t touchpointByIdentity $x
"#;

const BLOCK: &str = r#"
        not {
            $s0: Source { slug: $exclude }
            $t0: Touchpoint
            $t0 touchpointOnSource $s0
            $t0 touchpointByIdentity $x
            FORCE
        }
"#;

const RETURN: &str = r#"
    return { $x.slug, count($t) as repos, max($t.observed_at) as latest }
    order { repos desc }
    limit 500
"#;

/// The read with no block, with the block, and with the block forced
/// correlated (`$x.slug = $x.slug` reads the outer row).
fn reads() -> [(&'static str, String); 3] {
    let query = |block: &str| {
        format!(
            "query lookalikes($channel: String, $since: DateTime, $exclude: String) {{\n    match {{{OUTER}{block}    }}{RETURN}}}"
        )
    };
    [
        ("no block", query("").replace(", $exclude: String", "")),
        ("key_set", query(&BLOCK.replace("FORCE", ""))),
        (
            "correlated",
            query(&BLOCK.replace("FORCE", "$x.slug = $x.slug")),
        ),
    ]
}

/// A small deterministic generator: the fixture is the same on every run.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, bound: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 33) as usize) % bound
    }
}

/// The synthetic graph as JSONL batches, one commit each. Every node is
/// declared just before its first use, so a sequential split never names an
/// endpoint a later commit adds, and every table gathers about one fragment
/// per commit.
fn synthetic_batches(commits: usize) -> Vec<String> {
    const WATCHED: usize = 140;
    const OTHER: usize = 2_500;
    const IDENTITIES: usize = 2_000;
    const STARS: usize = 2;
    const ELSEWHERE: usize = 15;
    const EXCLUDED: usize = 1_600;
    let mut random = Lcg(7);
    let mut lines = vec![
        r#"{"type":"Channel","data":{"slug":"channel"}}"#.to_string(),
        r#"{"type":"Source","data":{"slug":"excluded"}}"#.to_string(),
    ];
    let mut declared = vec![false; OTHER];
    let mut touchpoints = 0usize;
    let mut touch = |lines: &mut Vec<String>, source: &str, identity: usize, day: usize| {
        let slug = format!("t{touchpoints}");
        touchpoints += 1;
        lines.push(format!(
            r#"{{"type":"Touchpoint","data":{{"slug":"{slug}","action":"star","observed_at":"2026-{:02}-{:02}T00:00:00Z"}}}}"#,
            1 + day % 9,
            1 + day % 28
        ));
        lines.push(format!(
            r#"{{"edge":"TouchpointOnSource","from":"{slug}","to":"{source}"}}"#
        ));
        lines.push(format!(
            r#"{{"edge":"TouchpointByIdentity","from":"{slug}","to":"x{identity}"}}"#
        ));
    };
    for identity in 0..IDENTITIES {
        lines.push(format!(
            r#"{{"type":"ExternalID","data":{{"slug":"x{identity}"}}}}"#
        ));
        if identity < WATCHED {
            lines.push(format!(
                r#"{{"type":"Source","data":{{"slug":"w{identity}"}}}}"#
            ));
            lines.push(format!(
                r#"{{"edge":"ChannelWatchesSource","from":"channel","to":"w{identity}"}}"#
            ));
        }
        for _ in 0..STARS {
            let source = format!("w{}", random.below(WATCHED.min(identity + 1)));
            let day = random.below(250);
            touch(&mut lines, &source, identity, day);
        }
        for _ in 0..ELSEWHERE {
            let other = random.below(OTHER);
            if !std::mem::replace(&mut declared[other], true) {
                lines.push(format!(
                    r#"{{"type":"Source","data":{{"slug":"o{other}"}}}}"#
                ));
            }
            let day = random.below(250);
            touch(&mut lines, &format!("o{other}"), identity, day);
        }
    }
    for identity in 0..EXCLUDED {
        let day = random.below(250);
        touch(&mut lines, "excluded", identity, day);
    }
    lines
        .chunks(lines.len().div_ceil(commits))
        .map(|chunk| chunk.join("\n"))
        .collect()
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// The rows every operator of a run's `plan` emitted by its `report`, and
/// those of the operators under the plan's `AntiJoin` inner tree.
fn rows_through(plan: &omnigraph_planner::PhysicalPlan, report: &serde_json::Value) -> (u64, u64) {
    let rows_of = |id: omnigraph_planner::NodeId| {
        report["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["id"] == serde_json::json!(id))
            .and_then(|row| row["attempts"].as_array()?.last()?["actual_rows"].as_u64())
            .unwrap_or(0)
    };
    let all = plan.live().map(|(id, _)| rows_of(id)).sum();
    let mut inner = 0;
    let mut pending: Vec<_> = plan
        .live()
        .filter_map(|(_, node)| match node {
            PhysicalNode::AntiJoin { inner, .. } => Some(*inner),
            _ => None,
        })
        .collect();
    while let Some(id) = pending.pop() {
        inner += rows_of(id);
        pending.extend(plan.node(id).unwrap().inputs());
    }
    (all, inner)
}

#[tokio::test]
#[ignore = "instrument: rows and wall time of a correlated block's two strategies over a fragmented, unindexed CRM-shaped graph (or OMNIGRAPH_ANTI_JOIN_STORE)"]
async fn correlated_block_strategy_rows_and_time() {
    let dir = tempfile::tempdir().unwrap();
    let uri = match std::env::var("OMNIGRAPH_ANTI_JOIN_STORE") {
        Ok(uri) => uri,
        Err(_) => {
            let uri = dir.path().to_str().unwrap().to_string();
            let db = session(Omnigraph::init(&uri, SCHEMA).await.unwrap());
            for batch in synthetic_batches(48) {
                db.load_jsonl(&batch, LoadMode::Append).await.unwrap();
            }
            uri
        }
    };
    let mut params = ParamMap::new();
    params.insert(
        "channel".to_string(),
        Literal::String(env_or("OMNIGRAPH_ANTI_JOIN_CHANNEL", "channel")),
    );
    params.insert(
        "exclude".to_string(),
        Literal::String(env_or("OMNIGRAPH_ANTI_JOIN_EXCLUDE", "excluded")),
    );
    params.insert(
        "since".to_string(),
        Literal::DateTime(env_or("OMNIGRAPH_ANTI_JOIN_SINCE", "2025-11-01T00:00:00Z")),
    );
    let mut answers = Vec::new();
    for (label, query) in reads() {
        let mut query_params = params.clone();
        if label == "no block" {
            query_params.remove("exclude");
        }
        let db = session(Omnigraph::open(&uri).await.unwrap());
        let started = Instant::now();
        let executed = db
            .query_inspected("main", &query, "lookalikes", &query_params)
            .await
            .unwrap();
        let elapsed = started.elapsed();
        let strategy = executed.plan.plan.live().find_map(|(_, node)| match node {
            PhysicalNode::AntiJoin { strategy, .. } => Some(*strategy),
            _ => None,
        });
        let report = serde_json::to_value(&executed.report).unwrap();
        let (all, inner) = rows_through(&executed.plan.plan, &report);
        println!(
            "{label:>10}: strategy {strategy:?}, {} result rows, {all} rows through operators, \
             {inner} through the block's inner tree, {elapsed:.2?}",
            executed.result.num_rows(),
        );
        match label {
            "key_set" => assert_eq!(strategy, Some(AntiJoinStrategy::KeySet)),
            "correlated" => assert_eq!(strategy, Some(AntiJoinStrategy::Correlated)),
            _ => assert_eq!(strategy, None),
        }
        answers.push((executed.result.to_rust_json().unwrap(), inner));
    }
    let [_, (key_set, key_set_rows), (correlated, correlated_rows)] =
        <[_; 3]>::try_from(answers).unwrap();
    assert_eq!(key_set, correlated, "both strategies keep the same rows");
    assert!(
        key_set_rows < correlated_rows,
        "the key set's inner tree emits fewer rows: {key_set_rows} against {correlated_rows}"
    );
}
