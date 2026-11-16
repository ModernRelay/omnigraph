//! The rrf prefilter gate (`exec::query::rrf_prefilter_gate`) decides the
//! plan of an `rrf` whose arms rank with `bm25`. Every full-text field has a
//! profile, and fusion does not rank an exact `bm25` arm, so each such read
//! is refused at type check (`T86`) and the gate decides nothing: no verdict
//! is recorded and no arm scans.

use serial_test::serial;

use omnigraph::db::Omnigraph;
use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::value::Literal;

use crate::helpers::*;

const GATE_SCHEMA: &str = r#"
node Chunk {
    slug: String @key
    text: String @index
    embedding: Vector(4)
}

node Artifact {
    slug: String @key
}

edge ChunkOfArtifact: Chunk -> Artifact {
    label: String
}

edge ChunkCites: Chunk -> Chunk {
    label: String
}
"#;

const GATE_QUERIES: &str = r#"
query single_hop_both_bm25($q1: String, $q2: String) {
    match {
      let $pop = { match { $c: Chunk, $a: Artifact, $c chunkOfArtifact $a } return { $c } },
      from rrf(bm25($pop, $c.text, $q1), bm25($pop, $c.text, $q2), k: 60) as $score,
    }
    return { $c.slug }
    order { $score desc }
    limit 5
}

query search_filter_single_hop($q1: String, $q2: String) {
    match {
      let $pop = { match { $c: Chunk, $a: Artifact, $c chunkOfArtifact $a } where match_terms($c.text, terms($q1, mode: any)) return { $c } },
      from rrf(bm25($pop, $c.text, $q1), bm25($pop, $c.text, $q2), k: 60) as $score,
    }
    return { $c.slug }
    order { $score desc }
    limit 5
}

query bm25_secondary_position($v: Vector(4), $q: String) {
    match {
      let $pop = { match { $c: Chunk, $a: Artifact, $c chunkOfArtifact $a } return { $c } },
      from rrf(nearest($pop, $c.embedding, $v, k: 5), bm25($pop, $c.text, $q), k: 60) as $score,
    }
    return { $c.slug }
    order { $score desc }
    limit 5
}

query multi_hop($q1: String, $q2: String) {
    match {
      let $pop = { match { $c: Chunk, $d: Chunk, $c chunkCites{1,2} $d } return { $c } },
      from rrf(bm25($pop, $c.text, $q1), bm25($pop, $c.text, $q2), k: 60) as $score,
    }
    return { $c.slug }
    order { $score desc }
    limit 5
}

query multi_hop_min_two($q1: String, $q2: String) {
    match {
      let $pop = { match { $c: Chunk, $d: Chunk, $c chunkCites{2,2} $d } return { $c } },
      from rrf(bm25($pop, $c.text, $q1), bm25($pop, $c.text, $q2), k: 60) as $score,
    }
    return { $c.slug }
    order { $score desc }
    limit 5
}

query dst_filtered($q1: String, $q2: String) {
    match {
      let $pop = { match { $c: Chunk, $a: Artifact, $c chunkOfArtifact $a } where $a.slug = "art-0" return { $c } },
      from rrf(bm25($pop, $c.text, $q1), bm25($pop, $c.text, $q2), k: 60) as $score,
    }
    return { $c.slug }
    order { $score desc }
    limit 5
}

query direction_both($q1: String, $q2: String) {
    match {
      let $pop = { match { $c: Chunk, $d: Chunk, $c <chunkCites> $d } return { $c } },
      from rrf(bm25($pop, $c.text, $q1), bm25($pop, $c.text, $q2), k: 60) as $score,
    }
    return { $c.slug }
    order { $score desc }
    limit 5
}

query direction_in($q1: String, $q2: String) {
    match {
      let $pop = { match { $c: Chunk, $d: Chunk, $d chunkCites $c } return { $c } },
      from rrf(bm25($pop, $c.text, $q1), bm25($pop, $c.text, $q2), k: 60) as $score,
    }
    return { $c.slug }
    order { $score desc }
    limit 5
}

query several_expands($q1: String, $q2: String) {
    match {
      let $pop = { match { $c: Chunk, $a: Artifact, $d: Chunk, $c chunkOfArtifact $a, $c chunkCites $d } return { $c } },
      from rrf(bm25($pop, $c.text, $q1), bm25($pop, $c.text, $q2), k: 60) as $score,
    }
    return { $c.slug }
    order { $score desc }
    limit 5
}

query antijoin_only($q1: String, $q2: String) {
    match {
      let $pop = { match { $c: Chunk } where not exists { $a: Artifact, $c chunkOfArtifact $a } return { $c } },
      from rrf(bm25($pop, $c.text, $q1), bm25($pop, $c.text, $q2), k: 60) as $score,
    }
    return { $c.slug }
    order { $score desc }
    limit 5
}

query undeclared_ranked_destination($q1: String, $q2: String) {
    match {
      let $pop = { match { $d: Chunk, $c: Chunk, $d chunkCites $c } return { $c } },
      from rrf(bm25($pop, $c.text, $q1), bm25($pop, $c.text, $q2), k: 60) as $score,
    }
    return { $c.slug }
    order { $score desc }
    limit 5
}

query no_traversal($q1: String, $q2: String) {
    match {
      let $pop = { match { $c: Chunk } return { $c } },
      from rrf(bm25($pop, $c.text, $q1), bm25($pop, $c.text, $q2), k: 60) as $score,
    }
    return { $c.slug }
    order { $score desc }
    limit 5
}
"#;

#[tokio::test]
#[serial]
async fn every_gated_fusion_is_refused_before_the_gate() {
    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), GATE_SCHEMA)
            .await
            .unwrap(),
    );
    let params: ParamMap = [
        ("q1".to_string(), Literal::String("needle".into())),
        ("q2".to_string(), Literal::String("sharp".into())),
    ]
    .into_iter()
    .collect();
    let names: Vec<&str> = GATE_QUERIES
        .lines()
        .filter_map(|line| line.strip_prefix("query "))
        .filter_map(|line| line.split('(').next())
        .collect();
    assert!(!names.is_empty());
    for name in names {
        let probes = QueryIoProbes::default();
        let error = with_query_io_probes(probes.clone(), async {
            query_main(&db, GATE_QUERIES, name, &params).await
        })
        .await
        .unwrap_err();
        let text = error.to_string();
        assert!(
            text.contains("T86") && text.contains("an `rrf` arm over `bm25`"),
            "{name}: {text}"
        );
        assert!(probes.rrf_gate_verdicts.lock().unwrap().is_empty(), "{name}");
    }
}
