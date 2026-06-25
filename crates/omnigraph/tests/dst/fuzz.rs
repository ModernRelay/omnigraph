//! PR-D parser/loader fuzz — proptest-based (no nightly / `cargo-fuzz` needed).
//!
//! The lesson of Lance #7230: a parse/merge path that was only ever fed
//! pre-sorted, dup-free input shipped a latent crash. So feed the GQ query
//! parser, the schema parser, and the JSONL loader BOTH arbitrary and
//! ADVERSARIAL (duplicate-key, reordered-field, malformed) input and assert they
//! return a `Result` — never PANIC. proptest shrinks any panic to a minimal
//! input and persists the seed under `proptest-regressions/`.
//!
//! (cargo-fuzz/libFuzzer targets are the deferred follow-up — they need a
//! nightly toolchain; this proptest form gets the same "parser survives hostile
//! input" coverage with the stable toolchain already in use.)

use proptest::prelude::*;

use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::find_named_query;
use omnigraph_compiler::schema::parser::parse_schema;

use crate::backend;

/// Random GQ-token sequences — far likelier to reach deep parser states (and
/// their panics) than purely random bytes.
fn arb_gq() -> impl Strategy<Value = String> {
    let token = prop_oneof![
        Just("query"),
        Just("q"),
        Just("("),
        Just(")"),
        Just("{"),
        Just("}"),
        Just("match"),
        Just("return"),
        Just("$x"),
        Just("$y"),
        Just(":"),
        Just("Person"),
        Just("Doc"),
        Just("knows"),
        Just("not"),
        Just("limit"),
        Just("3"),
        Just("order"),
        Just("desc"),
        Just(","),
        Just("."),
        Just("slug"),
        Just("\""),
        Just("count"),
        Just("="),
        Just("where"),
        Just("{1,3}"),
        Just("nearest"),
    ]
    .prop_map(|s| s.to_string());
    proptest::collection::vec(token, 0..40).prop_map(|toks| toks.join(" "))
}

/// Random `.pg`-token sequences for the schema parser.
fn arb_pg() -> impl Strategy<Value = String> {
    let token = prop_oneof![
        Just("node"),
        Just("edge"),
        Just("Person"),
        Just("Doc"),
        Just("{"),
        Just("}"),
        Just("slug"),
        Just(":"),
        Just("String"),
        Just("I64"),
        Just("@key"),
        Just("@index"),
        Just("@card(0..1)"),
        Just("enum(a,b)"),
        Just("->"),
        Just("Knows"),
        Just("\n"),
    ]
    .prop_map(|s| s.to_string());
    proptest::collection::vec(token, 0..40).prop_map(|toks| toks.join(" "))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    /// The GQ query parser must return a Result for ANY input — never panic.
    #[test]
    fn gq_parser_never_panics(src in ".{0,256}") {
        let _ = find_named_query(&src, "q");
    }

    /// ...including structured GQ-token soup that reaches deep parse states.
    #[test]
    fn gq_parser_structured_never_panics(src in arb_gq()) {
        let _ = find_named_query(&src, "q");
    }

    /// The schema parser must return a Result for ANY input — never panic.
    #[test]
    fn schema_parser_never_panics(src in ".{0,256}") {
        let _ = parse_schema(&src);
    }

    /// ...including structured `.pg`-token soup.
    #[test]
    fn schema_parser_structured_never_panics(src in arb_pg()) {
        let _ = parse_schema(&src);
    }
}

/// The JSONL loader must SURVIVE adversarial batches — duplicate `@key`s,
/// reordered/extra/missing fields, wrong types, malformed JSON, orphan edges —
/// returning `Ok`/`Err`, never panicking (and never corrupting: a later normal
/// load + read still works).
#[tokio::test]
async fn loader_survives_adversarial_jsonl() {
    let dir = tempfile::tempdir().unwrap();
    let db = backend::open_clean(dir.path().to_str().unwrap()).await;

    let adversarial: &[&str] = &[
        // duplicate @key within one batch
        "{\"type\":\"Person\",\"data\":{\"slug\":\"dup\",\"name\":\"a\"}}\n{\"type\":\"Person\",\"data\":{\"slug\":\"dup\",\"name\":\"b\"}}\n",
        // reordered top-level fields
        "{\"data\":{\"slug\":\"r1\",\"name\":\"n\"},\"type\":\"Person\"}\n",
        // extra unknown field
        "{\"type\":\"Person\",\"data\":{\"slug\":\"x1\",\"name\":\"n\",\"bogus\":42}}\n",
        // missing required property (name)
        "{\"type\":\"Person\",\"data\":{\"slug\":\"x2\"}}\n",
        // wrong type (name as number)
        "{\"type\":\"Person\",\"data\":{\"slug\":\"x3\",\"name\":99}}\n",
        // unknown node type
        "{\"type\":\"Ghost\",\"data\":{\"slug\":\"g\"}}\n",
        // null slug
        "{\"type\":\"Person\",\"data\":{\"slug\":null,\"name\":\"n\"}}\n",
        // orphan edge (endpoints don't exist)
        "{\"edge\":\"Knows\",\"from\":\"nope\",\"to\":\"nope\",\"data\":{}}\n",
        // malformed JSON line
        "{not json at all\n",
        // empty line + valid line
        "\n{\"type\":\"Doc\",\"data\":{\"slug\":\"d1\",\"source\":\"whatsapp\",\"body\":\"b\"}}\n",
        // enum out of range
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"d2\",\"source\":\"not_an_enum\",\"body\":\"b\"}}\n",
        // huge string
        &{
            let big = "z".repeat(50_000);
            format!("{{\"type\":\"Person\",\"data\":{{\"slug\":\"big\",\"name\":\"{big}\"}}}}\n")
        },
    ];

    for (i, batch) in adversarial.iter().enumerate() {
        // The ONLY contract: no panic. Ok or Err are both acceptable.
        let _ = load_jsonl(&db, batch, LoadMode::Merge).await;
        let _ = i;
    }

    // ...and the graph is still usable after all that abuse.
    load_jsonl(
        &db,
        "{\"type\":\"Person\",\"data\":{\"slug\":\"sane\",\"name\":\"ok\"}}\n",
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let n = db
        .query(
            omnigraph::db::ReadTarget::branch("main"),
            "query q() { match { $p: Person { slug: \"sane\" } } return { $p.slug } }",
            "q",
            &omnigraph_compiler::ir::ParamMap::new(),
        )
        .await
        .unwrap()
        .num_rows();
    assert_eq!(n, 1, "graph must remain usable after adversarial loads");
}
