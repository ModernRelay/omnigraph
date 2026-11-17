//! Shrunk cases of defects the physical-layout variants
//! (`tests/gq_layout_tests.rs`) found, each pinned as a `hunt:` until its
//! fix lands: the case runs under the layout that breaks it.

use std::path::Path;

use omnigraph_gqt::Layout;

/// Runs `text` as a corpus case under `layout`.
fn run_under(stem: &str, text: &str, layout: Layout) -> Result<(), String> {
    let case = omnigraph_gqt_core::parse_case(stem, text).map_err(|e| e.to_string())?;
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(omnigraph_gqt::execute_with_layout(
            &case,
            Path::new(stem),
            layout,
        ))
}

/// A `nearest` window of 2 from `[1, 0]` over `d0 [0, 0]`, `d1 [1, 0]`,
/// `d2 [2, 0]`: `d1` at distance 0, then `d0` and `d2` tied at 1, of which
/// the smaller identity, `d0`, is in the window. Under layout `d` (the
/// rows spread over fragments with deleted copies, then indexed), the
/// window takes `d2`: the tie at the window's boundary is decided by the
/// rows' physical order, not by identity. Found by the layout variants on
/// `rrf_refuses_arms_and_options_outside_its_contract.gqt`.
#[test]
#[ignore = "hunt: a nearest window's boundary tie is decided by fragment order under layout d"]
fn nearest_window_boundary_tie_is_by_identity_under_every_layout() {
    let text = r#"# issue: none

--- runner
timeout_ms: 10000
environments:
  - target: omnigraph-engine
    storage: local-filesystem

--- schema
node Doc {
    slug: String @key
    embedding: Vector(2) @index
}

--- seed
{"type":"Doc","data":{"slug":"d0","embedding":[0.0,0.0]}}
{"type":"Doc","data":{"slug":"d1","embedding":[1.0,0.0]}}
{"type":"Doc","data":{"slug":"d2","embedding":[2.0,0.0]}}

--- query
query window() {
    match {
      let $all = { match { $d: Doc } return { $d } },
      from nearest($all, $d.embedding, [1.0, 0.0], k: 2) as $s,
    }
    return { $d.slug }
}

--- expect unordered
{"d.slug":"d0"}
{"d.slug":"d1"}

--- expect shape
d.slug: String
"#;
    for layout in Layout::ALL {
        run_under("nearest_window_boundary_tie", text, layout)
            .unwrap_or_else(|e| panic!("layout {}: {e}", layout.letter()));
    }
}
