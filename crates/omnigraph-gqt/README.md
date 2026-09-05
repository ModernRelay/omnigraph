# omnigraph-gqt

The GQ logic-test corpus and its runner. Never part of a release build:
`publish = false`, not a workspace default member, not in `release.yml`. A
bare `cargo test` at the workspace root therefore skips it; `-p omnigraph-gqt`
or `--workspace` reaches it.

- `cases/*.gqt`: the corpus. One file is one case: a `.pg` schema, JSONL
  seed rows, and steps (queries, mutations, restarts, loops) with expected
  outcomes. Format, refusal set, and comparison semantics: RFC 0045
  (`docs/rfcs/0045-gq-logic-tests.md`). A regression for a merged fix is
  `issue_NNN_<short_name>.gqt`; `scripts/check-fix-regression.py` looks for it
  here.
- `src/lib.rs`: the runner (case parsing, execution against a fresh
  temporary store, the `--- expect shape` check of each rows step's result
  columns, the result-schema check against the compiler's inferred schema,
  row comparison, bless); `src/shape.rs` parses and compares the shape
  section. Format self-tests and the corpus layout check are its unit
  tests (`src/tests.rs`).
- `tests/gq_logic_tests.rs`: one libtest test per case, named
  `case::<file>.gqt`, registered at run time by `datatest-stable`
  (`harness = false`). A new case file is picked up without any Rust change.

```bash
cargo test -p omnigraph-gqt                                      # everything
cargo test -p omnigraph-gqt --test gq_logic_tests issue_563      # cases whose name contains issue_563
cargo test -p omnigraph-gqt --test gq_logic_tests -- --list      # one line per case
cargo test -p omnigraph-gqt --test gq_logic_tests -- --test-threads=2
OMNIGRAPH_GQ_BLESS=1 cargo test -p omnigraph-gqt --test gq_logic_tests my_case   # rewrite the failing expect
```

`OMNIGRAPH_GQ_CASE_TIMEOUT_SECS=<n>` (default 10) bounds each case's wall
time; a case over budget belongs in a `heavy-repro:` `#[ignore]`d Rust test,
not here.
