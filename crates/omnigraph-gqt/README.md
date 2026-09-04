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
- `cases/kuzu_*.gqt`: cases derived from the [Kuzu](https://github.com/kuzudb/kuzu)
  e2e test corpus (`test/test_files/`), named `kuzu_<area>_<file>[_<case>]`,
  one file per Kuzu `-CASE`, or per `-LOG` block or statement when a case
  was split (`__<log>` / `__stmt_N` suffix; merged scenarios carry both
  numbers, `scenario3_4`). They are `# issue: none` feature cases; the fix
  regression gate never looks at them. Each Cypher statement is translated
  to GQ. The header names the source file and case, and its `# notes:`
  lines record every spelling substitution the translation needed and which
  of these rules the expectation follows:
  - Where the engines agree, the expectation is Kuzu's, translated to GQ;
    the attribution line in every header ("the expected rows are Kuzu's
    unless a line above says they were rewritten") says so. Numbers
    render at scale 12 (Compatibility), so a six-decimal Kuzu AVG is
    written at twelve.
  - Where the engines disagree on semantics, the statement is split into
    its own file, the header names Kuzu's value and the reading taken, and
    the expectation follows that reading: a bounded traversal as
    shortest-path distance per endpoint pair, an undirected self-loop as
    one row per matched node.
  - Where Kuzu asserts only `ok`, the expected rows are derived from the
    seed.
  - A translation may add an order key so a `limit` cut is deterministic,
    or split one statement into typed reads when an untyped edge has no GQ
    spelling; the header records each such change.
  - A case pinning a refusal or fix the engine does not have yet may be
    red on its pull-request branch; its header names what it waits on, and
    the PR merges only after that lands, so the corpus on `main` is never
    red.

  Statements Kuzu can express and GQ cannot (arithmetic, `WITH`, path
  variables, functions, multi-label patterns, and so on) are simply absent.
  The `kuzu_` prefix is a convention, not a harness distinction.

  Copyright (c) 2022-2025 Kùzu Inc., MIT License
  ([Kuzu's LICENSE](https://github.com/kuzudb/kuzu/blob/master/LICENSE)).
  This directory holds only `.gqt` files, so the permission notice is
  reproduced here:

  > Permission is hereby granted, free of charge, to any person obtaining a copy
  > of this software and associated documentation files (the "Software"), to deal
  > in the Software without restriction, including without limitation the rights
  > to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  > copies of the Software, and to permit persons to whom the Software is
  > furnished to do so, subject to the following conditions:
  >
  > The above copyright notice and this permission notice shall be included in all
  > copies or substantial portions of the Software.
  >
  > THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  > IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  > FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  > AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  > LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  > OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  > SOFTWARE.
- `src/lib.rs`: the runner (case parsing, execution against a fresh
  temporary store, row comparison, bless). Format self-tests and the corpus
  layout check are its unit tests (`src/tests.rs`).
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
