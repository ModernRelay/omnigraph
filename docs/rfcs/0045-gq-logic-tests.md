---
rfc: "0045"
title: "GQ logic tests"
track: maintainer
status: draft
implementation: not-started
authors:
  - azimafroozeh
created: 2026-08-29
updated: 2026-08-30
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0045: GQ logic tests

> A term set in ***bold italics*** is being defined at that exact spot; it is
> used plain everywhere after.

## Summary

Query-behavior tests become small text files, and the logic test corpus
is their default home: regressions for merged bug fixes, and feature
cases (cases with no issue anchor) for new or existing behavior alike. A
***logic test case*** is a single self-contained file (extension `.gqt`)
holding a `.pg` schema, seed rows as JSONL, and one or more ***steps***:
a read query or a mutation, each with its params and its expected outcome
(rows, affected counts, or an error message); a case may also restart the
store between steps and repeat a step group over a value list. One test
target, `crates/omnigraph/tests/gq_logic_tests.rs`, walks
`tests/gq_logic_tests/*.gqt` and runs each case against a fresh temporary
store: init, load, index, then the steps in order.

Around the harness sits an enforcement ladder: AGENTS.md contract
sentences making logic tests the default medium and holding every issue
fix to a regression, a CI check holding issue-closing fix PRs to a
matching regression, and a `no-repro` waiver label for fixes that cannot
carry one.

The boundary that does not change: Rust tests keep owning mechanism
assertions, scale symptoms, and cases needing process environment
(examples in the AGENTS.md sentences below). No second toolchain enters
the repo, and the harness calls only public engine surfaces.

## Motivation

Query-behavior tests today are hand-written Rust (the `_issue_NNN`
convention, for example `mutate_expected_head_precondition_issue_365` in
`tests/writes.rs`): every regression pays Rust-test cost, enforcement
stays review-only, and nothing structural forces a fix PR to carry a
regression. The concrete evidence is #563: review of its fix surfaced a
further pure input-to-output defect, aggregate returns computed over the
capped BM25 scan window, so `count($c)` reported the window size, not the
match count. A logic test case (twenty seeded chunks, one aggregate
query, expected `total: 20`) expresses it exactly and would have caught
it before review.

Cheap tests get written; this RFC adopts the precedent's mature
state. The sqllogictest
lineage (SQLite's format, extended by DuckDB and DataFusion) became the
primary test medium: DuckDB routes essentially all query-behavior tests
through data files ("We strongly prefer tests to be written using the
sqllogictest framework") and reserves C++ tests for what the format
cannot express. The lineage's load-bearing features (statements between
queries, loops, restart records) are v1 features here; its documented
mistakes (result hashing, type strings, bare any-error expectations) are
omitted; its run-both-plans verification is deferred (Compatibility below).
`docs/dev/testing.md` rule 2 already points the same way, and the corpus
(small schemas, seeds, queries with known-correct answers) doubles as
seed material for the DST generators.

## User and operational behavior

Authoring a regression for a fixed issue:

1. Write `tests/gq_logic_tests/issue_NNN_short_name.gqt`.
2. Run it on the unfixed build and watch it fail; record what failed in the
   `# red_on:` header line, mandatory for issue-anchored cases (a logic
   test nobody watched fail guards nothing).
3. Fix the bug; the case goes green.

Authoring a ***feature case*** (a case written for new or existing
behavior with no failure to witness): write
`<short_name>.gqt` with `# issue: none`; `red_on:` is omitted, or kept when
the case did witness a red state during development.

Running:

```bash
cargo test -p omnigraph-engine --test gq_logic_tests
```

The target prints one line per case (`ok issue_563_aggregate_uncapped` /
`FAIL issue_563_underfill_retry`) and fails at the end with the list of
failing cases and, per failure, the failing step named by ordinal and kind
(`step 3 (mutate)`), the iteration binding when the step sits in a loop
(`$who=carol`), and the expected-versus-actual row diff, count mismatch,
or error mismatch. A case stops at its first failing step (later steps
would run against a store state the failed step no longer vouches for);
across cases the target runs every case before failing, so one broken
case never hides another. A file the harness refuses (any fail-closed
check in the Design section) reports as a failing case carrying the
refusal message, and the walk continues. The lines reach the terminal
under `--nocapture`; a plain
`cargo test` shows them on failure. `OMNIGRAPH_GQ_LOGIC_TESTS=issue_563`
restricts the run to cases whose file name contains the value, and a
comma-separated list selects the union (the `OMNIGRAPH_DST_SEEDS`
precedent).

***Bless mode*** (the update-in-place workflow rustc calls `--bless` and
expect-test drives with `UPDATE_EXPECT=1`): `OMNIGRAPH_GQ_BLESS=1`
rewrites only the `--- expect` sections of the selected cases' failing
steps, one step per case per run since a case stops at its first failure,
so bless converges over reruns for row-body mismatches; a header-line
mismatch stops its case until hand-edited. It rewrites row bodies only,
in the comparison's normalized form (scale-12 decimals with trailing
zeros trimmed, object keys sorted, null cells explicit, one row per
line), in canonical sorted row order for `unordered` expects and the
run's positional order for `ordered`. Header-line expectations (`error:`
substrings, `affected:` counts) stay hand-written, since pasting a full
error message would defeat the stable-fragment rule in Comparison
semantics. Bless never runs in CI; the diff of the logic test file is the
review gate, and `red_on:` provenance stays hand-written. Bless never
changes a step's kind: when the run errors while the step expects rows or
counts, or the reverse, it reports the mismatch instead of rewriting.
Bless refuses cases containing loops (one expect body serves every
iteration).

CI: the workspace `Test Workspace` job picks the target up automatically
but does not run on pull requests (`ci.yml`); regressions must be
exercised before merge, so a per-change job (`GQ Logic Tests`) in a new
workflow file, `.github/workflows/gq-logic-tests.yml`, runs
`cargo test -p omnigraph-engine --test gq_logic_tests --locked -- --nocapture`
on every PR. The workflow triggers on push to `main`, `workflow_dispatch`,
and `pull_request` with its types declared explicitly (`opened`,
`synchronize`, `reopened`, `edited`, `labeled`, `unlabeled`), because the
gate job below must re-run when a body edit or the waiver label changes
its answer; the types list is workflow-level, so label and body-edit
events also re-run the test job, and both jobs run on docs-only PRs
(seconds-scale cost accepted). Action references are pinned, per the
repo's workflow-pin check; no failpoints features are needed. What a
green `GQ Logic Tests` job promises, quotable: every `.gqt` case parsed,
ran, and matched its expects, none refused.

Fix-PR gate: a required CI check (`Fix Regression Gate`, a second job in
the same workflow, running only on `pull_request` events since a push or
dispatch run has no PR body to read; a skipped context does not block)
reads the PR body for GitHub's closing keywords (close, closes, closed,
fix, fixes, fixed, resolve, resolves, resolved), matched the way GitHub's
own parser matches them: case-insensitive, a word boundary before the
keyword (so "hotfix #563" never fires on `fix`), an optional colon, then
whitespace (optional when the colon is present) and `#N`, with leading
zeros in `N` normalized away. Every issue so closed needs a matching
executable addition in the diff under `crates/*/tests/**`, checked
independently per issue: an added `.gqt` case in the logic test corpus
whose file name carries `issue_N`, an added `# issue: N` header line in a
corpus case, or an added Rust line defining a function whose name carries
`issue_N`, where `N` must be followed by a non-digit or by the end of the
line or path (`issue_5630` never matches issue 563; `issue_563_underfill`
does). A comment, string, or fixture line mentioning the issue never
satisfies the gate. Each accepted shape executes: the corpus walker runs
every case and refuses a malformed one, and the workspace clippy gate
refuses a dead test function; whether the test asserts the right thing
stays with review. The gate reads only that form in the PR body: closings
by full URL, `owner/repo#N` reference, a bare no-space `fixes#N`,
commit-message keyword, or manual close after merge pass unexamined; that
residue is accepted and belongs to review under the AGENTS.md regression
sentence.
The escape hatch is the `no-repro` label, applied to the PR by a
maintainer (label rights sit with triage and the label is visible on the
PR, so waiving is a reviewed maintainer act, not a silent skip). The
label waives the whole PR, an accepted coarseness; docs-only fixes,
perf-only issues, and non-deterministic races cannot carry an
input-to-output logic test, and a gate without an escape hatch gets
deleted. The gate's guarantee, quotable: exit 0 exactly when every issue
the body closes by keyword has its matching addition or the PR carries
`no-repro`, and the AGENTS.md contract sentence is present (the grep in
the Enforcement ladder).

## Design

### File format

Line-sectioned, readable top to bottom, paste-able whole into a GitHub
issue. The regression shape, one read step:

```
# issue: 563
# red_on: 2026-08-29, pre-fix build: total was 8, not 20.
# notes: free text.

--- schema
node Chunk {
    slug: String @key
    text: String @index
}

--- seed
{"type":"Chunk","data":{"slug":"chunk-00","text":"needle needle filler"}}
{"type":"Chunk","data":{"slug":"chunk-01","text":"needle filler"}}

--- query
query recall_count($q: String) {
    match {
        $c: Chunk
        search($c.text, $q)
    }
    return { count($c) as total }
}

--- params
{"q": "needle"}

--- expect unordered
{"total": 2}
```

A multi-step feature case, showing mutation steps, a restart, and a loop:

```
# issue: none
# notes: pins that committed writes survive a store reopen.

--- schema
node Person {
    name: String @key
    age: I32
}

--- seed
{"type":"Person","data":{"name":"alice","age":30}}

--- foreach $who bob carol

--- mutate
query insert_person($name: String) {
    insert Person { name: $name, age: 40 }
}

--- params
{"name": "${who}"}

--- expect affected: nodes=1 edges=0

--- endloop

--- restart

--- query
query all_names() {
    match { $p: Person }
    return { $p.name }
}

--- expect unordered
{"p.name": "alice"}
{"p.name": "bob"}
{"p.name": "carol"}
```

Grammar, fail-closed throughout: a section starts at a line beginning `--- `
and runs to the next such line or end of file, so no line inside a
section may begin with `--- ` (refused; neither `.pg` nor GQ text ever
needs one). Files are UTF-8 with `\n` endings; a trailing newline is
insignificant; blank lines in the JSONL sections (seed, expect) are
ignored; a `#` line inside a JSONL section is refused (comments live in
the header); `//` comments inside query and mutate sections are simply GQ
text. Header lines are `#` lines before the first section, keys
`# issue:`, `# red_on:`, `# notes:`, `# traversal:`. `# issue:` is always
required; `# red_on:` is required when `# issue:` names a number and
optional under `# issue: none`; a `#` line not starting a key continues
the previous entry (a first header line starting no key is refused); any
other `# <word>:` key is refused; `# notes:` and `# traversal:` are
optional. `# traversal:` takes `indexed` or `csr` and pins every step to
that mode, for cases whose subject is one traversal path.

A file is: `--- schema`, then `--- seed`, then one or more steps, of
which at least one is a query or mutate step; a file missing either
leading section, ordering them the other way, or carrying no query or
mutate step (nothing would be asserted, a restart-only step list
included) is refused. A step is one of:

- `--- query` holding exactly one GQ declaration with a read body, followed
  by an optional `--- params` section (JSON object) and a mandatory
  `--- expect` section with mode word `unordered`, `ordered`, or
  `error: <substring>`, where the substring is the trimmed remainder of the
  header line and the section body must be empty.
- `--- mutate` holding exactly one GQ declaration with a mutation body,
  followed by an optional `--- params` and a mandatory `--- expect` with
  mode word `ok` (success, counts unasserted),
  `affected: nodes=<N> edges=<M>` (success with both counts asserted; both
  are required, enumerated rather than summed), or `error: <substring>`.
  Mutation results carry no rows (`MutationResult` is the two counts), so
  there is no row expect on a mutate step.
- `--- restart`, body empty: drop the store handle and reopen it from the
  same URI before the next step.

The read/mutation classification is the compiler's own
(`query_body = { read_query_body | mutation_body }` in `query.pest`); the
harness refuses a mutation declaration under `--- query` or a read
declaration under `--- mutate`. The seed and expect bodies may be empty:
an empty seed loads nothing, and an empty `expect unordered` or
`expect ordered` body asserts an empty result, the green state of a
query-wrongly-returns-rows regression. A bare `--- expect`, and a query
or mutate step without an expect, are refused: a step whose outcome
nobody asserts hides regressions. `--- restart` alone carries no expect
(its assertion is the survival the following steps pin), and an expect
section after `--- restart` is refused. Unknown section names are
refused (Compatibility owns the evolution rationale), and a known section
out of position (a `--- params` not directly following a query or mutate
section, an `--- expect` with no step to bind to, a second `--- params`
for one step) is refused the same way.

Loops repeat a run of steps (DuckDB's shape):
`--- loop $i <start> <end>` iterates the integer half-open range
`[start, end)` over non-negative decimal bounds (a negative bound is
refused); `--- foreach $x <v1> <v2> ...` iterates the
whitespace-separated values, each over `[A-Za-z0-9_.-]` (no quoting
exists; a value needing more stays a Rust test); both close with
`--- endloop`. An empty iteration (`start >= end`, or a `--- foreach`
with no values) is refused: zero iterations would pass without asserting
anything. Inside
the loop, `${i}` in a step's params and expect bodies substitutes the
iteration's value textually before the section is parsed; query and
mutate bodies stay literal, parsed once before any iteration (which keeps
the index decision below well defined), so an iterating value reaches a
query through its params. Header-line expectations (`error:` substrings,
`affected:` counts) are never substituted, so an error fragment cannot
mention the iteration value. Loop variables are `$[a-z][a-z0-9_]*`; loops
may not nest and may not enclose `--- schema` or `--- seed`. The
substitution marker is fenced fail-closed: `${` appearing anywhere
outside a params or expect body (a query or mutate body, a seed row, a
header line), loop or no loop, is refused, and inside a params or expect
body every `${...}` must name the enclosing loop's variable (no enclosing
loop, or any other name, is refused); no escape syntax exists for a
literal `${`.

Null cells: a seed row sets a nullable property to null by writing JSON
`null` for it. A result row always carries every projected column key, with
null cells rendered as JSON `null` (never an absent key), and expected rows
are written the same way.

File names are `issue_<N>_<short_name>.gqt`, `<short_name>` over `[a-z0-9_]`,
and `<N>` must equal the `# issue:` header; the harness refuses
disagreement, which ties the CI gate's name-based match to the header's
anchor, and refuses a name matching neither shape (an `issue_` prefix with
no number or no short name). `# issue:` takes one number, so a fix closing
two issues anchors a case per issue (or a Rust twin for the second). A
feature case writes `# issue: none` and drops the prefix
(`<short_name>.gqt`); the gate never looks for it, since the gate fires only
on closing keywords.

GQ and `.pg` syntax are owned elsewhere (`docs/user/queries/index.md`;
grammar in `crates/omnigraph-compiler`); the format adds nothing on top.
Seed rows are `loader::load_jsonl`'s own shape, owned there:
`{"type": ..., "data": {...}}` for a node row,
`{"edge": ..., "from": ..., "to": ..., "data": {...}}` for an edge row. A
seed too large to sit inline belongs to the heavy tier below, not this
format; there are deliberately no external-file references, or single
files decay back into directories.

### Execution semantics

Per case, in order: create a `tempfile::tempdir()`, then
`Omnigraph::init(uri, schema_source)`, then
`loader::load_jsonl(&db, seed, LoadMode::Overwrite)`, then
`db.ensure_indices()` (its `Vec<PendingIndex>` return lists deferred
vector index builds; deferral is not failure, and reads stay correct
through brute-force search; BM25 has no such fallback). The index step is
skipped when no step's declaration uses any FTS or vector construct
(`search`, `fuzzy`, `match_text` in the match clause; `nearest`, `bm25`,
`rrf` in the order clause), implemented as an exhaustive match over the
compiler's expression variants so a newly added construct is a compile
error, never a silent skip; index builds dominate per-case cost, and
scalar-index fallbacks keep non-search results correct, only slower.

Then the steps run in file order against the case's handle:

- A query step runs
  `db.query(ReadTarget::branch("main"), query_source, name, &params)`.
- A mutate step runs `db.mutate("main", query_source, name, &params)`; its
  `MutationResult` carries `affected_nodes` and `affected_edges`, compared
  against an `affected:` expect, ignored under `ok`.
- A restart step drops the handle and reopens with `Omnigraph::open(uri)`;
  later steps use the reopened handle. What survives the reopen is exactly
  what the store committed, which is what the step exists to pin.

Traversal mode is pinned, never ambient: every step executes through the
scoped seam `instrumentation::with_traversal_mode("indexed" | "csr", fut)`,
under the case's `# traversal:` mode when present and under `indexed`
otherwise, so the `OMNIGRAPH_TRAVERSAL_MODE` process variable never
reaches a logic test. The seam is task-local and scope-bound, so
concurrent cases never interfere; it is public
today, used by `tests/proptest_equivalence.rs` and
`tests/traversal_indexed.rs`, which keep owning the engine's
modes-are-equivalent check (Compatibility below for the deferred
second-run verification).

The params section of each step is converted by the production path,
`json_params_to_param_map(Some(&value), &decl.params, JsonParamMode::Standard)`,
so logic tests accept exactly the JSON the server and CLI accept, including
the null-fill of omitted nullable params. Param keys carry no `$` sigil
(`parse_param` strips it), matching the server convention.

The harness parses each query and mutate section with
`omnigraph_compiler::parse_query`: it refuses anything but exactly one
declaration, classifies its body (read or mutation), takes the declaration's
name for `Omnigraph::query` / `Omnigraph::mutate`, reads a read body's order
clause for the `ordered` guard, and walks expressions for the index
decision. Loop iterations substitute `${var}` into params and expect bodies
and then parse; a substitution that produces an invalid section fails with
the iteration named. Every surface named here is public today.

Every case owns its store, so cases run concurrently (Runner mechanics
below); within a case, steps are strictly sequential. Cases needing
process-global state stay Rust tests: the harness refuses a schema using
`@embed` and a `nearest` over a string argument, both of which resolve an
embedding provider from process environment variables
(`EmbeddingClient::from_env`); a `nearest` over an explicit vector
parameter stays in scope. Failpoint cases stay Rust tests likewise.
Concurrency between connections (interleaved writers, transaction races)
stays out: that is DST's domain. This keeps the logic test binary out of
the serial group entirely.

### Comparison semantics

Actual rows come from `QueryResult::to_rust_json()`: a JSON array of row
objects whose keys are the result column names, `variable.property` with the
`$` stripped (`c.slug`) or the bare alias for `expr as alias` projections. The
expect section is JSONL, one object per row, same keys.

- Normalization first, both sides: one recursive walk rewrites every JSON
  number, integer-shaped or float-shaped, at any nesting depth (lists and
  structs included), to a decimal of scale 12. Integer-shaped numbers
  normalize exactly, as decimal strings, never through `f64` (an `f64`
  route would collapse distinct integers above 2^53 into equality);
  float-shaped values round at the twelfth decimal place. A hand-written
  `2` then equals a serialized `2.0`, and `f64` noise below the twelfth
  decimal place cannot fail a case (DataFusion's rule). Non-finite values compare as the strings
  `"NaN"`, `"Infinity"`, `"-Infinity"`, matching `json_float_value`.
- `expect unordered` (the default): after normalization, each row
  serializes to a canonical string (object keys sorted; the serde_json
  map in use is already order-deterministic), both row lists sort, and
  the comparison is positional, which makes it multiset equality:
  duplicate rows are legal and compare by multiplicity. GQ guarantees no
  order without an `order` clause, and ordering assertions are a
  documented flakiness source in comparable suites.
- `expect ordered`: positional comparison, allowed only when the query has an
  `order` clause; the harness checks the parsed declaration and refuses
  `ordered` without one. With an explicit `order` the engine's output order is
  a total, deterministic, shipped contract (`apply_ordering` appends the bound
  entities' key columns as an ascending tie-break), so positional comparison
  is stable.
- `expect error: <substring>`: the query must fail, and the error's rendered
  message must contain the substring. The substring is mandatory; a bare
  any-error expectation silently accepts the wrong error,
  which is the documented failure mode every sqllogictest descendant patched.
  The harness matches the substring only and does not distinguish the failure
  phase, so the author's fidelity lever is pinning the most specific stable
  fragment the refusal offers: typecheck refusals carry stable `T<N>:` codes
  (`T1`..`T24`), so `error: T21` pins a compile-time refusal precisely;
  runtime failures carry the prefixes `execution error:` and `query:`; and
  some refusals carry no prefix at all (the ordering refusal in the evidence
  section is the bare message `unsupported ordering expression`), where the
  raw message fragment is the pin.
- `expect affected: nodes=<N> edges=<M>`: exact equality on both counts.
  `expect ok` asserts success only.

Ranking scores stay unprojected in logic tests (existing search-test
practice): assert the resulting row order, never the score values.

### Runner mechanics

The walker is a single `#[tokio::test(flavor = "multi_thread")]` entry
point (tokio is already every engine integration test's runtime) that
lists `tests/gq_logic_tests/*.gqt` rooted at `CARGO_MANIFEST_DIR`
(the `forbidden_apis.rs` walk precedent) and spawns each case as a task
into a `tokio::task::JoinSet`. Case concurrency comes from that task set,
not from libtest: to libtest the whole walker is one test. Each case's
outcome, a panic included, is caught and recorded (the `JoinSet` surfaces
task panics as join errors), which lets the target run every case before
failing. The walker fails when its glob matches no files (a broken
checkout or a bad rename, never a green run). Zero new dependencies and
an ordinary libtest harness, so the workspace invocation (including its
`-- --nocapture`) is
untouched; per-file test identity via libtest-mimic is the recorded
upgrade path (Alternatives).

### Enforcement ladder and the heavy tier

The contract enters `AGENTS.md`'s Change discipline section verbatim as
three sentences (these exact lines are what a reviewer approves and what
the CI gate cites); the section's existing bug-fix bullet is rewritten in
the same PR to defer to the second sentence, so the fix-carries-regression
rule keeps one phrasing:

> Query-behavior tests default to `.gqt` logic tests under
> `crates/omnigraph/tests/gq_logic_tests/`; a Rust test needs a reason the
> logic test format cannot express (mechanism assertions, scale symptoms,
> process environment, concurrency).

> Every issue fix lands a regression test at the cheapest tier that catches
> the defect: a `.gqt` logic test when the defect is visible in rows, counts,
> or errors, a `_issue_NNN` Rust test when it needs mechanism or scale
> assertions; when the reported symptom additionally needs scale to
> manifest, a second `#[ignore]`d test in a `tests/repro_issue_*.rs` target
> guards it, and the two cross-reference each other in comments.

> Every `#[ignore]`d test opens its ignore message with its species
> (`instrument:`, `hunt:`, `heavy-repro:`, or the environment it needs);
> expensive regression repros use `heavy-repro:` and thereby enroll in the
> nightly job.

The first sentence is DuckDB's contribution rule translated. The third
generalizes the DST crate's existing practice (`instrument:` and `hunt:`
prefixes) and makes the ***heavy-repro tier*** (the `#[ignore]`d scale
repros in `tests/repro_issue_*.rs` targets) mechanically enumerable. A
blanket nightly `--ignored` run would be wrong (the workspace's ~55
`#[ignore]`d tests are mostly DST instruments and environment-gated
tests); the nightly job instead enumerates the `tests/repro_issue_*.rs`
files with a shell glob and runs each one as
`cargo test -p omnigraph-engine --test <name> -- --ignored` (one
invocation per target, so nothing depends on glob support inside cargo's
`--test` flag), with `dst-nightly.yml` as the workflow shape. A red
nightly means a heavy repro regressed at a scale the logic test tier
cannot reach; the failing target names its issue, and the failure is
triaged against that issue. Glob-discovered targets run under a default
runner sizing and timeout; a named override table in the workflow carries
the exceptions (the #563 repro needs disk for 1.2 GiB of seed and a
generous timeout), and an override naming a target the glob does not find
fails the job, so the table can never drift from the tier. The job fails
when its glob matches no `tests/repro_issue_*.rs` target, so a renamed
tier can never turn the nightly silently green. The tier is
engine-crate-scoped until a member elsewhere forces more; its first
member, `repro_issue_563`, arrives with the #563 fix PR, and the nightly
job lands only after that PR does.

The CI gate check and the `no-repro` waiver close the ladder (behavior in
the previous section). The gate script also asserts the first contract
sentence is still present in `AGENTS.md` (a literal grep for the corpus
path `crates/omnigraph/tests/gq_logic_tests/`), so deleting the contract
without deleting the gate fails closed. The ladder's recurring human
costs are named and accepted: maintainers apply `no-repro` and adjudicate
when an author believes no repro is possible; reviewers own the
plausibility of `red_on:` lines (the harness cannot check provenance) and
the scrutiny of bless-produced expect diffs, the same review surfaces the
Rust-test status quo already carries.

## Invariants

No architectural invariant is touched; the change is test-and-CI only. Every
engine surface the harness calls is public and chokepoint-registered in the
`forbidden_apis.rs` const registries: `query`, `query_with_head`, and
`run_query_at` read-only, `load_jsonl` / `load_jsonl_file` under `LOAD_V9`,
the `mutate` family under `MUTATION_V9`, and `open` / `open_with_storage`
under the `RecoveryExecutor` write protocol. That walker covers
`crates/omnigraph/src/**` only, so the test target itself adds no registry
entries; no deny-list item is affected, and no new public API is added.
The target adds no shared state to the test suite (Execution semantics).

## Compatibility and reversibility

No storage or wire surface changes; the RFC is purely additive to tests,
CI, and contributor docs. Reverting means deleting the logic test
directory, the test target, the two CI jobs, and the AGENTS.md sentences;
the logic test files remain readable, self-contained behavior records
either way. Format evolution is fail-closed: unknown sections, unknown
header keys, and missing required headers are refusals, never silent
skips, so an older harness refuses a newer logic test rather than
mis-running it.

Named compatible extensions, deferred until a real case demands each:
`require <capability>` guards for optional engine features, loop nesting,
a per-case float-precision override (unresolved questions), per-file test
identity via libtest-mimic (Alternatives), and run-twice verification:
each query step re-run under a second execution configuration (the other
traversal mode, index absence, or a forced canonical execution) with the
row sets compared. Run-twice is deferred deliberately, not for lack of a
seam: PR #544's adaptive mid-traversal switching replaces the
once-per-query mode choice such a check would pin, so the check is
specified once, against the execution model that lands there; until then
the modes-are-equivalent contract keeps its existing owner,
`tests/proptest_equivalence.rs`. Multi-connection interleaving stays out
permanently rather than deferred (DST's domain, per Execution semantics).

## Alternatives

- **Regression-only scope first, wider role later** (this RFC's own prior
  draft): rejected because every deferred feature is already proven
  load-bearing by DuckDB and Kuzu, the engine anchors for all of them are
  public today, and a corpus authored under the narrow format accumulates
  cases needing rewrites; the grammar is one parser either way. Loops
  were also weighed alone: without them the format re-opens the first
  time a case needs a seeded range. Run-twice verification was weighed
  the same way and cut from v1 (rationale in Compatibility).
- **Do nothing** (the `_issue_NNN` convention plus review vigilance): the
  cost stays Rust-sized, the obligation stays review-only, and #563
  demonstrates what slips through.
- **Dedicated `tests/regressions/issue_NNN.rs` Rust files:** greppable,
  but the marginal cost does not move, and regression tests drift away
  from the boundary that owns the behavior.
- **Python-over-HTTP suite:** a second toolchain, per-test server
  startup, e2e flakiness, and omnigraph has no bindings; revisit only if
  bindings happen for other reasons.
- **Adopt sqllogictest-rs and the `.slt` format:** its parser treats
  query text as opaque, so GQ fits, but a logic test case is a hermetic
  schema-plus-seed world built through `Omnigraph::init` and
  `loader::load_jsonl`, neither expressible as a GQ statement in a `.slt`
  stream; the setup machinery, restart record, and traversal-mode seam
  would still be hand-written, and its whitespace-separated expected rows
  discard the `to_rust_json` row-object match this design gets for free.
- **insta snapshot testing:** splits the query and its expectation across
  files and moves review into a bespoke tool; in-place expectations with
  git diff as the review gate preserve red-first provenance better.
- **libtest-mimic per-file tests:** one `Trial` per logic test gives real
  test identity (`cargo test -p omnigraph-engine issue_563` selects one
  case) and cargo-nextest compatibility, but costs `harness = false` and
  changes how the workspace's `-- --nocapture` flag lands (compatibility
  unverified). Deferred, not rejected: the env-var filter covers
  selection, and the walker can swap to Trials without touching the
  format.

## Evidence and tests

The harness proves itself on two fronts: the logic-test-expressible #563
regressions as the first corpus entries, each with a red state recorded
during the development of the #563 fix (their Rust twins arrive with that
fix), and feature cases exercising every step kind the format defines:

- `issue_563_aggregate_uncapped.gqt`: twenty matching chunks, `limit 2`,
  aggregate return; red produced `total: 8` (the capped window), green
  produces `total: 20`.
- `issue_563_underfill_retry.gqt`: edges only on the middle band the capped
  scan window excludes; red (retry disabled) returned zero rows, green
  returns exactly `chunk-08` and `chunk-09`.
- One `expect error` case pinning a refusal on the order clause: an aggregate
  written out in full in `order { }` rather than by its projection alias is
  refused with the bare message `unsupported ordering expression` (the #566
  shape), so the error path is exercised from day one.
- `restart_survives_reopen.gqt` (feature case, `# issue: none`): the
  multi-step example in the Design section, inserts via mutate steps inside
  a `foreach`, asserts affected counts, restarts, reads back; pins that
  committed mutations survive a reopen and exercises mutate, loop, restart,
  and read steps in one case.
- `mutation_error_typed.gqt` (feature case): a mutate step inserting a node
  that omits a non-nullable property (its `@key`; `@key` properties can
  never be nullable) is refused at typecheck, pinned by its stable code
  (`error: T12`). A
  mutation whose match merely misses is not an error: `update` and
  `delete` on an absent key succeed with zero affected counts, the shape
  `expect affected: nodes=0 edges=0` pins.

The third #563 regression (the 2 GiB offset-overflow repro) is deliberately
not a logic test: its symptom is a byte count, and it arrives with the #563
fix PR as the `#[ignore]`d `repro_issue_563`, the heavy-repro tier's first
member.

Harness self-tests pin every refusal this RFC specifies (the File
format, Execution semantics, and Bless mode sections), one test per
refusal.

Docs follow the testing map: the harness joins the "Query results and
operators" row of the engine ownership table in `docs/dev/testing.md`, plus a
focused-iteration command in its Commands section (`check-agents-md.sh` keeps
the docs indexes honest, so no new orphan doc file).

## Rollout

1. **Harness and first corpus** (one PR): the `gq_logic_tests` test target
   with the full format (steps, loops, restart), the five cases
   above, the self-tests, and the `docs/dev/testing.md` rows. Independently
   safe; ships regression value with zero workflow change. `implementation`
   advances to `partial`.
2. **Enforcement and tiers** (one PR): the AGENTS.md sentences
   (logic-tests-by-default, regression per fix, `#[ignore]`
   species-in-message, heavy-repro tier), the per-change logic-test CI job,
   the closing-keyword gate check with the `no-repro` label (the label
   itself is created in the repo in this phase), and the nightly
   heavy-repro job. Requiredness is wired the way this repo wires it: the
   `GQ Logic Tests` and `Fix Regression Gate` job names enter the
   `contexts` list in `.github/branch-protection.json` (applied via
   `scripts/apply-branch-protection.sh`, rationale recorded in
   `docs/dev/branch-protection.md`); both become required (seconds of
   hermetic cases should block a regression before merge).
   `implementation` advances to `complete`.

The nightly heavy-repro job is one workflow file, separable from phase 2,
but lands only after the #563 fix PR delivers the tier's first member.

## Unresolved questions

- Float policy: fixed scale 12 for all numbers, or a per-case precision
  override? Scale 12 is the draft's default; the author decides, forced
  by the first corpus case a fixed scale cannot express.
- Where the gate check's script lives: `scripts/` shell alongside
  `check-agents-md.sh`, or a workflow-inline step. The phase-2 PR review
  decides.
- Whether a case may pin a schema-IR vintage for bugs that only reproduce on a
  legacy vintage. The author decides, forced by the first real case that
  needs it.

## Decision log

None yet.
