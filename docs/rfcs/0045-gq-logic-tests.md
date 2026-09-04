---
rfc: "0045"
title: "GQ logic tests"
track: maintainer
status: draft
implementation: partial
authors:
  - azimafroozeh
created: 2026-08-29
updated: 2026-09-04
discussion: https://github.com/ModernRelay/omnigraph/pull/584
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
store between steps and repeat a step group over a value list. A
dedicated workspace crate, `omnigraph-gqt` (`publish = false`, outside
`default-members`, and outside the explicit `-p` list `release.yml`
builds, so never in a release), holds the corpus and the runner: its one
integration-test target, `crates/omnigraph-gqt/tests/gq_logic_tests.rs`,
registers every top-level, non-dot-prefixed
`crates/omnigraph-gqt/cases/*.gqt` file (any other entry except an
extension-less dot-file fails `corpus_layout`, Runner mechanics) as its own libtest-compatible test and
runs each case against a fresh temporary store: init, load, index, then
the steps in order.

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

1. Write `crates/omnigraph-gqt/cases/issue_NNN_short_name.gqt`.
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
cargo test -p omnigraph-gqt
```

The crate is not a workspace default member, so a bare `cargo test` at
the root skips it; `-p omnigraph-gqt` or `--workspace` reaches it. The
target prints one line per case with its elapsed time
(`ok issue_563_aggregate_uncapped 0.12s` /
`FAIL issue_563_underfill_retry 0.09s`) and fails at the end with the list of
failing cases and, per failure, the failing step named by ordinal and kind
(`step 3 (mutate)`), the iteration binding when the step sits in a loop
(`$who=carol`), and the expected-versus-actual row diff, count mismatch,
or error mismatch. A case stops at its first failing step (later steps
would run against a store state the failed step no longer vouches for);
across cases the target runs every case before failing, so one broken
case never hides another. A file the harness refuses (any fail-closed
check in the Design section) reports as a failing case carrying the
refusal message, and the remaining cases still run. The per-case lines
print on every run: the target's harness never captures output, so
`--nocapture` changes nothing for it (the crate's unit tests, under real
libtest, still honor it). Every case is its own libtest-compatible test,
named `case::<file>.gqt`, so
`cargo test -p omnigraph-gqt --test gq_logic_tests issue_563`
restricts the run to cases whose file name contains the argument, and
`-- --list` names every registered case.

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
and, since the pull-request tier landed, runs it on pull requests as a
reporting context (`ci.yml`); regressions must block a merge, and only a
required context can, so a per-change job (`GQ Logic Tests`) in a new
workflow file, `.github/workflows/gq-logic-tests.yml`, runs
`cargo test -p omnigraph-gqt --locked -- --nocapture`
on every PR. The workflow triggers on push to `main`, `workflow_dispatch`,
and `pull_request` with only its code-bearing types declared (`opened`,
`synchronize`, `reopened`); the gate below lives in its own workflow and
declares the body-edit and label types itself, so a body edit or a label
change never re-runs the Rust build. The test job compiles the engine
crate, the `omnigraph-gqt` library, and its two test binaries: minutes
with a warm cache, tens of minutes cold. The test job honors the
docs-only classification of `ci.yml`'s `Classify Changes` job, the way
the other required Rust jobs do, through a verbatim copy of that job
carried in its own workflow (`Classify Changes (GQ Logic Tests)`; GitHub
Actions cannot make a job depend on another workflow's job, and
`scripts/check-classify-copy.py` refuses drift from `ci.yml`): on a
docs-only PR it skips its build and reports success (the
`Test omnigraph-server --features aws` job's pattern), so the required
context never stays pending. The gate job always runs, at seconds-scale.
Action references are pinned, per the
repo's workflow-pin check; no failpoints features are needed. What a
green `GQ Logic Tests` job promises, quotable: every `.gqt` case parsed,
ran, and matched its expects, none refused.

Fix-PR gate: a required CI check (`Fix Regression Gate`, a job in its
own workflow, `.github/workflows/fix-regression-gate.yml`, on
`pull_request_target`, which takes the workflow file and the gate script
from the base branch and fetches the head only as data for the diff
range, so a pull request cannot weaken the copy that runs; the workflow
has no push or dispatch trigger, since those runs have no PR body to
read) reads the PR body for GitHub's closing keywords (close, closes, closed,
fix, fixes, fixed, resolve, resolves, resolved), matched the way GitHub's
own parser matches them: case-insensitive, a word boundary before the
keyword (so "hotfix #563" never fires on `fix`), an optional colon, then
whitespace (optional when the colon is present) and the target, one of
`#N`, `OWNER/NAME#N`, and `https://github.com/OWNER/NAME/issues/N` for the
repository the gate runs in (`--repo`; the workflow passes
`GITHUB_REPOSITORY`; a reference to another repository closes nothing here
and is not read), with leading zeros in `N` normalized away. Before any
issue is examined the gate lists the paths the diff changes (`git diff
--name-only --no-renames`, so a file moved out of a crate still shows its
source-side deletion); when none is under `crates/` or `tools/` (a Markdown
file there does not count) and none is the root `Cargo.toml` or
`Cargo.lock`, the code paths, where every workspace member lives (the
gate's self-test pins every `[workspace] member` to them), the PR passes
with its closed issues unexamined, as a log line and a notice annotation: a
fix made in a workflow, a script, a document, or a deployment file has no
logic or Rust test that could witness it, and a demand for one is a demand
for nothing. Every issue so closed by a PR that changes a code path needs a
matching regression in the diff, added or strengthened (the two owner locations
`docs/dev/testing.md` lists per package: `tests/` targets and in-source
test modules), checked independently per issue: a `.gqt` case in the
logic test corpus named `issue_N_*`, new or modified with
at least one added body line (not a `#` header line or a `//` comment),
or a Rust function whose name carries `issue_N`, either added with an
added `#[test]` or `#[<path>::test]` attribute line
(`#[tokio::test(...)]` included) directly above it in the same hunk, or
existing, test-attributed, and given an added line carrying an
alphanumeric character, not a comment or an attribute, inside its body;
`N` must be followed by a non-digit or by the end of the line (Rust
shape) (`issue_5630` never matches issue 563;
`issue_563_underfill` does). The Rust shape matches only in top-level
test targets, `crates/*/tests/<name>.rs` and `tools/*/tests/<name>.rs`,
and in-source modules, `crates/*/src/**` and `tools/*/src/**`; helper and
fixture modules under `tests/` never match (a helper named for an issue
is not a test). A plain function, however named, never satisfies the
gate; an added definition is skipped when its name starts with `_`, when
the line is a declaration ending in `;`, or when the same name is
removed elsewhere in the diff (a rename alone never counts, a rename
plus an added assertion does). A comment, string, or fixture line
mentioning the issue never satisfies the gate, and owners the gate does
not recognize inside the code paths (a helper or fixture module, a script
under a crate, a rustdoc-only change) satisfy it only through `no-repro`. Adjacency and body-location rules, with their named
residues, are in the Decision log (2026-09-02). The gate is a diff check, and what it guarantees about
execution differs by shape: a corpus shape executes, since the target
registers and runs every `.gqt` in the corpus, refuses a malformed one, and the
`GQ Logic Tests` job is required; a Rust shape is a test-attributed
definition, not a run: the gate consults only the required contexts, and
among Rust test targets only `omnigraph-gqt`'s (the corpus target and its
unit tests), `Test omnigraph-server --features aws`, and `DST pinned suite`
(`cargo test -p omnigraph-dst`, `dst.yml`) run on a pull request as
required contexts (`Test Workspace` runs every workspace target on the
pull request too, but as a reporting context, CI above); a test-attributed
`issue_N` function inside `crates/omnigraph-gqt/`, `crates/omnigraph-server/`,
or `crates/omnigraph-dst/` therefore runs in a required context, and the
Rust shape stays a naming check everywhere else, where a defined
function can besides be `#[ignore]`d or cfg-gated (workspace clippy on
the pull request refuses an unreferenced private function, not those),
so whether that test runs in the suite and asserts the right thing stays
with review, which the first AGENTS.md sentence primes: a Rust test needs
a reason the format cannot express. The gate reads only those three forms
in the PR body: closings by `GH-N`, a bare no-space `fixes#N`, an
autolink in angle brackets or a Markdown link whose text is `#N`, an
`http://` or `www.` URL,
commit-message keyword, or manual close after merge pass unexamined, a
keyword inside a code span, a fence, or an HTML comment is read, and a PR
against a non-default base is examined although GitHub closes nothing
there; that residue is accepted and belongs to review under the AGENTS.md
regression sentence. A failure names the code paths that made the gate
examine the PR, the ways through, any near miss the diff holds (a case
whose header says `# issue: N` under another name or a subdirectory; a
test named with the bare number, moved rather than added, under a leading
`_`, or in a helper module; a function named for the issue with no test
attribute directly above it), and a case skeleton; the same text goes out
as a GitHub error annotation, so it shows on the checks summary without
opening the log.
The escape hatch is the `no-repro` label, applied to the PR by a
maintainer (label rights sit with triage and the label is visible on the
PR, so waiving is a reviewed maintainer act, not a silent skip). The
label waives the whole PR, an accepted coarseness; perf-only issues,
non-deterministic races, a removal, and a rustdoc-only change cannot carry
an input-to-output logic test (a fix outside the code paths never reaches
the label), and a gate without an escape hatch gets deleted. The gate's guarantee, quotable: exit 0 exactly
when the diff changes no code path, or every issue the body closes by
keyword has its matching addition, or the PR carries `no-repro`, and in
every case the AGENTS.md contract sentence is present (the grep in
the Enforcement ladder); a corpus match means the case ran green in the
required job, and a Rust match means a test-attributed function of that
name was added or extended. The guarantee holds over the base branch's
copy of the script and workflow file and the labels triage rights
control: labels reach the check comma-joined, so a label name containing
a comma could carry the waiver token; creating a label needs the same
triage rights as applying one, and that residue is accepted.

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
needs one). Files are UTF-8 with `\n` endings (a `\r` anywhere is
refused); a trailing newline is
insignificant; blank lines in the JSONL sections (seed, expect) are
ignored; a `#` line inside a JSONL section is refused (comments live in
the header); `//` comments inside query and mutate sections are simply GQ
text. Header lines are `#` lines before the first section, keys
`# issue:`, `# red_on:`, `# notes:`, `# traversal:`. `# issue:` is always
required; `# red_on:` is required when `# issue:` names a number and
optional under `# issue: none`; a header line is accepted exactly when
it equals `# <key>: <value>` byte for byte, for one of the four keys in
that spelling and a value with no leading or trailing whitespace, and
every other non-blank line is refused (a stray space is answered with the
canonical line, a bad shape with the grammar, an unknown key with the key
list; no line ever continues a previous entry); a key given twice is
refused, except `# notes:`, which repeats to carry a multi-line note;
`# notes:` and `# traversal:` are optional. `# issue:` takes a number in canonical
spelling (no sign, no leading zeros) or `none`; any other spelling is
refused. `# traversal:` takes `indexed` or `csr` and pins every step to
that mode, for cases whose subject is one traversal path (Execution
semantics owns the default).

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
refused; a range over 10 000 iterations is refused, and a case needing
more stays a Rust test); `--- foreach $x <v1> <v2> ...` iterates the
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
seed too large to sit inline belongs to the heavy-repro tier below, not
this format; there are deliberately no external-file references, or single
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
`rrf` in the order clause) and the case pins no traversal mode (below),
implemented as an exhaustive match over the
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

Traversal mode: by default a case runs on the production traversal
path, with no override, so the corpus exercises the path that ships. A
`# traversal:` header is an opt-in pin: every step of that case executes
through the scoped seam
`instrumentation::with_traversal_mode("indexed" | "csr", fut)`, and the
index step runs for it regardless of the constructs the steps use, so the
pinned path runs covered rather than on a fallback. The trade-off in one
sentence: the default corpus exercises the shipped path, and a pin
reproduces a mode-specific defect. The `OMNIGRAPH_TRAVERSAL_MODE` process
variable would reach an unpinned case, so every case fails with a
refusal naming it while it is set. The seam is task-local
and scope-bound, so concurrent cases never interfere; it is public
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
- `expect ordered`: positional comparison. A stable positional comparison
  needs two conditions, and `ordered` is accepted only where both hold.
  First, the row set is a deterministic function of the store and the
  params: true of every read operation today (`nearest` is exact, the
  vector index is built as `ivf_flat(1)`; `bm25` is a formula over
  index-global statistics; `rrf` fuses ranks the engine derives from arm
  row order, an assumption `query.rs` names), provided the case seeds
  distinct scores at any `limit` or scan-cap cutoff, for `nearest`,
  `bm25`, and `rrf` alike, where a tie would change the set itself (an
  authoring rule, since no expect mode can absorb a varying set); an
  operation that later stops being deterministic is refused by name in
  the harness, the way `@embed` is today. Second, given that set, the
  engine's order is total, which is an authoring rule: the `order` keys
  must be total over the rows the step returns. The `<var>.id` tie-break
  `apply_ordering` appends to every non-aggregate ordering is an
  implementation detail no expect may depend on (it is the `@key` value
  for keyed node types and a per-load ULID otherwise, so an unkeyed
  type's order changes across runs), an aggregate result batch carries no
  `<var>.id` column at all, so group rows tied on the sort key have no
  guaranteed order, and a tie on the sort keys surfaces as flakiness the
  harness cannot see statically (`ordered_two_key_sort.gqt` is the corpus
  example). The harness checks the parsed declaration and refuses
  `ordered` where no total order is possible: no `order` clause; an
  `order` clause led by
  `rrf()`, whose fusion sorts by score alone; and any aggregate in the
  `return` list (an `Aggregate` expression, the engine's own
  `projections_have_aggregates` definition). One authoring rule follows
  for `bm25`-led `ordered` steps: such a step must not follow a `mutate`
  step that adds or changes indexed text, because rows in fragments the
  index does not cover are scored by a different scorer and two score
  scales would rank together; such a case asserts with `unordered`.
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

The test target is `harness = false` and hands discovery to
`datatest-stable`: every `cases/*.gqt` file, rooted at the crate, is
registered at run time as its own libtest-compatible test (a libtest-mimic
trial under `datatest-stable`) named `case::<file>.gqt`. The runner it
calls (parser, execution, comparison, bless) is the crate's library,
`crates/omnigraph-gqt/src/lib.rs`, and the format self-tests are unit
tests beside it in `crates/omnigraph-gqt/src/tests.rs`; the crate is
`publish = false` and never built for release. Cases run on one shared
multi-thread tokio runtime whose worker stacks are 16 MiB (the engine's
query futures overflow the 2 MiB default; the value equals the CI jobs'
`RUST_MIN_STACK`, so the harness target does not depend on that
variable; tokio is already every engine integration test's runtime).
Case concurrency is libtest-mimic's, not the target's: each case opens its
own store and may build its indexes, so the number of cases in flight at
once is set by libtest's `--test-threads=<n>` flag, which
`datatest-stable` honors, independently of corpus size; that flag is a
runner knob, not format contract.
The per-PR corpus is the ***fast tier***: a case is expected to finish
in well under a second. The runner's per-case budget defaults to 10
seconds, generous against that expectation so a slow CI runner never
trips it; `OMNIGRAPH_GQ_CASE_TIMEOUT_SECS` overrides the default, and the
timeout failure message prints the budget in force. The elapsed time on each
ok/FAIL line, not the timeout, is the drift signal a reviewer reads. A
case that trips the budget belongs to the heavy-repro tier, defined below
in the Enforcement ladder, so slowness fails the PR introducing it
instead of accumulating in the required job. Each case's
outcome, a panic included, is caught and reported as that case's own
failure, which lets the target run every case before
failing. A corpus directory holding no case file makes the target panic
at startup with `no test cases found for test 'case'`,
`datatest-stable`'s own refusal, before any name filter runs (a broken
checkout or a bad rename, never a green run; `--exact` excepted: it
resolves the one name without scanning); a name filter matching
nothing runs zero tests and exits green, libtest's own behavior, where
the merged selector failed on an unmatched value. The `corpus_layout`
unit test fails on an empty corpus and on any entry that is not a
top-level regular `.gqt` file with a UTF-8 name (a symlink is foreign),
dot-prefixed `.gqt` names included; dot-prefixed
entries without the extension (`.DS_Store`, `.gitkeep`) are skipped (a
mis-renamed, nested, or dot-prefixed case must never silently skip). One
new dev-dependency, `datatest-stable` (bringing `libtest-mimic`,
`fancy-regex`, `camino`, `escape8259` into the lockfile), which takes
libtest's own arguments, so the workspace's `-- --nocapture` is accepted
as before (inert for this target); per-file test identity, the upgrade
path the merged design recorded (Decision log, 2026-09-03), is thereby
taken.

### Enforcement ladder

The contract enters `AGENTS.md`'s Change discipline section verbatim as
three sentences (these exact lines are what a reviewer approves and what
the CI gate cites); the section's existing bug-fix bullet is rewritten in
the same PR to defer to the second sentence, so the fix-carries-regression
rule keeps one phrasing:

> Query-behavior tests default to `.gqt` logic tests under
> `crates/omnigraph-gqt/cases/`; a Rust test needs a reason the
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
`--test` flag), with `dst-nightly.yml` as the workflow shape. Enrollment
has one rule: the filename glob discovers the targets, and the job
asserts that every `#[ignore]` message in a discovered target opens with
`heavy-repro:`, failing otherwise, so the third AGENTS.md sentence is
enforced rather than advisory. A red
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
member, `repro_issue_563`, landed with the #563 fix, so the nightly job's
precondition is met. That member's ignore messages open with `expensive:`
today; they are renamed to `heavy-repro:` in the PR that lands the
nightly job (Rollout).

The CI gate check and the `no-repro` waiver close the ladder (behavior in
User and operational behavior). The gate is `scripts/check-fix-regression.py`, a
Python script beside `check-agents-md.sh`, run by the `Fix Regression
Gate` job after its own self-test. The gate script also asserts the first
contract sentence is still present in `AGENTS.md` (a literal grep for the
corpus path `crates/omnigraph-gqt/cases/`), so deleting the
contract without deleting the gate fails closed. The ladder's recurring human
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
under the `RecoveryExecutor` write protocol. That `forbidden_apis.rs`
walk covers `crates/omnigraph/src/**` only, so the `omnigraph-gqt` crate
adds no registry entries; no deny-list item is affected, and no new
public API is added.
The target adds no shared state to the test suite (Execution semantics).

## Compatibility and reversibility

No storage or wire surface changes; the RFC is purely additive to tests,
CI, and contributor docs. Reverting means deleting the `omnigraph-gqt`
crate and its `members` entry in the workspace `Cargo.toml` (which drops
`datatest-stable` and its lockfile closure), the two workflow files with
their scripts, and the
AGENTS.md sentences; the logic test files remain readable, self-contained behavior records
either way. Format evolution is fail-closed: unknown sections, unknown
header keys, and missing required headers are refusals, never silent
skips, so an older harness refuses a newer logic test rather than
mis-running it.

Named compatible extensions, deferred until a real case demands each,
with maintainers deciding each one when the first case that needs it
forces the question:

- `require <capability>` guards for optional engine features.
- Loop nesting.
- A per-case float-precision override; scale 12 for every number is the
  contract until a case a fixed scale cannot express appears.
- A schema-IR vintage pin, for bugs that reproduce only on a legacy
  vintage.
- A heavy `.gqt` case: a header tier marker routing a scale-sized case to
  the nightly job instead of the per-PR job, with its own concurrency
  bound and timeout. Out of v1; the heavy-repro tier stays Rust until a
  case that trips the fast-tier budget has a reason to stay in the
  format.
- Run-twice verification: each query step re-run under a second
  execution configuration (a pinned traversal mode, index absence, or a
  forced canonical execution) with the row sets compared. Run-twice is
  deferred deliberately, not for lack of a seam: the engine's adaptive
  mid-traversal switching replaced the once-per-query mode choice such a
  check would have pinned, so the check is specified once, against that
  execution model; until then the modes-are-equivalent contract keeps its
  existing owner, `tests/proptest_equivalence.rs`.

Multi-connection interleaving stays out
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
- **An execution-proving gate for the Rust shape** (per closed issue, a
  filtered `cargo test --workspace -- issue_N` run beside the diff check,
  passing only when at least one matching test ran and passed): closes
  the Rust-shape gap above. Corpus shapes need no such gate: the required
  job proves them, and
  `cargo test -p omnigraph-gqt --test gq_logic_tests issue_N` selects one
  locally. Two costs: the gate re-runs on PR-body edits and label events,
  and where the diff check costs seconds, a filtered run compiles every
  crate's test targets (`--workspace`), not the `omnigraph-gqt` crate the
  test job already builds; and libtest's substring filter has no word
  boundary, so `issue_563` also selects `issue_5630`. Deferred as the
  upgrade path, taken if review ever finds a named regression that never
  executed.
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
- **libtest-mimic per-file tests:** taken, in the `datatest-stable` form
  (Decision log). One libtest test per logic test gives real
  test identity
  (`cargo test -p omnigraph-gqt --test gq_logic_tests issue_563` selects
  one case) and cargo-nextest compatibility; it costs `harness = false`,
  but `datatest-stable` accepts libtest's arguments, so the workspace's
  `-- --nocapture` is accepted as before (inert for this target).

## Evidence and tests

The harness proves itself on two fronts: the logic-test-expressible #563
regressions as the first corpus entries, each with a red state recorded
during the development of the #563 fix (their Rust twins landed with that
fix in `tests/search.rs`), and feature cases exercising every step kind
the format defines:

- `issue_563_aggregate_uncapped.gqt`: twenty matching chunks, `limit 2`,
  aggregate return, asserted with `unordered` (a single aggregate row has
  no order to assert, and `ordered` is refused on aggregates); red
  produced `total: 8` (the capped window), green produces `total: 20`.
- `issue_563_underfill_retry.gqt`: edges only on the middle band the capped
  scan window excludes; red (retry disabled) returned zero rows, green
  returns exactly `chunk-08` and `chunk-09`.
- `order_clause_aggregate_refused.gqt`, an `expect error` case pinning a
  refusal on the order clause: an aggregate
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
not a logic test: its symptom is a byte count, and it landed with the #563
fix as the `#[ignore]`d `repro_issue_563`, the heavy-repro tier's first
member.

Harness self-tests pin every refusal this RFC specifies (the File
format, Execution semantics, and Bless mode sections), one test per
refusal.

Docs follow the testing map: `omnigraph-gqt` has its own row in the crate
table of `docs/dev/testing.md`, the "Query results and operators" row of the
engine ownership table points at it, and its Commands section carries the
whole-corpus, one-case, and `--list` invocations (`check-agents-md.sh` keeps
the docs indexes honest, so no new orphan doc file).

## Rollout

1. **The implementation PR** (one PR, shipped as #596): the
   `gq_logic_tests` test target with the full format
   (steps, loops, restart), the bounded runner with its per-case budget
   and elapsed-time lines, the cases above, the self-tests, the
   three AGENTS.md sentences (logic-tests-by-default, regression per fix,
   `#[ignore]` species-in-message), the gate script, the two workflows
   (the test workflow carrying its classification copy, the gate workflow
   on `pull_request_target`; Decision log 2026-09-02), and the docs
   (`docs/dev/testing.md` rows, `docs/dev/ci.md`). Requiredness is wired the way this repo wires
   it: the `GQ Logic Tests` and `Fix Regression Gate` job names enter the
   `contexts` list in `.github/branch-protection.json` in the same PR
   (rationale recorded in `docs/dev/branch-protection.md`); both become
   required (seconds of hermetic cases should block a regression before
   merge). Two operator steps surround the merge, in order: before it, a
   maintainer with triage rights creates the `no-repro` label; after it,
   an admin runs `scripts/apply-branch-protection.sh`, which makes the two
   contexts required. Until both happen, a waiver PR has no label to
   carry and the contexts are not yet required. `implementation` advances
   to `partial`.
2. **The nightly heavy-repro job** (one workflow file): the glob-driven
   job in the Enforcement ladder, plus the rename of `repro_issue_563`'s
   ignore messages from `expensive:` to `heavy-repro:` so the tier's first
   member passes the job's prefix assertion. Its precondition (a first
   tier member on `main`) is met. `implementation` advances to
   `complete`.

## Unresolved questions

None before acceptance. The deferred extensions and their decider are
listed in Compatibility and reversibility.

## Decision log

- 2026-09-02, from review of the RFC PR: the fix-PR gate is a diff check
  whose execution guarantee differs by shape (a corpus match ran green in
  the required job; a Rust match is a naming check), and the Rust shape
  matches only top-level `crates/*/tests/<name>.rs` targets and
  `crates/*/src/**`, never helper or fixture modules.
- 2026-09-02, from review of the RFC PR: `expect ordered` is accepted
  only where the row set is deterministic and the engine's order is
  total; the harness refuses it on a query with no `order` clause, an
  `order` clause led by `rrf()`, or any aggregate in the `return` list.
- 2026-09-02, from review of the RFC PR: the walker bounds cases in
  flight and budgets each case (10 seconds by default, env-overridable);
  the per-PR corpus is the fast tier; a case runs on the production
  traversal path unless `# traversal:` pins one.
- 2026-09-02, amendment from the implementation PR (#596), after this RFC
  merged. Where the body above or an earlier entry differs from this
  entry, this entry holds. Each item names the section and the sentences
  it supersedes.
  - Fix-PR gate, Rust shape (User and operational behavior: "an added
    Rust line defining a function whose name carries `issue_N`", "a Rust
    shape is a naming check only", "no Rust test target other than
    `gq_logic_tests` runs on a pull request", the quotable guarantee; and
    the first 2026-09-02 entry's "a Rust match is a naming check"). An
    added function named for `issue_N` counts only when an added `#[test]`
    or `#[<path>::test]` attribute line (`#[tokio::test(...)]` included)
    sits directly above it in the same hunk. Other `#[...]` attribute and
    `//` comment lines may sit between; a blank line, a block comment, or
    an attribute split across lines breaks adjacency. A plain function,
    however named, never satisfies the gate. The merged shape accepted a
    plain `fn` named for the issue, which no pull-request job ever runs,
    so the gate could pass on a function that asserted nothing. The Rust
    shape is thereby a test-attributed definition, not a run. A pull
    request runs only the corpus walker and `Test omnigraph-server
    --features aws` among Rust test targets (`Test Workspace` runs
    post-merge). Workspace clippy on the pull request refuses an
    unreferenced private function but not an `#[ignore]`d or cfg-gated
    one, so whether that test runs in the suite and asserts the right
    thing stays with review. One named residue stays with review too: a
    definition inside an added multi-line block comment or raw string
    still matches, since the parse is line-based. The quotable guarantee
    reads, as amended: exit 0 exactly when every issue the body closes by
    keyword has an added `issue_N_*.gqt`, an added `# issue: N` line, or
    an added `#[test]`-attributed `issue_N` function, or the PR carries
    `no-repro`, and the AGENTS.md contract sentence is present; a corpus
    match means the case ran green in the required job, and a Rust match
    means a test-attributed function of that name was added.
  - Fix-PR gate, owners and strengthened regressions (User and operational
    behavior: "an added `.gqt` case", "`crates/*/tests/<name>.rs`",
    "`crates/*/src/**`"; the first 2026-09-02 entry's "matches only
    top-level `crates/*/tests/<name>.rs` targets and `crates/*/src/**`").
    The gate's paths cover `tools/*` workspace members the same way as
    `crates/*`. A regression counts when added or strengthened: a `.gqt`
    case named `issue_N_*`, new, or modified with at least one added body
    line (not a `#` header line or a `//` comment), or an added line
    carrying an alphanumeric character, not a comment or attribute, inside
    the body of an existing test-attributed function named for `issue_N`,
    located by the hunk's new-file line number in the file at the head
    commit (the enclosing item found by brace counting with literals and
    `//` comments blanked; raw strings and block comments are a named
    residue, and a non-function item found open first ends the search).
    An owner test not named for the issue is extended by renaming it to
    carry `issue_N` in the same change: the rename alone never counts, the
    rename plus the assertion does. The "added `# issue: N` header line"
    shape of the quotable guarantee is subsumed: the walker requires the
    file name to match and refuses a second `# issue:`, so that line only
    ever appears in a new case named for the issue, which the name rule
    already credits; the gate no longer names it. The merged shape
    required an added definition, so a fix that extended an existing
    assertion, the testing guide's preferred form, could pass only through
    `no-repro`. Owners the gate does not recognize, Python and shell
    scripts among them, satisfy it only through `no-repro`, which a
    maintainer applies; the docstring, `ci.md`, and `testing.md` say so.
  - Workflow layout (User and operational behavior: "a second job in the
    same workflow", "running only on `pull_request` events", "`pull_request`
    with its types declared explicitly (`opened`, `synchronize`,
    `reopened`, `edited`, `labeled`, `unlabeled`)", "label and body-edit
    events also re-run the test job", "honors the docs-only classification
    that `ci.yml`'s `Classify Changes` job defines"; Rollout: "the workflow
    with both jobs"). The gate is a policy check on the pull request, so it
    runs code the pull request cannot edit: it lives in its own workflow,
    `.github/workflows/fix-regression-gate.yml`, on `pull_request_target`,
    which takes the workflow file and `scripts/check-fix-regression.py`
    from the base branch and fetches the head only as data for the diff
    range, never checking it out or executing it. Under `pull_request` a PR
    could replace the check with `true` while keeping the required context
    name, and branch protection requires no approving review. One residue
    is accepted: a pull request can add a job of its own under the
    required name in a `pull_request` workflow, and branch protection keys
    on the name alone. That evasion is dominated by the `no-repro` label,
    which any committer can apply in one click: the gate guards against
    forgetting a regression, not against a committer who decides to skip
    one, and the base-owned workflow closes the forgetting-shaped hole (a
    PR that edits the workflow or the script cannot weaken the copy that
    runs). Gating the label itself would be a ruleset that identifies the
    gate by file rather than by name, and would gate both at once. The
    base-branch workflow does not exist until this change merges, so the
    gate first runs on the next pull request after it. The gate
    workflow declares the `edited`, `labeled`, and `unlabeled` types and
    builds nothing; `gq-logic-tests.yml` keeps only the code-bearing types
    (`opened`, `synchronize`, `reopened`), so a body edit or label change
    no longer re-runs the Rust build. GitHub Actions cannot make a job
    depend on another workflow's job, so `gq-logic-tests.yml` carries a
    verbatim copy of `ci.yml`'s classification as a job of its own,
    `Classify Changes (GQ Logic Tests)`, and the required `Classify
    Changes` context keeps one reporter. That workflow carries two jobs.
    `ci.yml` is the source of truth; `scripts/check-classify-copy.py`, an
    unconditional step at the top of the `GQ Logic Tests` job right after
    checkout (so it runs on documentation-only pull requests too), refuses
    drift.
  - Header grammar (Format: "a `#` line not starting a key continues the
    previous entry (a first header line starting no key is refused); any
    other `# <word>:` key is refused; a key given twice is refused"). No
    header line continues a previous entry: a multi-line note repeats
    `# notes:`, which is the one key allowed more than once. A header line
    is accepted exactly when it equals `# <key>: <value>` byte for byte,
    for one of the four keys in that spelling and a value with no leading
    or trailing whitespace; every other line is refused: a value with stray
    whitespace is answered with the canonical line, a bad shape with the
    grammar, an unknown key with the key list. The merged grammar sent a
    misspelled key
    (`# Traversal:`, `# traversal :`, `# traversal=csr`) into the
    continuation branch, where it was read as prose and dropped, so a case
    meant to pin one traversal path ran on the default path and passed.
    With no continuation branch there is no prose to fall into; the
    harness walks the typo space exhaustively (key spelling, separator,
    leading and trailing whitespace, gap) and asserts that exactly one
    line is accepted. The pin is also checked at execution time: a pinned
    step runs with expand-path probes attached (`QueryIoProbes`'
    `expand_indexed_runs` and `expand_csr_runs`, incremented where the
    executor commits to a path), and any expand on the other path fails
    the step with the message `pinned <mode>, ran <other> on N
    expand(s)`. The pin and the probes are both task-local scopes, so a
    boundary that dropped the pin would drop the probes with it and read
    as a clean zero; for that reason a pinned query step whose match
    clause carries an unbound traversal must also show at least one expand
    on the pinned path once it succeeds, else it fails with `pinned
    <mode>, but no expand ran on it`. A bound edge (`$a $k:knows $b`) scans
    the edge dataset on a path no mode pins and is exempt. A header that
    parses correctly proves the pin was requested; this proves the
    executor honored it. The `.gqt` grammar gains nothing.
  - Runner mechanics and Execution semantics (Runner mechanics: "the cap
    is a walker implementation detail, not format contract", "an
    environment variable overrides the default"; Execution semantics:
    "the `OMNIGRAPH_TRAVERSAL_MODE` process variable never reaches a logic
    test either way"). The in-flight cap defaults to the machine's
    available parallelism and `OMNIGRAPH_GQ_JOBS` overrides it; the cap is
    a documented runner knob, still no format contract.
    `OMNIGRAPH_GQ_CASE_TIMEOUT_SECS` overrides the per-case budget. The
    variable does reach an unpinned case, so the walker refuses to run
    while `OMNIGRAPH_TRAVERSAL_MODE` is set.
  - Comparison semantics, `ordered` (qualifies "the engine's order is
    total: an `order` clause qualifies when the source batch carries
    `<var>.id` columns"). Authoring rule for every `ordered` step: the
    `order` keys must be total over the rows the step returns. The
    `<var>.id` tie-break is an implementation detail no expect may depend
    on: it is the `@key` value for keyed node types and a per-load ULID
    otherwise, so an unkeyed type's order changes across runs. A tie on
    the sort keys is an authoring error that surfaces as flakiness, and
    the harness cannot see it statically (`ordered_two_key_sort.gqt` is
    the corpus example).
- 2026-09-03, amendment from the PR that moved the corpus and runner into
  `omnigraph-gqt`, after #596 had merged. Each item names the design
  sentences it supersedes; path and command spellings changed with the
  corpus move throughout. Where the body or any earlier entry differs
  from this entry, this entry holds.
  - Summary, User and operational behavior (Running), Enforcement ladder,
    Runner mechanics, Compatibility and reversibility, and the
    libtest-mimic bullet of Alternatives now describe
    the taken shape: the corpus and the runner live in a dedicated
    workspace crate, `omnigraph-gqt` (`publish = false`, not a default
    member, never in the release build; corpus at
    `crates/omnigraph-gqt/cases/`), and every case file is its own
    libtest-compatible test named `case::<file>.gqt`, registered at run
    time by `datatest-stable` under `harness = false`.
    `cargo test -p omnigraph-gqt --test gq_logic_tests <substr>` selects
    cases by file name, `-- --list` names them, cargo-nextest sees each
    case, and an IDE's test-results view lists each case from
    the libtest-shaped output (no per-case gutter runnable exists, since
    no source item does). Discovery stays at run time, so a case-only
    pull request still needs no Rust change; the gate script's corpus
    path and the AGENTS.md contract sentence moved with the corpus.
    Superseded: Summary "One test target,
    `crates/omnigraph/tests/gq_logic_tests.rs`, walks
    `tests/gq_logic_tests/*.gqt`"; User and operational behavior
    "`OMNIGRAPH_GQ_LOGIC_TESTS=issue_563` restricts the run to cases whose
    file name contains the value" and "The lines reach the terminal under
    `--nocapture`; a plain `cargo test` shows them on failure";
    Enforcement ladder, the corpus path
    `crates/omnigraph/tests/gq_logic_tests/` in the first AGENTS.md
    sentence and in the gate grep; Runner mechanics "The walker is a
    single `#[tokio::test(flavor = "multi_thread")]` entry point",
    "Zero new dependencies and an ordinary libtest harness", "lists
    `tests/gq_logic_tests/*.gqt` rooted at `CARGO_MANIFEST_DIR`", "the
    `JoinSet` surfaces task panics as join errors", and "The walker fails
    when its glob matches no files"; Compatibility and reversibility
    "Per-file test identity via libtest-mimic"; Alternatives
    "Deferred, not rejected: the env-var filter covers selection"; the
    third 2026-09-02 review entry's "the walker bounds cases in flight";
    the 2026-09-02 amendment's "A pull request runs only the corpus
    walker and `Test omnigraph-server --features aws`" and "the cap is a
    documented runner knob".
  - Runner mechanics (the 2026-09-02 entry above: "the in-flight cap
    defaults to the machine's available parallelism and
    `OMNIGRAPH_GQ_JOBS` overrides it"): the semaphore walker is gone; case
    concurrency is the `--test-threads=<n>` flag, libtest's spelling,
    which `datatest-stable` honors, and `OMNIGRAPH_GQ_JOBS` no longer
    exists. `OMNIGRAPH_GQ_LOGIC_TESTS` no longer exists either; the
    libtest name filter is the selector, and a filter matching nothing
    runs zero tests and exits green where the merged selector failed on an
    unmatched value. `OMNIGRAPH_GQ_BLESS` and
    `OMNIGRAPH_GQ_CASE_TIMEOUT_SECS` are unchanged, and every case fails
    with the refusal while `OMNIGRAPH_TRAVERSAL_MODE` is set (superseding
    the 2026-09-02 amendment's "the walker refuses to run").
  - Runner mechanics, stack (new, supersedes nothing): each case runs on
    one shared multi-thread tokio runtime whose worker stacks are 16 MiB.
    The engine's query futures overflow the 2 MiB default even when
    spawned as tasks; the value matches the CI jobs' `RUST_MIN_STACK`, so
    a local run no longer depends on that variable.
  - Evidence and tests (new, supersedes nothing): the harness self-tests
    are the crate's unit tests in `crates/omnigraph-gqt/src/tests.rs`; the
    per-case budget, panic capture, and corpus layout (no foreign entry,
    never empty) each keep one test; the traversal-override and
    foreign-entry tests stay.
- 2026-09-04, amendment from the CI pull-request-tier PR, after #596 and
  #607 had merged. Where the body or any earlier entry differs from this
  entry, this entry holds.
  - CI (User and operational behavior, Enforcement ladder): `Test
    Workspace` (`ci.yml`) runs on every pull request that is not
    documentation-only, as a reporting context, and again after merge, on
    tags, and by dispatch; every Rust test target in the workspace
    therefore executes on a pull request. `GQ Logic Tests` stays the
    required per-PR context for the corpus: it exists because only a
    required context blocks a merge, no longer because the workspace suite
    skipped pull requests. The `Fix Regression Gate` is unchanged: a Rust
    match is still a test-attributed definition check, the gate consults
    only the required contexts, and whether the matched test asserts the
    right thing stays with review. Superseded: User and operational
    behavior "but does not run on pull requests (`ci.yml`); regressions
    must be exercised before merge" and "(`Test Workspace` runs
    post-merge, CI above)"; the 2026-09-02 amendment's "(`Test Workspace`
    runs post-merge)" and its "no Rust test target other than
    `gq_logic_tests` runs on a pull request" premise; the 2026-09-03
    amendment's "A pull request runs only the corpus walker and `Test
    omnigraph-server --features aws`" as a description of what runs.
- 2026-09-04, amendment from the PR that scoped the Fix Regression Gate to
  the code paths, after the CI pull-request-tier PR. Where the body or any
  earlier entry differs from this entry, this entry holds. Trigger: a
  workflow-only fix that closed its issue by keyword (#594, two files under
  `.github/workflows/`) was red on the gate with no way through but the
  `no-repro` label, which only a maintainer can apply; the gate was
  demanding a test that no location could hold.
  - Code paths (User and operational behavior, Fix-PR gate): before any
    closed issue is examined, the gate lists the paths the diff changes
    (`git diff --name-only --no-renames`) and passes the PR unexamined,
    with a `::notice` annotation naming the closed issues, when none is
    under `crates/` or `tools/` (Markdown files there excluded) and none is
    the root `Cargo.toml` or `Cargo.lock`. Those are where every workspace
    member lives (`Cargo.toml` `[workspace] members`; the self-test asserts
    each member sits under a code path, so a member added elsewhere turns
    the gate red on its next run), so they are the only paths a `.gqt`
    case or a Rust test can witness a change in. A PR that changes a
    workflow and a crate is examined as before, as is one whose only
    code-path change is `Cargo.lock`. `scripts/`, `deploy/`, `docker/`,
    `benchmarks/` (fixtures and suites, no member), `.github/`, `docs/`,
    and root files other than the two manifests are outside; a rustdoc-only
    change inside a `.rs` file is not told apart from code and goes through
    the label.
  - Closing forms: `#N`, `OWNER/NAME#N`, and
    `https://github.com/OWNER/NAME/issues/N`, the three GitHub's parser
    closes on, for the repository the gate runs in (`--repo`, refused
    unless `OWNER/NAME`; the workflow passes `GITHUB_REPOSITORY`; with
    neither, `#N` only and a `warn:` line). A reference to another
    repository closes nothing here and is not read. Residue, named in the
    body: `GH-N`, bare `fixes#N`, autolink and Markdown-link forms,
    `http://` and `www.` URLs, commit-message keywords, manual closes; a
    keyword inside a code span, fence, or HTML comment is read; a
    non-default base branch is examined.
  - Failure text: names the code paths that made the gate look, the ways
    through as a numbered list, near misses the diff holds (a case whose
    header says `# issue: N` under another name or a subdirectory; a test
    named with the bare number, moved rather than added, under a leading
    `_`, or in a helper module; an issue-named function with no added test
    attribute directly above it), and a case skeleton; emitted once as the
    log line and once as a `::error` annotation. Near misses are named,
    never credited: the match rules are unchanged.
  - PR template: the "Fixes an accepted issue" line says what a fix under
    the code paths must carry.
  - Guarantee: exit 0 exactly when the diff changes no code path, or every
    issue the body closes by keyword has its matching addition, or the PR
    carries `no-repro`; and in every case the AGENTS.md contract sentence
    is present.
  - Superseded: "and `#N`" as the whole target; "The gate reads only that
    form in the PR body: closings by full URL, `owner/repo#N` reference,
    ... pass unexamined"; "owners the gate does not recognize (Python and
    shell scripts among them) satisfy it only through `no-repro`" (a
    script outside a crate is now outside the code paths; one under a
    crate still goes through the label); "docs-only fixes" as a reason the
    label exists, narrowed to a rustdoc-only change inside a crate; the
    quotable guarantee's two-way form.
