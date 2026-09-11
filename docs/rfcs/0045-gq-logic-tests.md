---
rfc: "0045"
title: "GQ logic tests"
track: maintainer
status: draft
implementation: partial
authors:
  - azimafroozeh
created: 2026-08-29
updated: 2026-09-10
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

This amendment makes ***agent experience (AX)***, an agent's ability to
construct, execute, diagnose, and reproduce a case without guessing,
the format's design priority. Cases state the execution target, storage,
and fault conditions that matter to a failure. Concision removes repetition, not
required evidence. Convenience for a human author does not justify hidden
configuration, automatic repair, or a weaker assertion.

The proposed environment and fault contract is specified in Design.
It extends the existing format; its target combinations are not a claim
of implemented support. GQ declarations, schemas, and result comparison
retain their existing owners.

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

A fault reached during setup instead of the intended merge tests a different
failure. A case rerun on local files instead of the original object store
tests different storage behavior. A seed without the case and execution
configuration cannot identify the original experiment. File-owned controls
and explicit execution evidence address these ambiguities.

## User and operational behavior

For the proposed environment extension, author one set of steps and
expectations, list exact target/storage combinations before the schema,
and place a fault immediately before its target operation. The harness
validates all selected combinations first. An unsupported combination is
a named failure, never a skipped assertion or a fallback environment.

| Attempt to bypass the contract | Required refusal or evidence |
|---|---|
| Request DST from a build without it | Refuse before case setup. |
| Omit runner configuration to inherit a backend | Refuse before case setup; every case declares its execution. |
| Point an S3 case at another backend | Verify actual backend against the file. |
| Match injected text without firing the hook | Require correlated typed delivery evidence. |
| Run one environment and claim full coverage | Report the exact selection and omitted executions. |
| Hide a failed write with retry or reopen | Execute only authored operations; no harness recovery. |

| Supported author route | Accepted behavior |
|---|---|
| Existing engine/filesystem case | Add its explicit engine/filesystem environment before execution. |
| Explicit environment list | Fresh graph and shared assertions for each supported entry. |
| Fault before one operation | Injection, delivery verification, and cleanup at that operation. |
| Exact environment/seed rerun | Declared subset only, with complete reproduction context. |


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

Running from `crates/omnigraph-gqt`, whose Cargo configuration enables DST:

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
or error mismatch. Each execution stops at its first failing step (later
steps would use state the failed step no longer vouches for); explicit
environments follow the replay and continuation rules in Design;
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
so bless converges over reruns for row-body and shape mismatches; a
header-line mismatch stops its case until hand-edited. It rewrites the
body the failing check owns: the shape body when the shape check failed
(one line per executed column, the name as executed, the `.pg` spelling of
the Arrow type, `?` exactly when the column holds a null cell; the
executor's nullable flag is not read (Comparison semantics says why), so a
hand-written line without `?` survives where the data holds no null; a
column whose Arrow type has no `.pg` spelling fails bless with the type
named), the rows body when the rows failed, and nothing when the computed
check failed (that red is the executor disagreeing with the compiler, which
no expectation should absorb), in
the comparison's normalized form (scale-12 decimals with trailing zeros
trimmed, object keys sorted, null cells omitted, one row per line), in
canonical sorted row order for `unordered` expects and the run's
positional order for `ordered`. Bless pins the executed schema as it
stands, so a blessed shape can pin a wrong type, which the reviewed diff is
the gate for: a shape rewrite outside a migration PR is a review flag, and
the reviewer maps each rewritten line to the return clause's projection
and its `.pg` declaration. Header-line expectations (`error:`
substrings, `affected:` counts) stay hand-written, since pasting a full
error message would defeat the stable-fragment rule in Comparison
semantics. Bless never runs in CI; the diff of the logic test file is the
review gate, and `red_on:` provenance stays hand-written. Bless never
changes a step's kind: when the run errors while the step expects rows or
counts, or the reverse, it reports the mismatch instead of rewriting.
Bless refuses cases containing loops (one expect body serves every
iteration).

CI: the required per-change job `GQ Logic Tests` in
`.github/workflows/gq-logic-tests.yml` owns the complete corpus and runs
`cargo test -p omnigraph-gqt --locked -- --nocapture`
on every code-bearing PR, from `crates/omnigraph-gqt`,
where its `.cargo/config.toml` enables seeded Tokio. The job must build
the `omnigraph-gqt` worker binary and dispatch that executable from the same
build. The workflow triggers on push to `main`, `workflow_dispatch`,
and `pull_request` with only its code-bearing types declared (`opened`,
`synchronize`, `reopened`); the gate below lives in its own workflow and
declares the body-edit and label types itself, so a body edit or a label
change never re-runs the Rust build. The test job compiles the engine
crate, the `omnigraph-gqt` library, and its test binaries: minutes
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
repo's workflow-pin check. Explicit fault/DST enrollment requires the
test-only failpoint and seeded-runtime build owned by the environment
extension; fault-free engine execution alone needs neither. On a code-bearing
run,
a green `GQ Logic Tests` job guarantees that every corpus case passed
admission, every declared environment and mandatory seed/replay executed,
and each execution either matched all expectations or satisfied the explicit
Known recovery failures contract. Known failures remain separately reported;
they do not establish that the graph defect is fixed. The docs-only success above
reports classification, not corpus execution. A separate DST package job
or an environment-filtered GQT invocation cannot discharge this guarantee.
`Test Workspace` excludes GQT from both its compile and test commands with
`--exclude omnigraph-gqt`; the separate required context owns that package.
The GQT job also runs unit and dispatch tests from the workspace root to
prove that an unconfigured build refuses requested DST execution, and runs
Clippy in the configured build. Both dispatcher and worker in the complete
corpus run must use the seeded build configuration. This changes build
ownership, not coverage: every corpus case remains enrolled in the required
GQT job. The unavailable-runtime refusal check cannot substitute for that run.

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
required contexts (`Test Workspace` runs the remaining workspace targets on
the pull request as a reporting context, CI above); a test-attributed
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

--- runner
timeout_ms: 10000
environments:
  - target: omnigraph-engine
    storage: local-filesystem

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

--- expect shape
total: I64
```

A multi-step feature case, showing mutation steps, a restart, and a loop:

```
# issue: none
# notes: pins that committed writes survive a store reopen.

--- runner
timeout_ms: 10000
environments:
  - target: omnigraph-engine
    storage: local-filesystem

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

--- expect shape
p.name: String
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
refused. `# traversal:` takes `indexed` or `csr` and pins every
declaration step to that mode, for cases whose subject is one traversal
path (Execution semantics owns the default); a statement step traverses
nothing and runs outside the pin.

A file is: required `--- runner` (Explicit execution environments),
optional `--- known_failure` (Known recovery failures), then `--- schema`, then
`--- seed`, then one or more steps, of
which at least one is a query or mutate step; a file missing any of these
three leading sections, ordering them differently, or carrying no query or
mutate step (nothing would be asserted, a restart-only step list
included) is refused. A `--- fault` may precede an operation as specified
in Faults at an explicit step. A step is one of:

- `--- query` holding exactly one GQ declaration with a read body, followed
  by an optional `--- params` section (JSON object) and a mandatory
  `--- expect` section with mode word `unordered`, `ordered`, or
  `error: <substring>`, where the substring is the trimmed remainder of the
  header line and the section body must be empty. A ***rows step*** (an
  `unordered` or `ordered` expect) is followed by a mandatory
  `--- expect shape` section, the ***shape section***: one
  `<name>: <type>` line per result column in `return`-list order, in `.pg`
  property syntax with `?` permitting a null cell (`p.age: I32?`); a `?` on
  a `.pg`-nullable property is never wrong, and a line without `?` asserts
  that no cell is null. The name is the executed column name: `p.name` for
  an unaliased property or an aggregate over one, `p` for an aggregate over
  a bare variable (`count($p)`), the alias for `expr as alias`, `literal`
  for an unaliased literal, `x` for a bare `$x`, `__nanograph_now` for
  `now()`. A bare node projection (`return { $p }`) is spelled with its node
  type name, `p: Person`: the column must be that type's node object, a struct
  whose fields are the identity column and the declared properties minus
  `Blob` and `Vector`, in the catalog's order; it takes no `?` (the object is
  never null), and bless spells a struct column back as the one node type
  whose object it is. Otherwise the type is
  a `.pg` `type_ref` (`schema.pest`) parsed by the product schema parser as
  the one property of a `node Shape { }` declaration the runner wraps around
  it; annotations, body constraints, `enum(...)` (its Arrow type is `Utf8`,
  write `String`), `Blob` (not a read value, T24), comment lines, and `${`
  are refused, blank lines are ignored, and an empty body is accepted as the
  bless target (it asserts zero columns and is never green). An aggregate's
  type is `I64` for `count`, `F64` for `sum` and `avg`, and the argument's
  type for `min` and `max`; a literal's is the compiler's literal inference
  (`I64` for an integer, `F64` for a float, `String`, `Bool`, `Date`,
  `DateTime`). A shape section anywhere but directly after a rows expect is
  refused, and so is a rows expect without one; the latter refusal names the
  two routes: write it from the `.pg` schema, or fill it with
  `OMNIGRAPH_GQ_BLESS=1` and review the diff.
- `--- mutate` holding exactly one GQ declaration with a mutation body,
  followed by an optional `--- params` and a mandatory `--- expect` with
  mode word `ok` (success, counts unasserted),
  `affected: nodes=<N> edges=<M>` (success with both counts asserted; both
  are required, enumerated rather than summed), or `error: <substring>`.
  Mutation results carry no rows (`MutationResult` is the two counts), so
  there is no row expect on a mutate step.
- `--- restart`, body empty: drop the store handle and reopen it from the
  same URI before the next step.

`--- query` and `--- mutate` take one optional argument, `branch: <name>`
(a word, a colon, the trimmed remainder, the shape of `error:
<substring>`), the branch a declaration step runs against; absent, `main`.
The name is taken verbatim and unquoted, so a `/` needs no quoting there
and a quoted name targets a branch whose name carries the quotes.
Anything else after the section name is refused with the grammar. A
`--- mutate` step may hold a ***control write*** (`branch create`, `branch
delete`, `branch merge`) and a `--- query` step may hold `branch list`, the
branch statements of RFC 0055; the compiler classifies the body, and the
wrong kind is refused beside the read/mutation refusals (`a control write
under `--- query` is refused; use `--- mutate``, ``branch list` under
`--- mutate` is refused; use `--- query``). A statement step takes no
`branch:` argument (`a branch statement names its branches itself; drop the
`branch:` argument`) and no
`--- params` (`a branch statement takes no params`); its name in labels is
the statement's two words. `branch create` and `branch delete` take `ok` or
`error: <substring>`; `branch merge` takes `ok`, `error: <substring>`, or
`outcome: <word>`, body empty, `<word>` one of `already_up_to_date`,
`fast_forward`, `merged`, asserting the engine's merge outcome. `ok` on a
`branch merge` is satisfied by a no-op merge too (`already_up_to_date` is a
success), so `outcome:` is what pins a landing; `outcome:`
on any other step is refused, and so is `affected:` on a control write (no
counts exist). `branch list` is a rows step: it takes `unordered`,
`ordered`, or `error: <substring>` over rows `{"name": "…"}`, its rows are
sorted by `name` in byte order (a total order, so `ordered` is accepted and
the `order`-clause refusal for declarations does not apply), and it
carries the mandatory shape section, one line, `name: String`. A statement
step has no params, so an iterating value never reaches its body; `${` in a
quoted statement name parses and is refused by the runner's `${` fence, and
`${` anywhere else in a statement body does not parse.

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
`null` for it. A result row omits a null cell's key (the Arrow JSON writer,
RFC 0051), and expected rows are written the same way; the column's type
and its `?` live in the step's shape section, which is why a rows body
alone can never pin a type.

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

### Explicit execution environments

A ***test environment*** is one execution target, storage backend, and
configuration for a complete case. Exactly one required `--- runner` YAML
section before `--- schema` declares the environments. Every case, including
an existing fault-free engine case, carries it. The following syntax specifies
the amendment contract; the examples do not establish implementation
qualification.

```yaml
--- runner
timeout_ms: 10000
environments:
  - target: omnigraph-engine
    storage: local-filesystem
  - target: omnigraph-engine-dst
    storage: in-memory-object-store
    seeds: [0, 42]
```

`timeout_ms` and `environments` are required in every case.
An environment requires `target` and `storage`, plus the parameters required
by that target. Its full parameter set defines the environment; there is no
separate environment ID. Exact duplicate entries are refused. There is no
runner format version: the parser and corpus migrate together, and replay
requires the recorded source and executable identities. Omitted configuration
has no default execution route. There are 1 to 16 environments, executed in
listed order. The file budget is 1 to 600000 milliseconds of wall time,
starting before case reading and preflight. It measures profile resolution,
setup, every environment, both executions of every DST seed, and ordinary
teardown through the supervisor's final execution check. The supervisor checks
the remaining budget before each worker dispatch and after execution. Expiry
fails the case; unstarted executions are reported as not run. Worker execution
is interruptible through process containment, which has a separate bounded
allowance and cannot extend the case into success. Synchronous host-file reads,
executable hashing, and file cleanup are not interruptible; their elapsed time
counts when the supervisor next checks the budget. Terminal summary encoding
and publication happen outside this execution budget. This is not a hard bound
on total command wall time. Fast-tier admission independently
limits which budgets may enter the per-PR corpus, as defined in Runner
mechanics; selection cannot bypass that check.

Each environment starts from a fresh isolated graph, applies the schema
and seed, performs the existing index setup, then executes the complete
step list. Branch targets, parameters, rows, result shapes, affected
counts, and expected errors have one definition shared by all environments.
Each environment is checked against the file's expectations. Cross-target
identity of generated IDs, timestamps, or internal operation order is not
asserted. A target-specific expected result belongs in a separate case.
Target and storage selection cannot change inside a case execution.

| Target | Execution |
|---|---|
| `omnigraph-engine` | Direct calls to `Omnigraph`. |
| `omnigraph-engine-dst` | Direct engine execution under the seeded DST environment. |
| `omnigraph-server` | Requests through the server's HTTP API. |
| `omnigraph-server-dst` | Requests through the server's HTTP API with server tasks and its embedded engine under a qualified seeded DST environment. |

`target` is the only execution selector. There is no separate `runtime` or
DST-mode field. The server embeds `Omnigraph`; these names do not assert an
independent server-to-engine network connection. Server-DST requires its own
execution qualification; engine-DST qualification alone cannot establish it.

| Storage | Meaning |
|---|---|
| `local-filesystem` | A fresh temporary directory on the local filesystem. |
| `in-memory-object-store` | An injected object store whose contents live in process memory. |
| `s3-compatible` | An explicitly configured S3-compatible service. |
| `azure-blob-storage` | An explicitly configured Azure Blob service or emulator. |

These names are GQT configuration values. `StorageAdapter` is the existing
Rust interface, and `ObjectStorageAdapter` provides these storage
implementations. Selecting a control-object adapter alone is insufficient:
the environment must connect Lance dataset I/O to the corresponding
isolated storage too. The storage name does not certify a provider's
compatibility or change its existing qualification status.

| Target roadmap | Local filesystem | In-memory object store | S3-compatible | Azure Blob |
|---|---|---|---|---|
| `omnigraph-engine` | Intended | Intended | Intended | Intended |
| `omnigraph-engine-dst` | Refused | Intended | Refused | Refused |
| `omnigraph-server` | Intended | Refused | Intended | Intended |
| `omnigraph-server-dst` | Refused | Intended | Refused | Refused |

`Intended` means potential support gated by Rollout; it does not mean
implemented or qualified. Initial implementation admission is limited to
`omnigraph-engine` with `local-filesystem` and no fault directives, and
`omnigraph-engine-dst` with `in-memory-object-store`. Every other combination
must report `unsupported_environment` before case setup. Further targets,
storage combinations and controls require their own acceptance evidence.
A requested combination unavailable in the running build
fails before case setup. There is no automatic substitution or skip.

S3, Azure, and every server environment additionally require `profile`.
A ***connection profile*** supplies connection details and test lifecycle
control outside the case. It cannot override target, storage, seeds,
faults, assertions, or the file budget. The file carries the profile name;
the report carries its resolved non-secret configuration and digest.
Credentials stay outside the case and report. Profiles distinguish actual
services, including different S3-compatible providers and Azure emulators.
They record effective storage options, including request retry settings.
Changing a provider or these options changes the recorded environment.

For example, a future HTTP environment can declare:

```yaml
  - target: omnigraph-server
    storage: s3-compatible
    profile: gqt-server-s3
```

The profile must verify the server's actual backend and provide an
exclusively owned graph with fresh schema and seed state. The server is
cluster-only; graph provisioning follows cluster configuration and
lifecycle controls, not an invented graph-creation HTTP endpoint. Reuse
the server test support's lifecycle owner. A URL and credentials alone
cannot satisfy this contract. A profile that cannot prove isolation,
required result types, or a requested test control is refused.

#### Seeds and reproducibility

`seeds` is required for `omnigraph-engine-dst` and `omnigraph-server-dst`:
1 to 64 distinct unsigned 64-bit integers. Other targets reject it.
Declaring seeds does not make an unsupported target admissible. Each seed
executes twice in
fresh child processes, with seeded scheduling, engine IDs, engine time,
and in-memory storage setup. Process-global initialization happens after
the child receives its resolved configuration. Known ambient overrides
of test semantics, including inherited `FAILPOINTS` and traversal pins,
are refused rather than silently combined with the file.

DST replay first requires equal validated input and build identities,
as defined in Validation and agent diagnostics. It then compares all
assertion-relevant actual observations: rows, executed result types,
affected node/edge counts, and error codes with their compared payloads.
It also compares main snapshot IDs, the reached operation sequence, and
the final verdict. Each observation identifies its step ordinal and loop
iteration, when present. Verdict includes the failure classification and
structured assertion failure values; equality cannot rely on readable
error text alone. A mismatch fails even when both executions fail the
same assertion. Missing required observation evidence fails explicitly.
This observation comparison does not prove equality of every storage call,
unqueried branch state, or Lance thread schedule. `omnigraph-engine` and
`omnigraph-server` runs have no deterministic replay guarantee. Their value is repeatable
inputs and recorded conditions with the same explicit assertions.

The harness issues each authored operation once. It never adds a mutation
retry or reopens `Omnigraph` after an error. Engine and storage-client
retries remain their production behavior, with effective configuration
recorded. An authored retry is another explicit operation step.

### Shared seeded execution

`omnigraph-dst::run_universe(&environment, &scenario)` executes one selected
seed. The environment supplies configuration and resource setup through
`UniverseEnvironment`; the scenario supplies operations and checks through
`UniverseScenario`. This entry point is for seeded execution. The ordinary
engine target retains its ordinary runtime and the same GQT step comparisons.

The caller must establish process isolation, process-start pool and entropy
settings, failpoint registration, and the invocation wall deadline before
entering the universe. Environment setup must not attempt to repair already
initialized process settings. GQT's supervisor continues to own immutable input
validation, seed/replay expansion, containment, terminal reporting, and the
continuation rules in Validation and agent diagnostics.

The executor installs seeded scheduling, clock, IDs, and entropy before
constructing per-execution storage and controls. Configuration is not a live
store. `MemoryEnvironment` creates a fresh control-object adapter and requires
an empty, exclusively owned Lance graph namespace. It refuses existing graph
contents rather than deleting them during setup. Both storage paths remain
alive across a scenario's handle reopen. Teardown removes only that owned
Lance namespace after observations are collected; it does not reset shared
backend ETag counters or incomplete multipart state. GQT's fresh worker process
per attempt remains required for its replay contract.

Setup must guard partial acquisitions. `UniverseRun` retains the execution
phase, scenario/setup outcome, and cleanup outcome separately, including
original panic payloads. Teardown runs after a completed scenario returns or
unwinds; a cleanup failure must not replace its original failure evidence.
Required Rust census and fault-counter handles remain alive until their final
checks finish. A hung or killed process cannot establish cleanup through Rust
guards; the supervisor must apply the bounded containment rules above.

The environment's selected root seed is authoritative. The existing Rust
`harness::run_universe(root, scenario)` boundary translates `Scenario.seed`
once into the environment, preserving the runtime, ULID, workload, and entropy
child-seed draw order. Explicit `FaultPlan.seed` values keep their independent
meaning. Rust generated scenarios and GQT scenarios retain their own loops,
fixture setup, checks, and outputs; neither language is interpreted by the
shared executor.

### Faults at an explicit step

A ***fault directive*** is a `--- fault` YAML section immediately before
one query or mutate step. It arms a named engine hook only during that
operation; schema creation, seed loading, and earlier steps cannot consume
it. The following example begins after branch setup:

```text
--- fault
at: branch_merge.post_sidecar_pre_fork
occurrence: 1
action: return_error
scope: next_step

--- mutate
branch merge source into target

--- expect error: injected failpoint triggered: branch_merge.post_sidecar_pre_fork

--- mutate branch: target
query unrelated_write() {
    insert Marker { name: "after_failure" }
}

--- expect affected: nodes=1 edges=0
```

All four fault fields are required. `scope` accepts only `next_step`;
`action` accepts only `return_error`. `occurrence` is 1 to 1000000 and
counts crossings of that hook attributable to the selected operation,
including its production retries. It starts at zero when the operation
is armed. The selected crossing injects once; subsequent crossings do
not inject. Target and storage do not change this meaning.

The hook must be supported for the operation and selected environment.
No caller-supplied code or arbitrary failpoint action string is accepted.
Initial hook ownership is the prototype's merge hooks
(`branch_merge.post_authority_capture`,
`branch_merge.post_sidecar_pre_fork`,
`branch_merge.post_effects_pre_confirm`,
`branch_merge.post_phase_b_pre_manifest_commit`) and mutation hook
`mutation.post_sidecar_pre_fork`. New hooks require an implementation,
scope proof, and negative tests before the parser accepts them.

The selected operation must finish before fault cleanup can be considered
complete. On cancellation or timeout, stop the isolated worker or quarantine
the dedicated server graph until outstanding work has stopped and hooks are
disarmed. Use a bounded supervisor cleanup deadline; failure to establish
cleanup prevents reuse and is reported, never an unbounded wait. A killed
worker or quarantined graph does not count as a successful replay.

The operation must observe the exact injected error through a typed error
or an equivalent correlated server result. Matching text in query rows or
an unrelated error is insufficient. The runner verifies delivery and
disarms the hook before the following operation, including after an
assertion failure. An unreached hook, failed cleanup, timeout, or lost
delivery evidence fails the execution. A fault that leaked into another
operation or graph fails isolation even if the expected rows match.

One fault directive attaches to one operation; adjacent directives,
directives before restart/setup, orphan directives, and directives inside
loops are refused in this phase. Multiple faults in one operation and
faults during setup remain out of scope. A case may contain up to 16
fault directives at separate operations. No fault state survives a
restart, environment change, seed, or replay.

Initial `omnigraph-engine` admission rejects faults. A future fault-capable
direct-engine implementation can use process isolation where failpoints are
global. A server implementation needs equivalent isolation and explicit
operation attribution; a shared server-wide toggle does not satisfy it.
This amendment defines no production fault-control endpoint. HTTP fault
execution stays unavailable until the test-only lifecycle/control owner
proves this contract.

`--- restart` continues to mean closing the graph handle and reopening
the same stored graph. It does not mean process crash or HTTP reconnect.
In-memory contents must survive that handle reopen. A server environment
must perform the equivalent graph reopen or refuse the case. Process
crashes, multi-connection interleavings, and randomized fault discovery
remain outside this format extension.

### Known recovery failures

An optional `--- known_failure` section immediately after `--- runner` records one
known recovery failure while keeping the healthy operation expectations.
It is a narrow corpus admission rule, not an expected-error assertion:

```yaml
--- known_failure
step: 4
match:
  error: RecoveryRequired
  reason: "the exact OmniError::RecoveryRequired reason"
```

`step` and `match` are required, and other fields are refused. `step` is a
positive operation ordinal. `match` is a typed error matcher: the only supported
variant is `error: RecoveryRequired`, with a nonempty exact `reason` of at most
2048 bytes. It names `OmniError::RecoveryRequired` directly; there is no wildcard
or fallback variant. Unknown error names, including `Unknown`, are refused,
as is the old `--- fixme` syntax. The existing `# issue` and `# notes` headers
provide issue identity and context; `none` remains valid for an unassigned
issue. The marker does not repeat notes or contain the generated operation ID.

Admission requires only `omnigraph-engine-dst` with
`in-memory-object-store`, no loops, and an ordinary mutate at the declared
step with its healthy `ok` or `affected` expectation. At least one supported
fault must precede that step. Faults at or after the marked step are refused.

An execution qualifies only when every preceding assertion passes, every
declared fault has exact typed delivery evidence at its own operation, and
the marked assertion fails because that mutate returned the typed
`OmniError::RecoveryRequired` variant with exactly the declared reason.
The operation's typed error and the failed assertion must identify the same
step and actual error. Matching text in data, a different error variant,
another reason or step, and missing fault evidence never qualify.

The step loop still stops at its first failure; later healthy assertions
remain unchanged and unexecuted. Every selected seed and mandatory fresh
replay must qualify. The full raw worker reports, including their assertion
failures, operation IDs and other actual evidence, must still match. An
accepted attempt carries `known_failure: true`, the invocation code is
`known_failure`, and the runner prints `KNOWN_FAILURE`. This status permits a
successful CI exit while explicitly retaining the graph defect. It does
not mean that the scenario passed. Partial selection stays partial.

If the scenario passes, the marker is stale and the invocation fails with
`unexpected_pass`. Setup, worker panic, timeout, cleanup, observation and
report failures are never waived. Replay derives acceptance again from the
frozen marker and raw evidence and refuses inconsistent stored status.
Blessing a marked case is refused. Removing the marker after a fix restores
the ordinary requirement that every healthy assertion pass.

### Validation and agent diagnostics

Parse the complete file, resolve profiles, and check every selected
environment's capabilities before creating case state. Reject unknown or
duplicate YAML keys, YAML aliases/merge keys/tags, misplaced sections,
unknown enum values, empty selections, duplicate environment parameters or seeds, and fields
that do not apply to the chosen target. Do not infer a target from a
storage URI or repair a misspelled field during execution.

Preflight must produce one immutable input for the invocation: the exact
validated case bytes, declared and selected executions, resolved non-secret
profile settings, and expected engine/runner build identities. Every
execution consumes that input; workers cannot reopen the original case or
resolve a profile name again as configuration authority. The runner records
the input digest. Before setup, each worker verifies that digest and its
actual executable identity against the input. Known identity differences
fail with `environment_changed`; unknown identities remain explicitly
unverified and cannot support a verified reproduction or DST replay.
Server execution additionally revalidates its effective configuration and
build identity at readiness, before creating case state. Credentials remain
external and excluded from the recorded input. This binds tested inputs;
it does not freeze the external service's internal execution schedule.

Capability requirements are derived from actual steps and configuration.
For example, `--- fault` requires its exact hook, and `--- expect shape`
requires result type evidence. There is no second hand-maintained
`requires` list that can disagree with the case. A server cannot substitute
inferred types for missing executed types or omit an existing comparison.

Every result identifies its scope using the following applicability rules.
The result carries the invocation's case path and content hash when the
bytes were read, input digest when resolved, and known engine/runner build
identities. A required but unavailable value carries an explicit reason;
an inapplicable value is marked separately. No synthetic environment,
profile, step, or seed may stand in for missing evidence.

| Result scope | Applicable context |
|---|---|
| Case/preflight | Source span when locatable; environment parameters only when parsed unambiguously; profile digest only after resolution. No operation ordinal is required for malformed YAML. |
| Execution/step | Target, storage, full declared parameters, effective settings, profile digest when required, and seed/replay index for DST. Step ordinal, loop iteration, source span, and expected/actual values apply to operation assertions; setup/teardown failures identify their phase instead. |
| Replay comparison | Environment parameters, seed, both attempt identities, their verdicts, and differing observations when available. A comparison does not invent a single failing step when attempts reached different steps. |
| Supervisor | The affected execution or invocation, containment outcome, and original failure reference. Operation context is included only when known. |
| Invocation summary | Final outcome and coverage of every declared execution: selected or unselected, then passed, accepted known failure, failed, or not run with a reason. If parsing cannot establish that inventory, coverage is explicitly unavailable and the invocation fails. |

Within one invocation, full environment parameters and seed/replay index identify an
execution. Step ordinal and loop iteration identify an observation inside
it. Reports from different invocations must retain their invocation boundary;
case path or digest alone cannot identify a run. A replay compares operation
identities and values, not run-specific report references.

Build identity includes executable digests and source revision plus
dirty-source identity when available. Unknown identity is reported explicitly
and cannot claim a verified reproduction. Secret values are excluded.
Observation and diagnostic collection must be bounded; exceeding the bound
fails explicitly and cannot truncate compared evidence into success.
Structured failures retain applicable expected/actual values and a stable
error code alongside the readable explanation. Contract codes include
`invalid_case`, `unsupported_environment`, `environment_changed`,
`fault_unobserved`, `fault_cleanup_failed`, `assertion_failed`,
`replay_mismatch`, `worker_failed`, `report_failed`, `timeout`, and
`unexpected_pass`. `known_failure` is a separate accepted status defined
in Known recovery failures; its raw worker result remains an assertion failure.
Every invocation must emit a terminal summary. A missing, malformed,
incomplete, or unwritable report fails the invocation and cannot yield
a successful exit. If the structured output cannot be written, the runner
also reports `report_failed` through stderr and exits unsuccessfully.
Cleanup failure preserves the original operation failure as well as its
own outcome; it never replaces the evidence that triggered cleanup.

The runner supplies an exact rerun command plus the required build and
profile references. The `--target`, `--storage` and `--seed` filters select all
matching declared executions. Supplied filters combine with AND and only
narrow the file's declared executions; they cannot change them. A seed filter
requires a target or storage filter. The report lists
selected and unselected executions, and a partial run never claims the
whole case passed. Zero matching environments or seeds is a refusal.
Each individual execution stops its step loop at the first failed step.
A completed failing DST execution still gets its fresh replay, and later
selected seeds continue while the deadline and isolation permit. A replay
mismatch fails the case but does not suppress later seeds. A worker failure
without a complete report stops that environment's remaining executions;
other selected environments continue only after containment is confirmed.
Deadline exhaustion or uncontained cleanup stops all further dispatch.
Every suppressed execution, including a mandatory replay, is reported as
not run with its terminal cause. Overall success requires every selected
execution and mandatory replay to pass or satisfy Known recovery failures,
and a complete terminal report. Accepted known failures remain distinct from
genuine passes.

The report is derived execution evidence, never configuration authority.
A reproduction compares case, build, effective settings, and non-secret
profile identities before running; missing or changed evidence is reported
as a changed environment. Repeating a command against an altered server
must not be described as replaying the original conditions.

The precise command-line flags and structured-result wire schema are
implementation deliverables in Rollout. Their acceptance tests must prove
exact selection, stable refusal codes, and complete failure context.
Bless remains available for an explicit single-engine, fault-free route;
it is refused for fault, DST, known-failure, and multi-environment executions. A runner must never
resolve disagreeing environments by rewriting the shared expectation.


### Execution semantics

The following setup and direct calls describe an explicitly declared
`omnigraph-engine` / `local-filesystem` environment. Every environment uses
the lifecycle and operation contract above.
Per execution, in order: create a `tempfile::tempdir()`, then
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
  `db.query(ReadTarget::branch(branch), query_source, name, &params)`,
  `branch` the step's `branch:` argument or `main`.
- A mutate step runs `db.mutate(branch, query_source, name, &params)`; its
  `MutationResult` carries `affected_nodes` and `affected_edges`, compared
  against an `affected:` expect, ignored under `ok`.
- A control write runs `branch_create_from_as` (from `main` when `from` is
  unspelled), `branch_delete_as`, or `branch_merge_as` (into `main` when
  `into` is unspelled), each with no actor, so a statement exercises the
  compiler and the engine and never the server's policy dispatch. After a
  `branch delete` the runner awaits `wait_for_fork_reclaims` before the
  next step: the delete returns at the manifest flip and reclaims the
  branch's forks in a background task, and no next step, reopen,
  `branch list`, or teardown may overlap a fork delete still in flight
  (dropping the handle detaches those tasks, it does not abort them). A
  conflicting merge is an error
  whose message begins `merge conflicts: `, so `ok` fails on it and
  `error: merge conflicts` pins it.
- `branch list` runs `branch_list` and presents the names as one non-null
  `Utf8` column `name`, rows in byte order; the shape check holds against
  that column as against any rows step, and the computed check against
  the compiler's inferred schema is skipped, since a statement has no
  declaration to infer from.
- A restart step drops the handle and reopens with `Omnigraph::open(uri)`;
  later steps use the reopened handle. What survives the reopen is exactly
  what the store committed, which is what the step exists to pin.

Traversal mode: by default a case runs on the production traversal
path, with no override, so the corpus exercises the path that ships. A
`# traversal:` header is an opt-in pin: every declaration step of that case
executes through the scoped seam
`instrumentation::with_traversal_mode("indexed" | "csr", fut)`, and the
index step runs for it regardless of the constructs the steps use, so the
pinned path runs covered rather than on a fallback. A statement step
traverses nothing, so it runs outside the seam and carries no pin. The
trade-off in one
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
parameter stays in scope. Fault cases expressible by Faults at an explicit
step may use GQT; other failpoint cases stay Rust tests. Interleaved writers
and transaction races stay in the existing DST suites. Fault-enabled GQT
executions require the isolation described above; they cannot share a
process-global hook with unrelated cases.

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
  decimal place cannot fail a case (DataFusion's rule). A non-finite value renders as
  `null` (RFC 0051) and compares as `null`.
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
- The shape section of a rows step is checked first, against the executed
  result (`QueryResult::schema()` and the batches), in order: the column
  count equals the number of shape lines; per position the executed field
  name equals the line's name; per position the executed `DataType` equals
  the line's type through `PropType::to_arrow` (Arrow equality, child field
  included); per position, when the line carries no `?`, no executed cell
  in that column is null, summed over the batches. The executor's own
  nullable flag is not compared: `project_return` derives it from the data
  and the aggregate path declares every column nullable, so only the data
  can be held to the author's statement, and a `.pg`-nullable property may
  be written without `?` when the step's data holds no null. The first
  failing check fails the step with the column position, the column name,
  and both sides in `.pg` spelling where the executed type has one
  (`expected Date, the executor returned F64`), Arrow spelling otherwise.
  When the compiler infers the shape line's type too, the message says so
  (`the compiler infers I32 too; the executor is wrong, not the shape
  line`), and bless does not rewrite a shape whose executed schema the
  compiler disputes: it reports the disagreement instead, since a blessed
  line would pin an executor defect.
- When the shape section passes, the executed schema is checked against
  the schema the compiler infers for the step's declaration against the
  case's catalog (`infer_query_result_schema`): count, executed-spelling
  names (`executed_column_name`), `DataType`, and no null cell in a column
  the compiler infers non-nullable. This costs the author nothing and is
  the only check that ties `lint --json`'s promise to the executed result.
  Only then are the rows compared; a schema failure of either kind never
  reaches the rows comparison and bless never rewrites rows over it. The
  two checks exist because a row comparison cannot see a type: the JSON
  writer omits a null cell's key (RFC 0051), so a zero-row aggregate typed
  `Float64` instead of the declared `Date32` renders as `{"n": 0}` either
  way ([#623](https://github.com/ModernRelay/omnigraph/issues/623)); the
  shape section is the author's statement of the columns, the way the rows
  body is the author's statement of the values, and the computed check
  proves the compiler and the executor agree.

Ranking scores are projectable (`nearest` and `bm25` since v0.11.0, `T33`
ties the projection to the executed retrieval) and their values are
assertable: the 12-decimal normalization renders a `Float32` score stably. Row order is still asserted with `expect ordered`;
project the score only when the value itself is the claim.

### Runner mechanics

The test target is `harness = false` and hands discovery to
`datatest-stable`: every `cases/*.gqt` file, rooted at the crate, is
registered at run time as its own libtest-compatible test (a libtest-mimic
trial under `datatest-stable`) named `case::<file>.gqt`. The runner it
calls (parser, execution, comparison, bless) is the crate's library,
`crates/omnigraph-gqt/src/lib.rs`, and the format self-tests are unit
tests beside it in `crates/omnigraph-gqt/src/tests.rs`; the crate is
`publish = false` and never built for release. Direct-engine cases run on one shared
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
in well under a second. Every file owns its explicit `timeout_ms` budget;
there is no default and `OMNIGRAPH_GQ_CASE_TIMEOUT_SECS` is refused as an
ambient override. The timeout failure message prints the budget in force. The
elapsed time on each ok/FAIL line is the drift signal a reviewer reads.

The GQT corpus admission check must reject a declared file budget above
10000 milliseconds before case setup, independent of selected environments
or seeds. The required job must run this check over every corpus file
before execution. It must not clamp an explicit budget to make a case
admissible. A case that exceeds
its admitted budget fails the required job. Larger budgets remain valid
only for explicit file execution outside the per-PR corpus; the GQT
command-line runner owns that local reproduction route. It is not
automatically enrolled in CI. The existing heavy-repro tier, defined below
in the Enforcement ladder, continues
to discover Rust repro targets; it does not discover these external GQT
files. A slow GQT reproduction must be reduced for corpus enrollment or
converted to that tier's Rust route. Each case's
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

No architectural invariant is weakened; the change is test-and-CI only. The
existing engine surfaces are public and chokepoint-registered in the
`forbidden_apis.rs` const registries: `query`, `query_with_head`, and
`run_query_at` read-only, `load_jsonl` / `load_jsonl_file` under `LOAD_V9`,
the `mutate` family under `MUTATION_V9`, and `open` / `open_with_storage`
under the `RecoveryExecutor` write protocol. That `forbidden_apis.rs`
walk covers `crates/omnigraph/src/**` only, so the `omnigraph-gqt` crate
adds no registry entries; no deny-list item is affected, and no new
public API is added.
The proposed fault extension must preserve per-case isolation. It cannot
weaken graph publication, recovery, policy, or storage admission rules.
Future HTTP execution needs a separately qualified test-control boundary;
this amendment does not authorize production fault-control APIs.

## Compatibility and reversibility

No production storage format or wire surface changes; the RFC extends tests,
CI, and contributor docs. Reverting means deleting the `omnigraph-gqt`
crate and its `members` entry in the workspace `Cargo.toml` (which drops
`datatest-stable` and its lockfile closure), the two workflow files with
their scripts, and the
AGENTS.md sentences; the logic test files remain readable, self-contained behavior records
either way. Format evolution is fail-closed: unknown sections, unknown
header keys, and missing required headers are refusals, never silent
skips, so an older harness refuses a newer logic test rather than
mis-running it.

The explicit runner section is mandatory for all cases. It has no version
field; parser and corpus changes land together.
Omitting the section or any required field is `invalid_case`, including for
previously accepted engine/local-filesystem cases. Migration adds that
environment explicitly to every existing case; no provider, execution target,
seed or timeout is discovered from the host or supplied as a format default.

The prototype's `mode: normal` / `mode: dst` runner shape is not an alias
for this contract. The earlier draft's `runtime` field and `omnigraph-dst`
value are not aliases either; use `target: omnigraph-engine-dst` for direct
engine simulation. Migration rewrites every configuration explicitly. In
particular, the prototype's
fault occurrence count starts during initialization; the proposed count
starts at the selected operation. Migration must identify that operation
and re-establish the failure, never copy occurrence numbers mechanically.
Old parsers reject the new sections. Rollback requires retaining the newer
harness or migrating those files back, not silently dropping directives.

Named compatible extensions, deferred until a real case demands each,
with maintainers deciding each one when the first case that needs it
forces the question:

- Additional feature requirements not already derivable from the
  environment and step contract.
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

Whole-case execution across declared environments is distinct from the
per-query second-plan verification deferred above. Multi-connection
interleaving remains outside GQT and retains its existing DST owner.

## Alternatives

| Environment/fault alternative | Decision and concrete cost |
|---|---|
| Keep only the current local runner and Rust fault tests | Smallest change, but each merge-failure case duplicates setup and assertions in Rust. |
| Select environments entirely through CLI or CI | Short files, but copying a regression loses its required storage and fault conditions. Exact selection may narrow a file, not redefine it. |
| Inline every endpoint and credential | Self-contained connection data, but secrets and machine-specific addresses prevent portable cases. Profiles own connection data; resolved evidence exposes drift. |
| Reopen case/profile paths in every worker | Avoids handing workers resolved input, but an edit after preflight can make two workers agree on a different experiment. Preserve validated input and check worker identities. |
| Arm faults once at file startup | Simpler lifecycle, but setup can consume the occurrence intended for a later merge. Step scope gives the occurrence an explicit owner. |
| Add a separate capability list or expected file per target | Duplicates facts already present in operations and assertions, creating disagreement paths. Derive requirements and share expectations. |

The nearest in-repo owners are the GQT parser/comparator, DST environment,
`StorageAdapter`, and server test support. Extend those boundaries rather
than add a second assertion engine or cluster lifecycle implementation.
The [sqllogictest Runner](https://docs.rs/sqllogictest/latest/i686-pc-windows-msvc/sqllogictest/runner/struct.Runner.html)
separates database execution from validation. [RisingWave's 2023 account](https://risingwave.com/blog/applying-deterministic-simulation-the-risingwave-story-part-2-of-2/)
describes reusing scripts between real and simulated clusters. These
support the separation; neither establishes this proposed GQT syntax.


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

The amendment requires the following evidence before a target/storage
combination is advertised as supported:

| Owner | Required evidence |
|---|---|
| GQT parser self-tests | Missing runner sections on existing and new cases, required fields, distinct environment parameters, all field bounds, invalid positions, unsupported combinations, old `runtime`/`mode` spellings, and migration refusals. |
| GQT execution/dispatch tests | Every declared environment runs fresh; exact subset selection is visible; deadlines include preflight/setup/replays/cleanup; no-match selections fail. |
| GQT input identity tests | Editing the original case/profile after preflight cannot change worker input; replacing a worker build is refused before setup; unequal identities cannot count as replay. |
| GQT scheduling tests | A completed assertion-failing first seed still replays and later seeds run; mismatch, worker failure, deadline, and uncontained cleanup follow the specified continuation rules and account for every not-run attempt. |
| GQT diagnostic tests | Malformed YAML, unresolved profiles, setup failures, replay mismatch, and cleanup failure have applicable context; missing/incomplete/unwritable reports fail, preserving the original failure. |
| GQT CI/admission tests | A workflow-equivalent mixed engine/engine-DST corpus with explicit configuration executes every declared attempt; an over-10000-ms corpus file fails admission even under subset selection; explicit out-of-corpus execution preserves its declared budget. |
| GQT comparison tests | Rows, executed types, counts, and error checks remain active through each executor; transport conversion preserves the existing contract. |
| Fault isolation tests | Setup cannot consume a fault; another case cannot trigger it; occurrence counting includes production retries; spoofed or missing delivery fails. |
| Fault cleanup tests | Expected errors, unexpected success, panic, cancellation, and timeout cannot leave a hook armed for another operation. |
| DST replay tests | Successful and completed failing seeds repeat in fresh processes; changed actual counts, executed types, error payloads, rows, operation sequence, or main snapshot IDs fail comparison even with the same failure classification. |
| Known recovery failure tests | Exact typed recovery reason and step qualify only with all earlier assertions and fault deliveries; changed reasons, other failures, missing faults, stale markers, and forged replay status fail. Raw reports still compare in full. |
| Server test support | Backend verification, exclusive fresh graphs, authentication, actual graph reopen, and typed fault correlation before advertising each capability. |

The existing merge-refusal cases retain successful-write expectations
after the injected failure. An explicitly marked exact recovery defect can
qualify only under Known recovery failures; its accepted status remains
distinct from a genuine pass. Unmarked or mismatching failures remain
regression failures. Neither blessing nor target selection can hide them.

Lance's [object-store configuration](https://lance.org/guide/object_store/)
documents backend options outside the query text. GQT must record the
effective settings used by the tested build; current upstream defaults
are not evidence of the pinned dependency's defaults. The initial engine/DST
runner records its Rayon and Lance thread counts, deterministic-backoff setting,
entropy seed when applicable, and Tokio runtime kind with explicit direct-engine
worker count and stack size. These recorded values configure the worker and are
verified against both this build's settings and the process environment before
setup. Replay validates every recorded settings object before dispatching any
worker. Lance's memory pool is recorded as `dependency_default`, with
`LANCE_MEM_POOL_SIZE` absent; this identifies the pinned dependency's default
selection, not a measured or independently verified pool capacity.


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

### Environment extension rollout

1. Deliver the first complete engine/DST integration: mandatory configuration
   and corpus migration, immutable invocation inputs, exact selection, corpus
   admission, structured diagnostics, preserved lifetime, scoped faults with
   delivery evidence, shared assertions, and observation replay. Admit only
   fault-free `omnigraph-engine` / `local-filesystem` and
   `omnigraph-engine-dst` / `in-memory-object-store`. The GQT maintainer
   decides the CLI/result schema before enabling this phase, including
   explicit file execution outside the corpus and acceptance tests for
   result applicability and failed report publication. The maintainer owns
   the required workflow's seeded build, its `omnigraph-gqt` worker binary,
   and the workspace test
   invocation described in User and operational behavior. Before enrolling
   DST cases, a workflow-equivalent mixed corpus must prove full declared
   execution and admission enforcement. The DST package workflow alone is
   insufficient. Migrate prototype faults by their intended operation.
   Keep parser capability refusals for every unqualified combination. An
   explicitly marked recovery defect must meet Known recovery failures,
   with its raw failure evidence retained; other known-red cases fail.
2. Qualify direct-engine in-memory storage and, independently, fault controls
   for ordinary engine execution. These extend the first integration rather
   than delay its engine-DST path. Preserve the same isolation, attribution,
   result and reproduction evidence requirements.
3. Qualify configured S3 and Azure engine execution with provider-specific
   evidence and existing admission rules. CI must supply declared profiles;
   missing configured services fail their selected cases.
4. Add HTTP execution through the existing server lifecycle owner. Enable
   `omnigraph-server` cases first; enable reopen and faults only when their
   separate capability tests pass. Qualify `omnigraph-server-dst` independently,
   including controlled server tasks and the embedded engine. Neither server
   target is a prerequisite for the first engine/DST integration.

The RFC's `implementation` remains `partial` while this extension has
unqualified targets. No stage changes engine recovery behavior or claims
determinism for real external services.

## Unresolved questions

The proposed format and failure semantics above are the acceptance
decisions. Two implementation proposals remain, with explicit owners:

| Proposal | Decision owner | Required event |
|---|---|---|
| CLI and diagnostic wire schema | GQT maintainer | Accept the proposal and pass phase 1's selection, applicability, coverage, and report-failure tests before enabling the interface. |
| HTTP profile and test-control protocol | Server lifecycle maintainer, with GQT maintainer acceptance of the execution contract | Accept the proposal and qualify backend identity, exclusive graphs, result typing, and each requested control before enabling phase 4 capabilities. |

These roles own the decisions, not an assertion that a particular maintainer
has already approved them. Neither proposal may weaken the specified
isolation, selection, typing, or evidence requirements.

## Decision log

The entries below record earlier decisions. The current environment amendment
supersedes their implicit execution and ambient-budget rules and assigns the
complete corpus to the separate configured `GQ Logic Tests` context. Their
historical command and configuration descriptions are not migration aliases.

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
- 2026-09-05, amendment from the PR that added the result-schema check to
  the runner. Trigger: the pending fix for #623 removes a zero-row
  aggregate shortcut that typed every non-`count` column `Float64` against
  the
  declared type, and its `.gqt` case could not tell: the JSON writer omits
  a null cell's key (RFC 0051), so `{"n": 0}` renders from the wrong type
  and the right one alike. The check that a Rust test had to carry now
  runs on every rows step.
  - Comparison semantics: a `unordered`/`ordered` step compares the
    executed `QueryResult::schema()` against the compiler's
    `infer_query_result_schema` for the step's declaration before its rows
    are compared: column count; per position the executed column name
    (`executed_column_name`, the spelling the executor uses and T25 guards,
    not `projection_name`'s, which names an unaliased property by the
    property alone), the Arrow `DataType`, and no null cell in a column the
    compiler infers non-nullable. The executor's own nullable flag is
    outside the comparison (data-derived on the projection path, always
    `true` on the aggregate path). A mismatch fails the step naming the
    position, the column, and both types; bless does not run over it.
  - Invariant added: a `.gqt` step that passes has an executed result
    schema equal in count and Arrow types to the schema the compiler
    infers for it, with the executed column names
    (`executed_column_name`), and holds no null in a column inferred
    non-nullable. A type-level regression in the executor therefore turns
    a case red even when every affected cell is null.
  - Runner mechanics: the runner keeps each query step's parsed declaration
    and typechecks it against the open store's catalog after execution; the
    typecheck cannot fail for a query that just executed, and a failure
    there is reported as the step's failure.
  - File format, the shape section: every rows step carries a mandatory
    `--- expect shape` section, the author's statement of the result
    columns in `.pg` property syntax, checked against the executed result
    before the computed check and the rows (File format, Comparison
    semantics, Bless mode). The computed check alone proves the executor
    agrees with the compiler and cannot see a rule both share; its
    expectation is produced by the code under test, while the rows body is
    the author's, and the columns should be too (sqllogictest's per-query
    type string, in the engine's own type vocabulary rather than three
    letters). Names follow the executed spelling; the compiler's inferred
    schema and `lint --json` still spell an unaliased property by the
    property alone, and whether that spelling folds into the executed one
    is a separate compiler decision. Landing order: until that fix lands, a
    rows step whose zero-row result carries a non-`count` aggregate is red
    by design (the executor types the column `Float64`; the `.pg` shape line
    is right and the failure message says so), and no corpus case carries
    one. Superseded: File format "a mandatory
    `--- expect` section with mode word `unordered`, `ordered`, or
    `error: <substring>`" as the whole of a query step's expectations;
    File format "A result row always carries every projected column key,
    with null cells rendered as JSON `null` (never an absent key)" (false
    since RFC 0051 landed); Bless mode "rewrites row bodies only" and
    "null cells explicit"; Motivation's "type strings" among the omitted
    sqllogictest mistakes, for the three-letter form only; and this
    entry's earlier "No new section, no opt-out", which described the
    computed check alone.
- 2026-09-06, amendment from the PR that makes a bare node projection
  return the node object (#631). Trigger: the shape line's type was a `.pg`
  `type_ref`, which has no spelling for a struct column, so the three cases
  that pin the object could not carry the mandatory shape section.
  - File format, the shape section: a node type name (`p: Person`) spells a
    bare node projection; the runner checks that the executed column is a
    struct whose field names are that type's node object (the identity
    column and the declared properties minus `Blob` and `Vector`, in the
    catalog's order); `Person?` is refused (the object is never null);
    bless spells a struct column as the one node type whose object it is,
    and refuses a struct that is no type's object. Superseded: File format
    "A bare node projection (`return { $p }`) has no green shape today ...
    no corpus case carries one until the engine returns the node object".
- 2026-09-06, amendment from RFC 0055 (`0055-gq-branch-statements.md`,
  Design, Logic tests), which adds branch statements to GQ. Trigger: the
  merge-family findings (#583, #600, the seed-221206 re-adoption) are
  five-step stories (fork, write, write, merge, read) the format could not
  hold, since every step ran against `main` and no step could create,
  merge, delete, or list a branch.
  - File format, the step list: `--- query` and `--- mutate` take one
    optional argument, `branch: <name>`; a `--- mutate` step may hold a
    control write and a `--- query` step `branch list`, classified by the
    compiler and refused under the wrong section; a statement step refuses
    `branch:` and `--- params`; `branch merge` takes the new expect mode
    `outcome: <word>`, refused on any other step; `affected:` is refused
    on a control write; `branch list` is a rows step over `{"name": "…"}`
    whose shape section is `name: String`. Superseded: "`--- query`
    holding exactly one GQ declaration with a read body" and "`--- mutate`
    holding exactly one GQ declaration with a mutation body" as the only
    bodies a step holds.
  - Execution semantics: a declaration step runs against its `branch:`
    argument; a control write runs `branch_create_from_as`,
    `branch_delete_as` (joined by `wait_for_fork_reclaims`), or
    `branch_merge_as` with no actor; `branch list` runs `branch_list` and
    is presented as one non-null `Utf8` column `name` in byte order, shape
    checked, computed check skipped. Superseded: "A query step runs
    `db.query(ReadTarget::branch("main"), …)`" and "A mutate step runs
    `db.mutate("main", …)`" as the only targets.
  - Compatibility: the fail-closed rule names sections and header keys;
    for the step argument and the expect mode it rests on the runner's own
    refusals (`takes no arguments`, `unknown expect mode`, and the refusal
    of a statement body), so an older harness refuses a case using either
    amendment and never mis-runs it.
  - Invariant added: a `branch list` rows step that passes has one
    non-null `Utf8` column `name` with its rows in byte order.
  - 2026-09-06, from the implementation of that PR:
    sentence rewrites in the sections this entry amends. Superseded: File
    format "`# traversal:` takes `indexed` or `csr` and pins every step to
    that mode" and Execution semantics "every step of that case executes
    through the scoped seam", both of which read onto statement steps, which
    traverse nothing and run outside the pin; Execution semantics "which a
    following `--- restart` would otherwise drop with the handle" (the
    reclaims are `JoinHandle`s with no `Drop` and no `abort`, so dropping the
    handle detaches them; the join is there because no next step, reopen,
    `branch list`, or teardown may overlap a fork delete still in flight);
    File format "an iterating value never reaches it" and "`${` in a
    statement body is refused as in any other step body" (a quoted name
    carries `${` past the compiler and the runner's own fence refuses it);
    File format's `branch: <name>` sentence, silent on the name being taken
    verbatim and unquoted; File format's `branch merge` expect sentence,
    silent on `ok` being satisfied by a no-op merge; and the refusal string
    `a branch statement names its branches itself`, which now names the next
    action. Frontmatter `updated:` bumped to 2026-09-06.
