# Autoresearch: branch and write operations, 2026-11-02

Operation group: `branch create`, `branch merge` (three-way and fast
forward), `branch delete`, `load` (append and merge), and a one-row `mutate`,
on `main` and on a named branch, warm and cold, measured as history grows.

- Branch: `autoresearch/branch-ops-20261102`
- Base commit: `3e0e467e39756355c981c2f598f63d92a394529f` (main, 2026-11-02)
- Box: Modal dev box `autoresearch-branch-ops`, Claude Opus 5.5 (`claude-opus-5-5`)
- No prior runs of this workflow existed.

## Result

Merges no longer grow with history. With one-row commits accumulating as
uncompacted fragments, a fast-forward merge went from 68 requests (no history)
to 4,168 (1,024 commits); it now stays at 50–54 at every depth. A three-way
merge went from 132 to 3,210; it now stays at 123–135. Writes, merges and
deletes on a named branch send 6–10 fewer requests at every depth. Four
hypotheses were measured: three kept (H1, H3, H4), one dropped (H5); one more
(H2) was analysed and deferred. H3 and H4 together are the redesign prototype
(merge reads follow the changed fragments, not the table).

What still grows: any write that filters a table (a one-row `update` by key)
reads every uncompacted fragment once per warm handle and about twice per
cold one, and a cold strict insert probes every fragment once. That follows
from per-commit fragments, not from history depth; the fix is maintenance
(compaction, index coverage), proposed below and not prototyped.

## Method

**Instrument.** GQT `--measure` (`crates/omnigraph-gqt`, DST in-memory object
store), which records every object-store request of every step with its path,
verb and the engine seam phase it ran in. I classified requests by path into
four realms: `manifest` (`__manifest`, its branch trees and refs), `history`
(`__history`), `table` (node and edge datasets) and `control` (the
`StorageAdapter`). The GQT report's own classes file `__history` under
`table_*`; the tables below do not. Counts are exact and deterministic (each
DST seed runs twice and the runner compares the two), so one run per variant
is the score. HEADs and failed requests count, as S3 bills them.

**Cases.** `scripts/autoresearch/branch_ops/gen_cases.py` generates the cases;
`run.sh` runs them; `analyze.py` and `report_tables.py` tabulate them. Each
case builds a history, then runs a fixed tail of 19 measured operations: on a
warm handle (the same `Omnigraph` that built the history, as a long-running
server holds), then each cold one after `--- restart`. In the `i` cases every
history commit is its own measured step, so the insert curve comes for free
(a GQT `--- loop` is measured as one step, so the `d` cases' history is not
broken down).

Tail operations: `mutate` is a one-row `update Person set { age: $n } where
name = "<key>"`; `load` is a generated 10-row `Person` load; `branch create`,
`branch merge`, `branch delete` are GQ branch statements. The three-way merge
merges `feat` (a branch update plus two 10-row loads) into `main` after `main`
took an update and two loads; the fast forward merges `ff` (one update) into
an unchanged `main`. "Cold" operations run first after a restart.

- `d0000`: no history. `i0032` … `i2048`: 32 to 2,048 one-row inserts of new
  `Person` rows on `main` (a sync writer's shape: one fragment per commit).
- `d2048`: 2,048 one-row updates of the same row (history depth without
  fragment growth: Lance drops the fully deleted fragment).
- `d0032_dead0256`, `d0032_live0256`: 256 created-then-deleted branches, or
  256 live branches. `i0256_tables32`: 32 tables, 256 commits on one.

**Release settings.** A debug build defaults `merge_lineage` and
`change_lineage` to `verify`, which runs both merge classifiers. Every case
sets both to `on`, the release default, before its first step. Without that
the merge counts are about double (my first baseline run had this error; it
is discarded).

**Not the bench harness.** `omnigraph-bench` records logical store calls and
wall clock per sample only from a qualified release build in SHA-attested
worker processes; that environment does not exist on this box. GQT
`--measure` drives the same engine core and records every request with its
path and phase, which the tracing needed.

## Baseline and final counts

Tail totals (all 19 operations): 723 → 607 with no history, 1,313 → 845 at 32 commits, 5,138 → 2,206 at 256, 18,194 → 6,814 at 1,024, 35,576 → 12,932 at 2,048.

**Baseline growth curve** (total requests per operation; one-row commits on `Person` before the tail)

| operation | 0 commits | 32 commits | 256 commits | 1024 commits | 2048 commits |
|---|---:|---:|---:|---:|---:|
| one-row insert (history commit at that depth) | - | 19 | 19 | 19 | 19 |
| mutate main warm | 17 | 54 | 278 | 1046 | 2070 |
| branch create warm | 11 | 11 | 11 | 11 | 11 |
| mutate branch warm | 52 | 54 | 58 | 58 | 54 |
| load append main warm | 12 | 12 | 12 | 12 | 12 |
| load merge branch warm | 31 | 33 | 33 | 33 | 33 |
| load merge main warm | 17 | 19 | 19 | 19 | 19 |
| load append branch warm | 31 | 33 | 33 | 33 | 33 |
| branch merge threeway warm | 132 | 236 | 912 | 3216 | 6284 |
| branch delete warm | 25 | 25 | 25 | 25 | 25 |
| branch create ff | 11 | 11 | 11 | 11 | 11 |
| mutate branch ff | 52 | 56 | 58 | 58 | 54 |
| branch merge fastforward warm | 68 | 200 | 1096 | 4168 | 8264 |
| branch delete ff | 25 | 25 | 25 | 25 | 25 |
| mutate main cold | 28 | 95 | 544 | 2080 | 4126 |
| branch create cold | 11 | 11 | 11 | 11 | 11 |
| mutate branch cold | 54 | 122 | 572 | 2108 | 4152 |
| branch merge fastforward cold | 85 | 219 | 1117 | 4189 | 8281 |
| branch delete cold | 37 | 38 | 39 | 39 | 37 |
| load append main cold | 24 | 59 | 284 | 1052 | 2074 |

**Final growth curve** (total requests per operation; one-row commits on `Person` before the tail)

| operation | 0 commits | 32 commits | 256 commits | 1024 commits | 2048 commits |
|---|---:|---:|---:|---:|---:|
| one-row insert (history commit at that depth) | - | 19 | 19 | 19 | 19 |
| mutate main warm | 17 | 54 | 278 | 1046 | 2070 |
| branch create warm | 11 | 11 | 11 | 11 | 11 |
| mutate branch warm | 42 | 44 | 48 | 48 | 44 |
| load append main warm | 12 | 12 | 12 | 12 | 12 |
| load merge branch warm | 24 | 26 | 26 | 26 | 26 |
| load merge main warm | 17 | 19 | 19 | 19 | 19 |
| load append branch warm | 24 | 26 | 26 | 26 | 26 |
| branch merge threeway warm | 123 | 131 | 135 | 135 | 131 |
| branch delete warm | 18 | 18 | 18 | 18 | 18 |
| branch create ff | 11 | 11 | 11 | 11 | 11 |
| mutate branch ff | 42 | 46 | 48 | 48 | 44 |
| branch merge fastforward warm | 50 | 54 | 54 | 54 | 54 |
| branch delete ff | 18 | 18 | 18 | 18 | 18 |
| mutate main cold | 28 | 95 | 544 | 2080 | 4126 |
| branch create cold | 11 | 11 | 11 | 11 | 11 |
| mutate branch cold | 44 | 112 | 562 | 2098 | 4142 |
| branch merge fastforward cold | 64 | 70 | 72 | 72 | 68 |
| branch delete cold | 27 | 28 | 29 | 29 | 27 |
| load append main cold | 24 | 59 | 284 | 1052 | 2074 |


## Where the calls come from

Traced from the per-request logs (`python3 -I` over the report's
`measurements[].value.log`) and the code they name.

**One-row mutate on `main`, warm (17 requests at depth 0).** Capture: a
freshness probe of `__manifest` (HEAD of the held version and of the next,
`lance.md` "Catalog freshness probe"). Staging: open the table at its pin
(HEAD + GET of the detached manifest, twice), read the data the predicate
needs, write the new fragment. Gate: the same freshness probe again. Commit:
transaction file and detached manifest PUT. Publish: one `_versions` listing
(`current_control_dataset`), then data, transaction and manifest PUTs. The
`manifest` realm is 8 requests and does not grow with commits, branches or
tables. The `table` realm grows with fragments when the mutation filters: the
update `where name = "bob"` reads the `name` column of every fragment, about
one GET per fragment on a warm handle (Lance's session caches what it has
read) and two on a cold one. The warm branch update in the tail stays flat
only because the main update just before it read every `name` page on the
same handle; its cold twin grows like the main one. A one-row insert stays
flat (11 table requests):
the strict-insert absence probe reads `id` pages, and a warm handle has read
every older fragment's `id` page before.

**One-row mutate on a named branch (52 at depth 0).** Three costs `main`
does not pay. The engine's one-entry authority cache misses after a branch
create, so the first write opens the branch coordinator in full (`__manifest`
of main, refs listing, the branch ref, the tree's versions and manifest, the
rows). The publisher for a named branch never reuses its acknowledged
dataset (`GraphNamespacePublisher::dataset` reuses only main's;
`retain_control_dataset` refuses branch images), so every publish reopens
main's `__manifest`, resolves the branch through the refs listing and checks
it out: 7 requests where main pays 1. And every read of the branch ref was
Lance's `exists` HEAD, `from_path` size HEAD and ranged GET: 3 requests, 4 to
6 times per write (H1).

**Branch create (11, flat).** Freshness probe, two listings of
`_refs/branches`, a GET of the absent target ref, the target tree listing, the
source manifest, then Lance's shallow clone (transaction and manifest PUT)
and the ref PUT. Flat in commits, live and dead branches and tables.

**Branch delete (25, flat).** Probe; the branch ref read five times (cached
coordinator check, resolution, Lance's `replace_metadata`, archive check);
main's `__manifest` open; refs and tags listings; a conditional PUT of the
`__history` block the branch's buffer would release (refused as present,
then read back); the retirement metadata PUT on the ref, the archive PUT and
the ref DELETE.

**Fast-forward merge (68 at depth 0, 4,168 at 1,024 commits).** The manifest
side is flat (33). The table side was the adopt route: for a table the target
left at the base, unless the source's change is a proven pure-insert history,
`compute_adopt_delta` walked the base and the source in full `id` order
(`OrderedTableCursor::from_snapshot_lazy`), sorting keys and hydrating every
row, to build a validation delta. That is about four requests per fragment of
every touched table, independent of how much the branch changed. It did not
consult `merge_lineage` at all.

**Three-way merge (132 at depth 0, 3,210 at 1,024).** With `merge_lineage =
on`, `plan_lineage_merge` finds candidate ids from fragment and deletion-file
metadata (cheap), then `stage_lineage_table_merge` read each candidate chunk
from the base, the source and the target with an `id IN (...)` filter over
every fragment: about three requests per fragment.

**Load (12 append, 17 merge on main; 31 on a branch).** Flat on a warm handle
for the same reasons as an insert. A cold append (24 at depth 0, 2,074 at
2,048) probes every fragment once.

**What did not grow.** History depth alone (`d2048`: 2,048 commits, every
tail operation equal to `d0000` within 2 requests), dead branches, live
branches, and the number of untouched tables. `__manifest` stayed at 1–3
fragments through 2,048 commits on a graph born at format 16: a cold open
reads 13 manifest-realm objects at depth 0 and 13 at depth 2,048.

## Hypotheses

Counts are total requests per operation, baseline → after, on the warm tail
unless marked cold. Every row's other operations stayed equal or fell.

| # | What, where | Counts before → after | Gates | Outcome |
|---|---|---|---|---|
| H1 | Read an exact branch ref with one GET instead of Lance's exists HEAD, size HEAD and ranged GET (`omnigraph-core/src/branch_control.rs`, `read_ref_contents`; legacy refs without an identifier keep Lance's reader). Commit `eb124279`. | depth 0: branch mutate 52 → 42, branch load 31 → 24, branch delete 25 → 18 (cold 37 → 27), fast forward 68 → 62, three-way 132 → 126; flat in depth. DST golden: every changed row is an `l.get` drop (BranchDelete 26 → 16, BranchWrite 23 → 16, MergeTargetHistory 100 → 91, diff pages 55 → 44). | core lib 87/87 (three stale-HEAD tests now assert no HEAD is sent; the persistent-unreadable check now corrupts the GET body); catalog lib 214/214; DST golden regenerated; GQT corpus 432/432 | **Kept** |
| H2 | Let the named-branch publisher reuse its acknowledged `__manifest` image (as main does) behind a refs resolution and a tree listing, instead of reopening main and checking the branch out. | Not built. Expected about −4 per branch publish attempt. | — | **Deferred**: needs `retain_control_dataset` to retain branch images. It refuses any image with `manifest.branch` or `base_paths`, and every shallow-cloned branch tree carries its parent's `base_paths` through each Overwrite (Lance `lance-table` `transaction/manifest_build.rs`, `reference_paths` from the current manifest). The refusal dates from #863 with no stated reason; relaxing it needs a proof that a rebound image resolves inherited bases correctly. |
| H3 | Compute an adopt (fast-forward) delta from lineage candidates: reuse `plan_lineage_merge` with the target at the base, read candidates by `id IN` scoped to each end's changed fragments (`LineageSideScope`); `verify` runs the walk too and fails on any difference in deleted, inserted or updated ids (`exec/merge.rs`). Commits `fc2d9d0b`, `fdf8c373` (boxing its futures; the first version overflowed a debug test stack) and `41d37eb2` (a clippy fix). | fast forward 68 → 50 (depth 0), 200 → 54 (32), 1,096 → 54 (256), 4,168 → 54 (1,024); cold fast forward 85 → 64 … 4,189 → 72. DST golden unchanged (no labeled scenario adopts a non-insert change). | GQT corpus 432/432 in debug `verify` (every merge case compares lineage adopt with the walk); engine `merge_fast_forward`, `merge_truth_table`, `merge_cost`, `branching`, `merge_cleanup`, `changes`: 144 passed | **Kept** |
| H4 | Scope the three-way lineage candidate reads to the fragments that can hold a candidate: base over both sides' base scopes, each side over its own changed fragments plus the base fragments the other side changed (`LineageMergePlan::three_way_fragments`). Commit `ab5373a0`. | three-way 126 → 123 (depth 0), 230 → 131 (32), 906 → 135 (256), 3,210 → 135 (1,024). DST golden: MergeBackAfterThreeWay 106 → 103. | as H3, on the H4 binary; corpus result in Gates | **Kept** |
| H5 | Give each merge an operation-local handle cache for its three pinned inputs, so repeated opens of one pin reuse a handle. | 0 change on every case. | — | **Dropped**: the repeated table-manifest reads come from the pure-insert proof's chain walk (`checkout_version` per link in `proven_chain_fragments`), not from repeated opens. |

H1's counts include nothing else; H3's include H1; H4's include H1 and H3.

**Baseline realm split at 256 commits** (`i0256`: requests by realm, with verbs g=get h=head l=list p=put d=delete)

| operation | total | manifest | history | table | control |
|---|---:|---:|---:|---:|---:|
| mutate main warm | 278 | 8 (h4 l1 p3) | 0 | 270 (g264 h2 p4) | 0 |
| branch create warm | 11 | 11 (g1 h4 l3 p3) | 0 | 0 | 0 |
| mutate branch warm | 58 | 43 (g22 h12 l6 p3) | 0 | 15 (g10 h2 p3) | 0 |
| load append main warm | 12 | 8 (h4 l1 p3) | 0 | 4 (g1 p3) | 0 |
| load merge branch warm | 33 | 23 (g6 h11 l3 p3) | 0 | 10 (g5 h2 p3) | 0 |
| load merge main warm | 19 | 8 (h4 l1 p3) | 0 | 11 (g6 h2 p3) | 0 |
| load append branch warm | 33 | 23 (g6 h11 l3 p3) | 0 | 10 (g5 h2 p3) | 0 |
| branch merge threeway warm | 912 | 53 (d3 g17 h25 l2 p6) | 1 (p1) | 858 (g833 h13 l4 p8) | 0 |
| branch delete warm | 25 | 23 (d1 g6 h11 l3 p2) | 2 (g1 p1) | 0 | 0 |
| branch create ff | 11 | 11 (g1 h4 l3 p3) | 0 | 0 | 0 |
| mutate branch ff | 58 | 43 (g22 h12 l6 p3) | 0 | 15 (g10 h2 p3) | 0 |
| branch merge fastforward warm | 1096 | 33 (d2 g3 h22 l1 p5) | 1 (p1) | 1062 (g1053 h6 l2 p1) | 0 |
| branch delete ff | 25 | 23 (d1 g6 h11 l3 p2) | 2 (g1 p1) | 0 | 0 |
| mutate main cold | 544 | 15 (g7 h4 l1 p3) | 0 | 529 (g524 h2 p3) | 0 |
| branch create cold | 11 | 11 (g1 h4 l3 p3) | 0 | 0 | 0 |
| mutate branch cold | 572 | 43 (g22 h12 l6 p3) | 0 | 529 (g524 h2 p3) | 0 |
| branch merge fastforward cold | 1117 | 54 (d2 g20 h23 l4 p5) | 1 (p1) | 1062 (g1053 h6 l2 p1) | 0 |
| branch delete cold | 39 | 37 (d1 g16 h12 l6 p2) | 2 (g1 p1) | 0 | 0 |
| load append main cold | 284 | 15 (g7 h4 l1 p3) | 0 | 269 (g264 h2 p3) | 0 |

**Final realm split at 256 commits** (`i0256`: requests by realm, with verbs g=get h=head l=list p=put d=delete)

| operation | total | manifest | history | table | control |
|---|---:|---:|---:|---:|---:|
| mutate main warm | 278 | 8 (h4 l1 p3) | 0 | 270 (g264 h2 p4) | 0 |
| branch create warm | 11 | 11 (g2 h3 l3 p3) | 0 | 0 | 0 |
| mutate branch warm | 48 | 33 (g22 h2 l6 p3) | 0 | 15 (g10 h2 p3) | 0 |
| load append main warm | 12 | 8 (h4 l1 p3) | 0 | 4 (g1 p3) | 0 |
| load merge branch warm | 26 | 16 (g6 h4 l3 p3) | 0 | 10 (g5 h2 p3) | 0 |
| load merge main warm | 19 | 8 (h4 l1 p3) | 0 | 11 (g6 h2 p3) | 0 |
| load append branch warm | 26 | 16 (g6 h4 l3 p3) | 0 | 10 (g5 h2 p3) | 0 |
| branch merge threeway warm | 135 | 47 (d3 g17 h19 l2 p6) | 1 (p1) | 87 (g62 h13 l4 p8) | 0 |
| branch delete warm | 18 | 16 (d1 g6 h4 l3 p2) | 2 (g1 p1) | 0 | 0 |
| branch create ff | 11 | 11 (g2 h3 l3 p3) | 0 | 0 | 0 |
| mutate branch ff | 48 | 33 (g22 h2 l6 p3) | 0 | 15 (g10 h2 p3) | 0 |
| branch merge fastforward warm | 54 | 27 (d2 g3 h16 l1 p5) | 1 (p1) | 26 (g17 h6 l2 p1) | 0 |
| branch delete ff | 18 | 16 (d1 g6 h4 l3 p2) | 2 (g1 p1) | 0 | 0 |
| mutate main cold | 544 | 15 (g7 h4 l1 p3) | 0 | 529 (g524 h2 p3) | 0 |
| branch create cold | 11 | 11 (g2 h3 l3 p3) | 0 | 0 | 0 |
| mutate branch cold | 562 | 33 (g22 h2 l6 p3) | 0 | 529 (g524 h2 p3) | 0 |
| branch merge fastforward cold | 72 | 45 (d2 g20 h14 l4 p5) | 1 (p1) | 26 (g17 h6 l2 p1) | 0 |
| branch delete cold | 29 | 27 (d1 g16 h2 l6 p2) | 2 (g1 p1) | 0 | 0 |
| load append main cold | 284 | 15 (g7 h4 l1 p3) | 0 | 269 (g264 h2 p3) | 0 |


History depth alone, branch counts and table counts (all at 32 commits unless named; the `d` cases update one row, so their table stays at about two fragments):

**Baseline: other growth dimensions**

| operation | 256 dead branches | 256 live branches | 32 tables, 256 commits | 2048 commits updating one row |
|---|---:|---:|---:|---:|
| mutate main warm | 19 | 19 | 278 | 19 |
| branch create warm | 11 | 11 | 11 | 11 |
| mutate branch warm | 51 | 51 | 58 | 51 |
| load append main warm | 12 | 12 | 12 | 12 |
| load merge branch warm | 31 | 31 | 33 | 31 |
| load merge main warm | 17 | 17 | 19 | 17 |
| load append branch warm | 31 | 31 | 33 | 31 |
| branch merge threeway warm | 132 | 132 | 912 | 132 |
| branch delete warm | 25 | 25 | 25 | 25 |
| branch create ff | 11 | 11 | 11 | 11 |
| mutate branch ff | 54 | 54 | 58 | 52 |
| branch merge fastforward warm | 68 | 68 | 1096 | 68 |
| branch delete ff | 25 | 25 | 25 | 25 |
| mutate main cold | 29 | 29 | 544 | 28 |
| branch create cold | 11 | 11 | 11 | 11 |
| mutate branch cold | 56 | 56 | 572 | 54 |
| branch merge fastforward cold | 87 | 87 | 1117 | 85 |
| branch delete cold | 38 | 38 | 39 | 37 |
| load append main cold | 25 | 25 | 284 | 24 |

**Final: other growth dimensions**

| operation | 256 dead branches | 256 live branches | 32 tables, 256 commits | 2048 commits updating one row |
|---|---:|---:|---:|---:|
| mutate main warm | 19 | 19 | 278 | 19 |
| branch create warm | 11 | 11 | 11 | 11 |
| mutate branch warm | 41 | 41 | 48 | 41 |
| load append main warm | 12 | 12 | 12 | 12 |
| load merge branch warm | 24 | 24 | 26 | 24 |
| load merge main warm | 17 | 17 | 19 | 17 |
| load append branch warm | 24 | 24 | 26 | 24 |
| branch merge threeway warm | 123 | 123 | 135 | 123 |
| branch delete warm | 18 | 18 | 18 | 18 |
| branch create ff | 11 | 11 | 11 | 11 |
| mutate branch ff | 44 | 44 | 48 | 42 |
| branch merge fastforward warm | 50 | 50 | 54 | 50 |
| branch delete ff | 18 | 18 | 18 | 18 |
| mutate main cold | 29 | 29 | 544 | 28 |
| branch create cold | 11 | 11 | 11 | 11 |
| mutate branch cold | 46 | 46 | 562 | 44 |
| branch merge fastforward cold | 66 | 66 | 72 | 64 |
| branch delete cold | 28 | 28 | 29 | 27 |
| load append main cold | 25 | 25 | 284 | 24 |


## Redesign prototype: merge reads follow the changed fragments

**Finding.** Before this run a merge's storage cost was a function of the
size (fragment count) of every table the source touched, not of the change:
the adopt route walked both versions in full, and the three-way lineage route
read candidates with filters over every fragment of three versions. That is
the production observation "merge 6.5–7 s and flat in the amount of
divergence".

**Design.** Lance version metadata already says which fragments differ
between two pins of one table: fragments one end lacks or rewrote, and
deletion vectors that differ. Under the exact-id primary key a live id is
unique within a version, so a row outside those fragments is live at the same
position on both ends and cannot be a candidate. H3 and H4 compute, per
comparison, the fragment scope of each end (`LineageSideScope`) during the
existing candidate discovery and read candidate rows only from there. Both
merge routes now cost the changed fragments plus a constant.

**Prototype counts.** Fast forward 54 and three-way 135 at 1,024 commits,
equal to their 256-commit values (tables above).

**Correctness evidence.** The GQT corpus in a debug build runs every merge
case under `merge_lineage = verify`, which now runs the full walk and the
scoped lineage path for adopts and for three-way merges and fails the merge
on any difference: 432 of 432 cases pass. The engine merge suites pass (144).
The scoping argument rests on the same exact-id primary-key contract the
lineage gate already checks (`exact_id_primary_key_field_id`); every other
gate (Blob schema, differing paths or schemas, stable row ids, the 32 MiB
candidate budget) still falls back to the walk.

**What a full implementation still needs.**

- Read candidate rows by row address instead of an `id IN` filter inside the
  scope. Discovery already knows the deleted offsets and the new fragments;
  the filter is now cheap only because the scope is small.
- A cost contract: a `merge_cost.rs` curve asserting flat requests over
  fragment depth for both routes, and DST golden rows `MergeAdoptHistory2`
  and `MergeAdoptHistory32` (a non-insert adopt at two depths).
- Failpoint and DST coverage of a scoped read that meets a pin whose fragment
  inventory changed under it (it cannot, since pins are immutable, but the
  test should fail loudly if that assumption breaks).
- A decision on keeping the full walk as the `off` fallback. It remains the
  route for Blob tables, which the lineage gate excludes; Blob merges still
  grow with the table.
- An explicit proof, or a guard, that tables forked before v11 (Lance branch
  datasets with inherited `base_id` files) keep the uniqueness argument across
  their bases. The existing gate accepts them; I did not construct one.

## Proposal, not prototyped: bound per-commit fragment growth

Every write that filters a table, and every cold strict insert, reads each
uncompacted fragment. A sync writer produces one fragment per commit, so this
cost grows with the commits since the last `optimize` (one-row update: 17
requests at depth 0, 1,046 at 1,024 and 2,070 at 2,048, warm; twice that
cold). History depth, branch counts and table counts do not cause it; the
fragment layout does. Two directions, both preserving the invariants:

Measured with the ignored DST instrument
`instrument_filtered_update_cost_over_per_commit_fragments`
(`crates/omnigraph-dst/tests/scenarios.rs`; storage calls of a one-row update
by key, `get`+`list`+`put`, DST cost ledger):

| one-row inserts before the update | cold, before `optimize` | warm repeat, before | cold, after `optimize` | warm repeat, after |
|---:|---:|---:|---:|---:|
| 32 | 88 | 20 | 21 | 18 |
| 256 | 538 | 20 | 29 | 20 |

Compaction removes the growth; a warm repeat hides it only because the same
handle read every fragment a moment before.

1. An idempotent reconciler that runs `optimize` and `index build` on a table
   whose fragment count since its last compaction passes a threshold. The
   invariants allow this shape (it derives everything from the manifest and
   needs no queue). Operationally, run `optimize` on the hosted graph now.
2. Index coverage for keyed predicates: an update `where <key> = ...` could
   resolve through the id BTREE plus the uncovered tail, which stays small
   only if the reconciler above keeps coverage current.

## Production context, explained from the counts

These are engine counts on a DST memory store; the hosted graph is served,
on S3, with about 47k artifacts. I had no access to it, so every mapping
below is an inference to verify there.

- **Branch create 1.8 s.** 11 requests, flat; 9 of them are serial (each
  listing, read or PUT waits on the one before). At S3 request latency plus
  server work that is about one second. Design, not fragmentation.
- **One-row mutate 5.5 s.** 17 requests on a fresh graph, plus one or two
  per uncompacted fragment of the table when the mutation filters it.
  With thousands of per-commit fragments that is thousands of GETs. Mostly
  storage fragmentation; the per-write serial chain (about 12 dependent
  requests on `main`, about 35 on a branch before H1) is design.
- **Branch merge 6.5–7 s, flat in divergence.** The adopt and three-way
  routes read every fragment of every touched table (about 4 and 3 per
  fragment). Design; fixed by H3 and H4.
- **Branch delete 5.2 s.** 25 serial requests (18 after H1) explain perhaps
  2–3 s at S3 latency. The rest is not explained by engine requests; look at
  the served route (admission, drain, policy) and at S3 PUT latency for the
  ref rewrite. Not verified.
- **10-row load 5.9 s versus ten one-row mutates 55.8 s.** Each write pays
  the fixed capture-and-publish chain and the per-fragment reads once; a load
  pays them once for ten rows. Consistent with the counts (a 10-row load on a
  warm handle is 12 requests, as cheap as a one-row insert).
- **About 800 GETs to reload `__manifest`.** Not reproduced: a graph created
  at format 16 keeps `__manifest` at 1–3 fragments through 2,048 commits and
  a cold reload reads 13 objects. The hosted graph's `__manifest` likely
  carries many fragments from an older format or from cold registry
  fragments. Check its fragment count on the hosted root before attributing
  the cost to the server release.

## Wall clock

No qualified benchmark environment was available, so there is no measured
wall clock. As a model only, the GQT `s3-like` latency model (17 ms per
request plus bytes at 50 MiB/s, unlimited overlap; `--model s3-like`):

| operation | no history: requests | no history: model ms | 256 commits: requests | 256 commits: model ms |
|---|---:|---:|---:|---:|
| mutate main warm | 17 → 17 | 265 → 265 | 278 → 278 | 429 → 429 |
| branch create warm | 11 → 11 | 173 → 173 | 11 → 11 | 173 → 173 |
| mutate branch warm | 52 → 42 | 826 → 656 | 58 → 48 | 941 → 771 |
| load append main warm | 12 → 12 | 176 → 176 | 12 → 12 | 179 → 179 |
| load merge branch warm | 31 → 24 | 507 → 388 | 33 → 26 | 546 → 427 |
| load merge main warm | 17 → 17 | 246 → 246 | 19 → 19 | 303 → 303 |
| load append branch warm | 31 → 24 | 507 → 388 | 33 → 26 | 546 → 427 |
| branch merge threeway warm | 132 → 123 | 1849 → 1747 | 912 → 135 | 2289 → 1971 |
| branch delete warm | 25 → 18 | 418 → 299 | 25 → 18 | 418 → 299 |
| branch create ff | 11 → 11 | 173 → 173 | 11 → 11 | 173 → 173 |
| mutate branch ff | 52 → 42 | 808 → 638 | 58 → 48 | 887 → 717 |
| branch merge fastforward warm | 68 → 50 | 873 → 771 | 1096 → 54 | 1235 → 845 |
| branch delete ff | 25 → 18 | 418 → 299 | 25 → 18 | 418 → 299 |
| mutate main cold | 28 → 28 | 354 → 354 | 544 → 544 | 557 → 557 |
| branch create cold | 11 → 11 | 173 → 173 | 11 → 11 | 173 → 173 |
| mutate branch cold | 54 → 44 | 808 → 638 | 572 → 562 | 1032 → 862 |
| branch merge fastforward cold | 85 → 64 | 1156 → 1003 | 1117 → 72 | 1559 → 1118 |
| branch delete cold | 37 → 27 | 629 → 459 | 39 → 29 | 649 → 479 |
| load append main cold | 24 → 24 | 318 → 318 | 284 → 284 | 449 → 449 |

The 256-commit "after" column is the H4 build (before the boxing fix, which changes no request).

Requests fell 7–20× on merges, modeled latency 14–32%: the removed reads
were parallel scans, and what remains is a chain of dependent requests (the
three-way merge's 123 requests take about 1.7 s under the model). On real S3
the gain lies between the two, depending on Lance's I/O parallelism. The
next latency target is that serial chain, not the request count.

## Gates

All on this box, debug builds. "Base" means the same command with the two
changed source files restored to `3e0e467e`.

| Gate | Result |
|---|---|
| GQT corpus, full (`omnigraph-gqt crates/omnigraph-gqt/cases`, debug `verify`: every merge compares lineage with the walk) | 432/432 on H3, H4 and the final stack |
| DST storage-cost golden (`dst_bench_cost_count_golden`) | regenerated in H1 and H4; every changed row decreases; H3 changes none |
| Engine merge and branch suites (`--test integration` `merge_fast_forward`, `merge_truth_table`, `merge_cost`, `branching`, `merge_cleanup`, `changes`) | 144 passed, 2 ignored (after the boxing fix; before it one stack overflow) |
| `omnigraph-core` lib, `omnigraph-catalog` lib | 87/87, 214/214 |
| Canonical workspace graph (`cargo dev-test --no-fail-fast`) | 3,529 passed, 7 failed, plus 5 threads that overflowed their stack. Base: 3,531 passed, 5 failed, the same 5 overflows. Every failure but two also fails on base (symlink refusal under this sandbox, CLI MCP, upgrade interruption, server owned shutdown). The two HEAD-only failures (`app::tests::baseline_keeps_installed_snapshot_locked_through_cursor_delivery`, `operator_transport::tests::http_connect_proxy_preserves_custom_ca_and_hides_origin_credentials`) are flaky: the same binary passes and fails them across reruns, and neither reaches the changed code. |
| DST suite (`cargo test` in `crates/omnigraph-dst`: crash, concurrency, faults, golden) | 112 passed, 31 ignored (fleet and hunt instruments), 0 failed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings -W clippy::dbg_macro`; the same for `-p omnigraph-gqt` | clean, after one `needless_option_as_deref` fix in H3's code (commit `41d37eb2`) |
| `cargo fmt --all --check` | clean |

The overflows on base mean this box's environment does not reproduce CI's
green run; CI on a pull request is the authoritative check and has not run.


## What could not be verified

- Wall clock on a qualified environment; the served path; S3 itself.
- The hosted graph's `__manifest` fragment count and the server-side part of
  branch delete's 5.2 s.
- The `i2048` replay: each DST seed runs twice; the second run of the 2,048
  commit case passed the 600 s budget, so its counts come from the first run
  only (the baseline and the final run both).
- CI. This box overflows the stack in five workspace tests and fails eight
  more on the base commit too (Gates), so its canonical run is not green on
  main either; only a CI run on a pull request is authoritative.

## Workflow feedback

- The instructions name the bench harness (`benchmarks.yaml`, per-sample
  `logical_store_calls`) as the instrument, but it needs a qualified release
  build and has no branch-write workloads. GQT `--measure` is the practical
  instrument for counts and should be named, with its realm caveat
  (`__history` is classed as `table_*`).
- Missing: debug builds default `merge_lineage`/`change_lineage` to
  `verify`, which doubles merge counts. The definition should say to pin
  release session defaults in every measured case.
- "Execute in its own cloud dev box and stop it when the run ends" conflicts
  with running inside that box: stopping it ends the session that writes the
  final reply. I scheduled a delayed stop after pushing; the orchestrator may
  prefer to stop the box itself.
- "One change per attempt, committed separately" works, but the DST
  storage-cost golden must be regenerated per commit, and a regeneration
  build overlapped my next edit once; the definition could suggest parking
  the next change while a gate builds.
- History depth in production means two things here: commit depth (cheap)
  and per-commit fragments (expensive). Naming both in the scope would have
  saved the first, flat, baseline.
- The 6-hour budget fits about five hypotheses on this box: an engine change
  rebuilds in 1–7 minutes, but each gate (DST golden, corpus, canonical test
  graph) is 10–60 minutes.
