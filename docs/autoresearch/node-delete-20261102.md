# Autoresearch: node delete, 2026-11-02

Operation group: deleting nodes on `main` and on a named branch, warm and
cold. Forms measured: one node by `@key` with no edges, with two edges and
with a 64-edge hub; several nodes by a non-key predicate; an explicit edge
delete by `@src`; a predicate deleting N nodes with N edges; and the
`load csv … mode overwrite` removal of a whole type.

- Branch: `autoresearch/node-delete-20261102` on the fork `ragnorc/omnigraph`
- Base commit: `de776c14473d91130bb55529392102fdce5b5c67` (main, 2026-11-02)
- Box: Modal dev box, Claude Opus 5.5 (`claude-opus-5-5`)
- Prior runs read: `autoresearch/branch-ops-20261102` (merge reads, #1229)
  and `autoresearch/warm-read-main-20261102` (stale-read refresh, #1230) on
  the fork. Neither touched the delete path, and no hypothesis here repeats
  theirs. No autoresearch `Workflow` is visible in the read view of the
  `personal` graph (38 workflows, none of them this one), so the reports on
  the fork are the record I used.
- The run was stopped once by the orchestrator to widen the scope from
  history to every growth dimension; the branch, scripts and notes survived
  and the work continued from them.

## Result

A delete now reads each table it deletes from once, uses the id index for a
delete by key, and validates against cached tables. Three hypotheses kept
(H1–H3), two analysed and not built (H4, H5). Totals over the targeted
scenarios (every measured delete step of all 33 cases, 319 steps; branch
creates and restarts excluded): **51,939 requests before, 29,891 after
(−42%)**; no measured step rose.

What a one-node delete costs now, by what it grows with:

- **Flat** in commit depth, dead and live branches, untouched tables, row
  width, hub degree and the size of the delete (1 to 4,096 nodes: 24 → 35).
- **Flat in fragments when the table is indexed**: a cold delete by key is
  37 / 49 / 55 requests at 32 / 256 / 1,024 indexed fragments (baseline 87 /
  327 / 1,102). It then grows by about one request per *uncovered* fragment
  (49 → 113 → 304 at 0 / 64 / 256 fragments written after the index).
- **Still grows when the table is not indexed**: about one read per fragment
  of the node table and of every incident edge table on first touch (cold
  one-edge delete at 1,024 per-commit fragments: 3,130 → 2,090), and bytes
  linear in table rows (100k rows: 6.3 MB → 3.9 MB read for a one-node
  delete). That follows from the design (a key or adjacency lookup without
  an index is a scan) and needs index coverage, not another patch; see the
  redesign proposal.
- **Grows with the number of incident edge types** on first touch: about 3
  requests per incident edge table on a cold handle (2 → 18 types: 107 →
  155), because the cascade and its check look in every incident table.

The main finding, in the brief's terms: before this run a one-node delete
grew with things it should not depend on — the number of incident edge
types even on a warm handle (124 vs 76 requests), every fragment of every
incident edge table on a warm handle (1,068 requests at 1,024 fragments for
a delete whose node had 2 edges), and the bytes of the whole key column read
twice. H3 removed the warm growth (31 at every depth and type count), H1 the
double read, H2 the unindexed key lookup on indexed tables.

## What a node delete does

Traced from `crates/omnigraph/src/exec/mutation.rs` and the request logs:

1. Capture: freshness probe of `__manifest` (HEAD of the held version and
   of the next).
2. `execute_delete_node`: open the node table at its pin, scan
   `filter → __id` for the matched ids (budgeted by `write_max_bytes`).
3. Cascade: for **every edge type whose source or target is the node type**,
   open its table and scan `__src IN ids OR __dst IN ids` for the edge ids;
   record a delete predicate where anything matched. Edges of a deleted node
   are always deleted (cascade); there is no refusal and no orphan state.
4. Validation (`validate_staged_mutation` → `evaluate_edge_ri`): for every
   edge type with a deleted endpoint, scan the edge table again for edges
   that still reference a deleted id (`CommittedState::edges_referencing`)
   and refuse if one is not being removed. Also `@card` recounts.
5. `stage_all`: per table, reopen the pin and run Lance's
   `DeleteBuilder::execute_uncommitted`, which **scans the table again with
   the combined predicate** to find row addresses, reads each affected
   fragment's deletion file and writes the extended one.
6. `commit_all`: detached commit per table (transaction file + manifest
   PUT), then one `__manifest` publish (listing + data + transaction +
   manifest PUT).

An explicit edge delete is steps 2, 5 and 6 on the edge table (13 requests,
flat in every dimension). The overwrite removal (`load csv … overwrite`) is
27 requests and flat; it does not cascade.

Before this run, steps 2, 3, 4 and 5 each read the matched columns of every
fragment: the node table twice (2 and 5), each incident edge table up to
three times (3, 4 and 5). Step 4 also opened each table through a
zero-capacity statement session, so every open re-read the manifest and
every index lookup re-read the manifest's index section (HEAD + GET, up to
nine times per indexed edge table).

## Method

**Instrument.** GQT `--measure` on the DST in-memory object store
(`omnigraph-engine-dst`, seed 0): every request of every step with its
path, verb, bytes and seam phase. Counts are exact and each seed runs twice
in fresh workers, which must agree. HEADs and failed requests count. Realms
are classified by path (`scripts/autoresearch/node_delete/analyze.py`):
`manifest` (`__manifest`, its branch trees and refs), `history`
(`__history`), `Person` (the node table), `incident edges` (edge tables
with Person as an endpoint), `other tables`, `control`. The DST storage-cost
golden (`crates/omnigraph-dst/cost_table.txt`) is the second counter; its
changed rows are listed under Gates.

**Cases.** `scripts/autoresearch/node_delete/gen_cases.py` writes one case
family per dimension, holding the others small; `run.sh` runs them;
`analyze.py`, `compare.py` and `report_tables.py` tabulate. Each case builds
its state and then runs a fixed measured tail (T01–T16): deletes on `main`
on the handle that built the state ("warm", as a long-running server holds),
a branch create and two branch deletes, then deletes after `--- restart`
("cold"). The `f`/`g` families build Person in N one-row commits, then (for
`f`) let GQT build every declared index over them, and run a shorter tail
(F01–F07). Every step asserts its `affected` counts, so a wrong cascade
fails the case. Every case sets `merge_lineage` and `change_lineage` to `on`
(release defaults).

**Warm numbers on this store.** Lance's file reader reads a small file's
last 64 KiB block whole and the session keeps it, so one-row per-commit
fragments (the sync-writer shape) are served from cache once any read has
touched them. That is why T02–T04 are flat in fragments after T01 warmed
the handle. On large fragments only the file metadata is cached and the
data pages are read again. Cold and first-touch rows (T01, T10, T12, T14,
F05) are the conservative numbers.

**Not the bench harness.** `omnigraph-bench` needs a qualified release
build in SHA-attested workers, which this box cannot provide (its machine
identity probe fails under gVisor: no `Cpus_allowed_list`).

## Expected versus measured growth

What a one-node delete should cost: find the node (an index lookup, or a
scan of the key column if there is no index), find its incident edges (an
index or adjacency lookup per incident edge type), extend one deletion
vector per affected fragment, one detached commit per touched table, one
publish. It should grow with the edges deleted and the fragments they live
in, and with the number of incident edge types only as one cheap lookup
each. It should not grow with history, branches, untouched tables, row
width, or (given an index) total rows and fragments.

Measured before: grew with incident edge types and incident edge fragments
even warm; read the key column twice; ignored the id index. Measured after:
the warm path is flat everywhere; the cold path is flat in fragments where
an index covers them, and grows by about one read per uncovered fragment per
touched table. The remaining growth is the absence of an index, which is a
maintenance and design question (proposal below), not a redundant call.

## Growth curves, baseline → final

Requests per operation (and KiB read where size matters). "Final" is the
branch head (H1–H3). `DIMENSIONS` in `report_tables.py` maps each column
to its case in `gen_cases.py`. Warm T02–T04 rows on per-commit fragments are
flat partly because T01 already pulled the small files into the session
cache (Method); the T01, T10, T12 and T14 rows are the first-touch costs.

**Per-commit fragments (N one-row Person commits and N one-row Knows commits)** (requests, baseline → final)

| operation | 0 | 32 | 256 | 1024 |
|---|---:|---:|---:|---:|
| T01 one node, no edges, main warm | 32 → 24 | 135 → 93 | 807 → 541 | 3111 → 2077 |
| T02 one node, 2 edges, main warm | 39 → 28 | 76 → 31 | 300 → 31 | 1068 → 31 |
| T03 hub node (64 edges unless stated), main warm | 42 → 28 | 82 → 34 | 306 → 34 | 1074 → 34 |
| T04 predicate, 5 nodes, main warm | 30 → 21 | 70 → 27 | 294 → 27 | 1062 → 27 |
| T05 edge delete by @src, main warm | 14 → 13 | 14 → 13 | 14 → 13 | 14 → 13 |
| T07 one node, 1 edge, branch warm (first write) | 73 → 59 | 115 → 67 | 339 → 67 | 1105 → 65 |
| T08 hub2 (64 edges), branch warm | 55 → 41 | 95 → 47 | 319 → 47 | 1087 → 47 |
| T10 one node, 1 edge, main cold | 50 → 36 | 155 → 107 | 827 → 555 | 3130 → 2090 |
| T12 one node, 1 edge, branch cold | 76 → 62 | 182 → 134 | 854 → 582 | 3156 → 2116 |
| T14 one node, no edges, main cold | 39 → 30 | 144 → 101 | 816 → 549 | 3119 → 2084 |
| T15 load csv overwrite of Company | 27 → 27 | 27 → 27 | 27 → 27 | 27 → 27 |

**Commit depth without fragment growth (one-row updates of one node)** (requests, baseline → final)

| operation | 0 | 512 |
|---|---:|---:|
| T01 one node, no edges, main warm | 32 → 24 | 35 → 27 |
| T02 one node, 2 edges, main warm | 39 → 28 | 39 → 28 |
| T03 hub node (64 edges unless stated), main warm | 42 → 28 | 42 → 28 |
| T04 predicate, 5 nodes, main warm | 30 → 21 | 30 → 21 |
| T05 edge delete by @src, main warm | 14 → 13 | 14 → 13 |
| T07 one node, 1 edge, branch warm (first write) | 73 → 59 | 75 → 61 |
| T08 hub2 (64 edges), branch warm | 55 → 41 | 55 → 41 |
| T10 one node, 1 edge, main cold | 50 → 36 | 52 → 38 |
| T12 one node, 1 edge, branch cold | 76 → 62 | 79 → 65 |
| T14 one node, no edges, main cold | 39 → 30 | 41 → 32 |
| T15 load csv overwrite of Company | 27 → 27 | 27 → 27 |

**Deletion files from earlier deletes (N four-row fragments, one row deleted in each)** (requests, baseline → final)

| operation | 0 | 64 | 256 |
|---|---:|---:|---:|
| T01 one node, no edges, main warm | 32 → 24 | 158 → 86 | 542 → 278 |
| T02 one node, 2 edges, main warm | 39 → 28 | 171 → 96 | 555 → 288 |
| T03 hub node (64 edges unless stated), main warm | 42 → 28 | 174 → 96 | 558 → 288 |
| T04 predicate, 5 nodes, main warm | 30 → 21 | 34 → 25 | 34 → 25 |
| T05 edge delete by @src, main warm | 14 → 13 | 14 → 13 | 14 → 13 |
| T07 one node, 1 edge, branch warm (first write) | 73 → 59 | 207 → 129 | 591 → 321 |
| T08 hub2 (64 edges), branch warm | 55 → 41 | 187 → 109 | 571 → 301 |
| T10 one node, 1 edge, main cold | 50 → 36 | 247 → 169 | 823 → 553 |
| T12 one node, 1 edge, branch cold | 76 → 62 | 274 → 196 | 850 → 580 |
| T14 one node, no edges, main cold | 39 → 30 | 236 → 163 | 812 → 547 |
| T15 load csv overwrite of Company | 27 → 27 | 27 → 27 | 27 → 27 |

**Rows in Person and in Knows (one fragment each)** (requests, baseline → final, KiB read)

| operation | ~80 | 1k | 10k | 100k |
|---|---:|---:|---:|---:|
| T01 one node, no edges, main warm | 32 → 24 (24 → 15) | 37 → 28 (98 → 79) | 40 → 30 (481 → 394) | 40 → 30 (4706 → 3916) |
| T02 one node, 2 edges, main warm | 39 → 28 (33 → 18) | 40 → 28 (127 → 78) | 42 → 28 (631 → 387) | 42 → 28 (6265 → 3908) |
| T03 hub node (64 edges unless stated), main warm | 42 → 28 (36 → 19) | 43 → 28 (136 → 85) | 45 → 28 (633 → 387) | 45 → 28 (6265 → 3906) |
| T04 predicate, 5 nodes, main warm | 30 → 21 (24 → 13) | 32 → 22 (77 → 64) | 33 → 22 (371 → 332) | 33 → 22 (3647 → 3377) |
| T05 edge delete by @src, main warm | 14 → 13 (4 → 3) | 14 → 13 (29 → 16) | 14 → 13 (141 → 72) | 14 → 13 (1559 → 781) |
| T07 one node, 1 edge, branch warm (first write) | 73 → 59 (82 → 65) | 74 → 59 (176 → 125) | 76 → 59 (680 → 434) | 76 → 59 (6312 → 3952) |
| T08 hub2 (64 edges), branch warm | 55 → 41 (42 → 25) | 56 → 41 (135 → 84) | 58 → 41 (640 → 393) | 58 → 41 (6271 → 3912) |
| T10 one node, 1 edge, main cold | 50 → 36 (61 → 44) | 55 → 40 (159 → 108) | 59 → 42 (668 → 422) | 59 → 42 (6308 → 3948) |
| T12 one node, 1 edge, branch cold | 76 → 62 (99 → 80) | 81 → 66 (194 → 141) | 85 → 68 (706 → 457) | 85 → 68 (6346 → 3984) |
| T14 one node, no edges, main cold | 39 → 30 (48 → 38) | 44 → 34 (121 → 102) | 47 → 36 (505 → 417) | 47 → 36 (4730 → 3939) |
| T15 load csv overwrite of Company | 27 → 27 (4 → 4) | 27 → 27 (4 → 4) | 27 → 27 (4 → 4) | 27 → 27 (4 → 4) |

**Row width (filler rows with a 16 KiB string and a 16 KiB Blob)** (requests, baseline → final, KiB read)

| operation | 1k narrow | 256 wide | 1024 wide |
|---|---:|---:|---:|
| T01 one node, no edges, main warm | 37 → 28 (98 → 79) | 39 → 30 (46 → 36) | 39 → 30 (100 → 81) |
| T02 one node, 2 edges, main warm | 40 → 28 (127 → 78) | 40 → 28 (57 → 35) | 40 → 28 (130 → 80) |
| T03 hub node (64 edges unless stated), main warm | 43 → 28 (136 → 85) | 43 → 28 (60 → 36) | 43 → 28 (133 → 81) |
| T04 predicate, 5 nodes, main warm | 32 → 22 (77 → 64) | 33 → 23 (37 → 27) | 33 → 23 (79 → 66) |
| T05 edge delete by @src, main warm | 14 → 13 (29 → 16) | 14 → 13 (11 → 7) | 14 → 13 (29 → 16) |
| T07 one node, 1 edge, branch warm (first write) | 74 → 59 (176 → 125) | 74 → 59 (109 → 84) | 74 → 59 (181 → 129) |
| T08 hub2 (64 edges), branch warm | 56 → 41 (135 → 84) | 56 → 41 (66 → 42) | 56 → 41 (139 → 87) |
| T10 one node, 1 edge, main cold | 55 → 40 (159 → 108) | 57 → 42 (91 → 66) | 57 → 42 (163 → 111) |
| T12 one node, 1 edge, branch cold | 81 → 66 (194 → 141) | 83 → 68 (129 → 102) | 83 → 68 (202 → 148) |
| T14 one node, no edges, main cold | 44 → 34 (121 → 102) | 46 → 36 (70 → 59) | 46 → 36 (125 → 105) |
| T15 load csv overwrite of Company | 27 → 27 (4 → 4) | 27 → 27 (4 → 4) | 27 → 27 (4 → 4) |

**Declared indexes (BTREE, full-text, vector) over N rows, one fragment** (requests, baseline → final, KiB read)

| operation | 1k | 10k | 100k |
|---|---:|---:|---:|
| T01 one node, no edges, main warm | 74 → 36 (143 → 79) | 78 → 40 (366 → 153) | 78 → 40 (1797 → 162) |
| T02 one node, 2 edges, main warm | 72 → 26 (97 → 24) | 74 → 26 (251 → 20) | 74 → 26 (1683 → 23) |
| T03 hub node (64 edges unless stated), main warm | 67 → 28 (117 → 34) | 70 → 28 (287 → 23) | 70 → 28 (1719 → 26) |
| T04 predicate, 5 nodes, main warm | 54 → 22 (92 → 21) | 57 → 23 (149 → 31) | 57 → 23 (157 → 34) |
| T05 edge delete by @src, main warm | 13 → 13 (4 → 4) | 13 → 13 (3 → 3) | 13 → 13 (4 → 4) |
| T07 one node, 1 edge, branch warm (first write) | 98 → 59 (163 → 81) | 101 → 59 (341 → 77) | 101 → 59 (1773 → 80) |
| T08 hub2 (64 edges), branch warm | 80 → 41 (112 → 29) | 83 → 41 (292 → 28) | 83 → 41 (1725 → 31) |
| T10 one node, 1 edge, main cold | 89 → 55 (183 → 121) | 95 → 60 (416 → 197) | 95 → 60 (1859 → 213) |
| T12 one node, 1 edge, branch cold | 115 → 81 (240 → 161) | 121 → 86 (473 → 236) | 121 → 86 (1916 → 253) |
| T14 one node, no edges, main cold | 73 → 43 (160 → 103) | 77 → 47 (388 → 177) | 77 → 47 (1822 → 188) |
| T15 load csv overwrite of Company | 27 → 27 (7 → 7) | 27 → 27 (7 → 7) | 27 → 27 (7 → 7) |

**Indexed Person fragments (N one-row commits, then indexes built)** (requests, baseline → final, KiB read)

| operation | 32 | 256 | 1024 |
|---|---:|---:|---:|
| F01 one node, 1 edge, main warm | 45 → 23 (28 → 12) | 56 → 29 (83 → 51) | 63 → 34 (97 → 62) |
| F02 one node, 1 edge, main warm (second) | 50 → 24 (37 → 19) | 57 → 24 (104 → 61) | 62 → 26 (214 → 163) |
| F03 predicate, 4 nodes, main warm | 50 → 24 (37 → 19) | 57 → 24 (100 → 57) | 62 → 26 (213 → 163) |
| F05 one node, 1 edge, main cold | 87 → 37 (106 → 64) | 327 → 49 (526 → 256) | 1102 → 55 (1358 → 347) |
| F07 one node, 1 edge, branch warm | 83 → 57 (123 → 105) | 94 → 61 (418 → 375) | 99 → 63 (482 → 432) |

**Uncovered fragments after the index (256 indexed fragments, then N one-row commits)** (requests, baseline → final, KiB read)

| operation | 0 | 64 | 256 |
|---|---:|---:|---:|
| F01 one node, 1 edge, main warm | 56 → 29 (83 → 51) | 127 → 99 (196 → 163) | 319 → 291 (406 → 374) |
| F02 one node, 1 edge, main warm (second) | 57 → 24 (104 → 61) | 57 → 24 (113 → 70) | 57 → 24 (139 → 96) |
| F03 predicate, 4 nodes, main warm | 57 → 24 (100 → 57) | 57 → 24 (109 → 66) | 57 → 24 (135 → 93) |
| F05 one node, 1 edge, main cold | 327 → 49 (526 → 256) | 391 → 113 (610 → 340) | 582 → 304 (741 → 470) |
| F07 one node, 1 edge, branch warm | 94 → 61 (418 → 375) | 94 → 61 (455 → 411) | 92 → 59 (323 → 280) |

**The same fragments without indexes** (requests, baseline → final, KiB read)

| operation | 32 | 256 | 1024 |
|---|---:|---:|---:|
| F01 one node, 1 edge, main warm | 64 → 59 (64 → 57) | 295 → 286 (337 → 317) | 1065 → 1055 (1218 → 1185) |
| F02 one node, 1 edge, main warm (second) | 35 → 26 (30 → 24) | 39 → 26 (82 → 62) | 40 → 26 (230 → 198) |
| F03 predicate, 4 nodes, main warm | 35 → 26 (30 → 24) | 39 → 26 (82 → 62) | 40 → 26 (230 → 198) |
| F05 one node, 1 edge, main cold | 68 → 59 (92 → 86) | 302 → 289 (482 → 461) | 1072 → 1058 (1340 → 1307) |
| F07 one node, 1 edge, branch warm | 68 → 59 (113 → 107) | 76 → 63 (394 → 374) | 77 → 63 (497 → 464) |

**Edges on the hub node (T03)** (requests, baseline → final)

| operation | 16 | 64 | 1024 |
|---|---:|---:|---:|
| T01 one node, no edges, main warm | 32 → 24 | 32 → 24 | 35 → 27 |
| T02 one node, 2 edges, main warm | 39 → 28 | 39 → 28 | 39 → 28 |
| T03 hub node (64 edges unless stated), main warm | 42 → 28 | 42 → 28 | 42 → 28 |
| T04 predicate, 5 nodes, main warm | 30 → 21 | 30 → 21 | 33 → 24 |
| T05 edge delete by @src, main warm | 14 → 13 | 14 → 13 | 14 → 13 |
| T07 one node, 1 edge, branch warm (first write) | 73 → 59 | 73 → 59 | 76 → 61 |
| T08 hub2 (64 edges), branch warm | 55 → 41 | 55 → 41 | 58 → 43 |
| T10 one node, 1 edge, main cold | 50 → 36 | 50 → 36 | 56 → 41 |
| T12 one node, 1 edge, branch cold | 76 → 62 | 76 → 62 | 79 → 65 |
| T14 one node, no edges, main cold | 39 → 30 | 39 → 30 | 44 → 35 |
| T15 load csv overwrite of Company | 27 → 27 | 27 → 27 | 27 → 27 |

**Incident edge types (32 history commits)** (requests, baseline → final)

| operation | 2 | 18 |
|---|---:|---:|
| T01 one node, no edges, main warm | 135 → 93 | 231 → 141 |
| T02 one node, 2 edges, main warm | 76 → 31 | 124 → 31 |
| T03 hub node (64 edges unless stated), main warm | 82 → 34 | 130 → 34 |
| T04 predicate, 5 nodes, main warm | 70 → 27 | 118 → 27 |
| T05 edge delete by @src, main warm | 14 → 13 | 14 → 13 |
| T07 one node, 1 edge, branch warm (first write) | 115 → 67 | 195 → 99 |
| T08 hub2 (64 edges), branch warm | 95 → 47 | 143 → 47 |
| T10 one node, 1 edge, main cold | 155 → 107 | 251 → 155 |
| T12 one node, 1 edge, branch cold | 182 → 134 | 278 → 182 |
| T14 one node, no edges, main cold | 144 → 101 | 240 → 149 |
| T15 load csv overwrite of Company | 27 → 27 | 27 → 27 |

**Untouched tables (32 node types, 16 edge types not incident to Person)** (requests, baseline → final)

| operation | 0 | 48 |
|---|---:|---:|
| T01 one node, no edges, main warm | 32 → 24 | 32 → 24 |
| T02 one node, 2 edges, main warm | 39 → 28 | 39 → 28 |
| T03 hub node (64 edges unless stated), main warm | 42 → 28 | 42 → 28 |
| T04 predicate, 5 nodes, main warm | 30 → 21 | 30 → 21 |
| T05 edge delete by @src, main warm | 14 → 13 | 14 → 13 |
| T07 one node, 1 edge, branch warm (first write) | 73 → 59 | 75 → 61 |
| T08 hub2 (64 edges), branch warm | 55 → 41 | 55 → 41 |
| T10 one node, 1 edge, main cold | 50 → 36 | 51 → 37 |
| T12 one node, 1 edge, branch cold | 76 → 62 | 78 → 64 |
| T14 one node, no edges, main cold | 39 → 30 | 40 → 31 |
| T15 load csv overwrite of Company | 27 → 27 | 27 → 27 |

**Branches (32 history commits)** (requests, baseline → final)

| operation | none | 256 dead | 256 live |
|---|---:|---:|---:|
| T01 one node, no edges, main warm | 135 → 93 | 135 → 93 | 135 → 93 |
| T02 one node, 2 edges, main warm | 76 → 31 | 76 → 31 | 76 → 31 |
| T03 hub node (64 edges unless stated), main warm | 82 → 34 | 82 → 34 | 82 → 34 |
| T04 predicate, 5 nodes, main warm | 70 → 27 | 70 → 27 | 70 → 27 |
| T05 edge delete by @src, main warm | 14 → 13 | 14 → 13 | 14 → 13 |
| T07 one node, 1 edge, branch warm (first write) | 115 → 67 | 115 → 67 | 115 → 67 |
| T08 hub2 (64 edges), branch warm | 95 → 47 | 95 → 47 | 95 → 47 |
| T10 one node, 1 edge, main cold | 155 → 107 | 155 → 107 | 155 → 107 |
| T12 one node, 1 edge, branch cold | 182 → 134 | 182 → 134 | 182 → 134 |
| T14 one node, no edges, main cold | 144 → 101 | 144 → 101 | 144 → 101 |
| T15 load csv overwrite of Company | 27 → 27 | 27 → 27 | 27 → 27 |

**Nodes deleted by one predicate, each with one edge (T16)** (requests, baseline → final)

| operation | 1 | 16 | 256 | 4096 |
|---|---:|---:|---:|---:|
| T16 predicate deleting N nodes and N edges, main warm | 38 → 24 | 38 → 24 | 39 → 25 | 52 → 35 |

## Hypotheses

Counts are total requests per step, baseline → after (KiB read where the
change is about bytes). Each kept change is its own commit; the DST golden
regeneration is a separate commit.

| # | What, where | Counts before → after | Gates | Outcome |
|---|---|---|---|---|
| H1 | Capture each matched row's `_rowaddr` in the delete's own id scan and stage the deletion vectors from those addresses (`TableStore::stage_delete_rows`: `FileFragment::extend_deletions` per affected fragment, the same `Operation::Delete` and `affected_rows` Lance's `DeleteBuilder` builds after its scan), instead of letting `DeleteBuilder` scan the table again. Addresses are recorded per table at its pin; a version mismatch, or a predicate recorded without addresses, takes the scanning delete. Commit `d1790636`. | −1 to −2 requests per deleted table on single-fragment tables, and the second read of every fragment where files are not cached: x0064 two-edge delete 171 → 105; s100k two-edge delete 6,265 → 3,931 KiB; i100k 1,683 → 895 KiB. DST golden: `DeletePerson` l.get 6 → 5, `RemoveFriendshipsFrom` 20 → 19. | forbidden-API guard extended (`.extend_deletions(` sealed in `table_store.rs`, both new trait methods registered); all gates below | **Kept** |
| H2 | For a node delete whose predicate has a top-level `<key> = <String literal or parameter>` conjunct on the type's single String `@key`, add `__id = derive(v)` (the planner's `canonical_key_id_of`) to the id scan when the pinned table has an id BTREE, as `scan_access::narrow_key` does for reads. The recorded predicate stays the written one. Commit `0708062d`. | Indexed fragments, cold: f0256 327 → 81 (H1+H2), f0032 87 → 63. Bytes on one indexed 100k-row fragment: 895 → 111 KiB. Warm single-fragment indexed tables first paid +2 to +8 requests (index pages, and step 4's uncached index-metadata reads, which H3 then removed): i100k cold 94 → 99 before H3, 60 after. Unindexed tables: no change. | as H1 | **Kept**, with the warm +2…+8 on single-fragment indexed tables stated; after H3 every indexed step is below baseline |
| H3 | Mutation validation reads the write's pinned base through the shared Lance session and the held-handle cache keyed by the exact pin, as the write's own opens already do, instead of a snapshot without read caches (a zero-capacity statement session per open). Commit `e4f40bca`. | Warm two-edge delete 37 → 28 (fresh graph), 298 → 31 (256 fragments), 122 → 31 (18 incident edge types); i100k 73 → 26; cold one-edge delete at 32 fragments 153 → 107. DST golden: `AddFriend` (edge insert) l.get 111 → 60, `_verify` 1471 → 1463. | as H1 | **Kept** |
| H4 | Let edge-RI validation reuse the cascade's own result for the same table, pin and id set instead of scanning again. | Not built. After H3 the rescan costs 2–3 requests on a warm single-fragment table and is served from cache on per-commit fragments (Knows data GETs on a cold branch delete 75 → 39 from H3 alone). | — | **Dropped before building**: small gain, and feeding the check its own cascade's answer would remove the independent check that a cascade bug would trip. |
| H5 | Skip incident edge tables whose manifest `entity_count` is 0 in the cascade and the RI check, saving the open and scan (~3 requests each, cold). | Not built. | — | **Not pursued**: it turns a manifest statistic into a logical shortcut for integrity; a wrong count from any writer path would strand edges silently. Only helps empty edge types. |

**Per-hypothesis attribution on representative steps** (requests, KiB read;
"not run" where that intermediate binary did not run the case):

| case, step | baseline | H1 | H1+H2 | H1+H2+H3 (final) |
|---|---:|---:|---:|---:|
| p0000 T02 two-edge delete, warm | 39 (33) | 37 (29) | 37 (29) | 28 (18) |
| p0000 T10 one-edge delete, cold | 50 (61) | 48 (57) | 48 (57) | 36 (44) |
| p0032 T10 one-edge delete, cold | 155 (197) | 153 (193) | 153 (193) | 107 (149) |
| p0256 T02 two-edge delete, warm | 300 (296) | 298 (292) | not run | 31 (60) |
| s100k T02 two-edge delete, warm | 42 (6,265) | 40 (3,931) | 40 (3,931) | 28 (3,908) |
| i100k T02 two-edge delete, warm | 74 (1,683) | 73 (895) | 73 (111) | 26 (23) |
| i100k T10 one-edge delete, cold | 95 (1,859) | 94 (1,071) | 99 (332) | 60 (213) |
| f0256 F05 one-edge delete, cold | 327 (526) | not run | 81 (298) | 49 (256) |
| t16 T02 two-edge delete, warm | 124 (99) | 122 (95) | 122 (95) | 31 (30) |
| t16 T10 one-edge delete, cold | 251 (266) | 249 (262) | 249 (262) | 155 (194) |
| x0064 T02 two-edge delete, warm | 171 (163) | 105 (103) | 105 (103) | 96 (92) |
| n0256 T16 256 nodes + 256 edges | 39 (51) | 37 (47) | 37 (47) | 25 (29) |

## Redesign proposal: deletes find rows through indexes whose coverage is kept current

**Finding.** After H1–H3 a delete's remaining growth is entirely "find the
rows": the node by key, its edges by `__src`/`__dst`, in every incident edge
table. With no index that is a read of the key or endpoint columns of every
fragment (≈1 request per fragment cold, bytes linear in rows). With an id
BTREE and edge `__src`/`__dst` BTREEs it is an index lookup plus the
uncovered tail. The curves above show both regimes directly:

| fragments in Person | 32 | 256 | 1,024 |
|---|---:|---:|---:|
| cold one-edge delete, indexes built over all of them (final) | 37 | 49 | 55 |
| the same without indexes (final) | 59 | 289 | 1,058 |
| 256 indexed + 0 / 64 / 256 written after the index (final) | 49 | 113 | 304 |

So the design point is not the delete path any more; it is index coverage.
OmniGraph builds indexes only on explicit `index build`/`ensure_indices`,
and per the Lance alignment guide never adds delta segments (lance#9817:
scalar and full-text search don't apply segment ownership), so every
rebuild is whole. A sync writer making one fragment per commit therefore
leaves an ever-growing uncovered tail, and every delete, update and keyed
insert scans it.

**Prototype (walking skeleton).** H2 plus the existing `ensure_indices`:
the `f`/`g`/`f…u` cases measure exactly the regime a reconciler would keep
the graph in. No new engine code beyond H2 was needed to measure it.

**What a full implementation needs.**

1. An idempotent reconciler (allowed by the invariants: derived from
   manifest state, no queue) that runs `optimize` and `index build` on a
   table once its uncovered fragments pass a threshold. The threshold is a
   cost trade: a whole BTREE rebuild reads every row, so it should follow
   the write rate, not each write. This is the same proposal the branch-ops
   run made for filtered updates; deletes give it a second, measured
   reason.
2. A DST golden pair (`DeleteKeyIndexed2`/`DeleteKeyIndexed32`) recording a
   keyed delete at two fragment counts under full coverage, so flatness is a
   contract, and a GQT case asserting `affected` counts through the narrowed
   path on a table whose ids came from every write path (load, mutate,
   merge, upgrade), since H2 relies on `id = derive(key)` holding for every
   row, as the read planner already does.
3. For tables that are never indexed, a cheaper pruning structure is the
   known alternative: per-file key ranges or bloom filters, as Apache Hudi
   keeps for upserts and deletes (see Sources). Lance has zone-map and
   bloom-filter scalar indexes; OmniGraph does not build them yet, and they
   carry the same coverage question.
4. For the incident-edge-type dimension, the graph-native answer is an
   adjacency structure per edge table (Kuzu keeps forward and backward CSR
   and still visits every incident rel table on `DETACH DELETE`, but each
   visit is one adjacency read, not a scan). OmniGraph's `GraphIndex` is a
   per-snapshot cache keyed by edge-table versions; every edge write
   invalidates it, so it cannot serve a write-heavy delete path without an
   incremental form. Not prototyped.

## Other proposals, not attempted

- **One HEAD per cold table open.** Every open of a pinned version is a HEAD
  (for the manifest size) then a GET. Lance resolves the size because
  `ManifestLocation.size` is unknown; recording the manifest size in the pin
  (a `__manifest` format decision, needs an RFC) or a suffix-range read in
  Lance would remove one request per table per cold open, engine-wide.
- **Named-branch first write** costs 59 requests against 28 on main (T07):
  the branch coordinator open and the named-branch publisher's reopen, which
  the branch-ops run analysed (its H2 and "seed the new branch's
  coordinator"). #1229's single-GET branch ref read will reduce it; not
  repeated here.
- **Edge inserts during history building** grew with the node table's
  fragments: the original `p1024` history (1,024 one-row Knows inserts after
  1,024 one-row Person inserts) exceeded GQT's 128 MiB evidence budget, so I
  reordered it. H3 cut the edge-insert validation reads (`AddFriend` golden
  111 → 60); I did not measure the edge-insert curve itself.

## Wall clock

Not measured: no qualified benchmark environment exists on this box (the
bench harness refuses its machine identity under gVisor). As a model only,
GQT's `s3-like` latency model (17 ms per request plus bytes at 50 MiB/s,
requests overlap freely), same cases and steps:

| case | step | requests | modeled ms |
|---|---|---:|---:|
| p0000 | T02 two-edge delete, warm | 39 → 28 | 658 → 463 |
| p0000 | T10 one-edge delete, cold | 50 → 36 | 800 → 587 |
| p0256 | T02 two-edge delete, warm | 300 → 31 | 821 → 518 |
| p0256 | T10 one-edge delete, cold | 827 → 555 | 1,161 → 840 |
| s100k | T02 two-edge delete, warm | 42 → 28 | 816 → 539 |
| i100k | T02 two-edge delete, warm | 74 → 26 | 1,180 → 374 |
| i100k | T10 one-edge delete, cold | 95 → 60 | 1,291 → 750 |
| t16 | T02 two-edge delete, warm | 124 → 31 | 1,597 → 518 |
| t16 | T10 one-edge delete, cold | 251 → 155 | 2,641 → 1,544 |
| f0256 | F05 one-edge delete, cold | 327 → 49 | 1,192 → 734 |
| g0256 | F05 one-edge delete, cold | 302 → 289 | 893 → 698 |
| p0000 | T07 one-edge delete on a branch, warm | 73 → 59 | 1,201 → 988 |

Modeled latency falls 22–68%, less than the request count where the removed
reads were parallel. A real S3 run is the evidence still missing.

## Gates

All on this box, debug builds, on the final branch head unless noted.

| Gate | Result |
|---|---|
| GQT corpus, complete (`gq_logic_tests`, debug: `merge_lineage`/`change_lineage` = `verify`, `expect same as v1` reference-engine parity) | 436 passed, 0 failed (on H3) |
| Every measurement case asserts its `affected` node and edge counts | 33 cases pass on baseline, H1, H2 (where run) and H3 |
| Canonical workspace graph (`RUST_MIN_STACK=16777216 cargo dev-test --no-fail-fast`) | 5,091 passed, 10 failed, 41 ignored (on H3). All 10 are environmental and outside the change: 8 `omnigraph-bench` tests (gVisor has no `Cpus_allowed_list`; the sandbox refuses symlinks), 1 CLI symlink-refusal test, and `db::upgrade::tests::interrupted::unreadable_legacy_object_asks_for_a_rerun` (root reads a `chmod 000` file; #1230 reports the same on main). No stack overflows at 16 MiB. Engine integration, `writes`, `failpoints`, `detached_commit_matrix`, `forbidden_apis`, `lance_surface_guards` all pass. |
| DST suite (`cargo test` in `crates/omnigraph-dst`: crash, concurrency, faults, golden) | 112 passed, 0 failed, 31 ignored (fleet and hunt instruments) |
| DST storage-cost golden | regenerated once after H3 (commit `28608166`); every changed row drops: `AddFriend` l.get 111 → 60, `DeletePerson` 6 → 5, `RemoveFriendshipsFrom` 20 → 19, `_verify` 1471 → 1463. Attribution is by mechanism (H1 for the delete rows, H3 for `AddFriend`), not by regenerating at each commit. |
| `cargo dev-clippy` (workspace, all targets, `-D warnings -W clippy::dbg_macro`) and the same for `-p omnigraph-gqt` | clean |
| `cargo fmt --all --check` | clean |
| `scripts/check-docs.py`, `scripts/check-agents-md.sh` | OK |
| `typos` | not run: not installed on the box |

CI on a pull request remains the authoritative run; none was opened.

## Sources

Shaped a hypothesis or the proposal (each fetched and read):

- Delta Lake, `DMLWithDeletionVectorsHelper.scala`, delta-io/delta,
  https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/commands/DMLWithDeletionVectorsHelper.scala
  — one scan collects file name and `_metadata.row_index` for matching rows
  and builds one bitmap per file. H1 is the same one-pass capture.
- Lance 13.0.0 `DeleteBuilder` (`lance/src/dataset/write/delete.rs`, crates.io
  source in the local registry) — `execute_impl` scans again for row ids and
  `apply_deletions` extends each fragment's deletion vector. H1 reuses the
  second half.
- Apache Iceberg Table Spec, https://iceberg.apache.org/spec/, and the dev
  thread "[DISCUSS] Deprecate Equality Deletes" (J.-B. Onofré, 2024),
  https://www.mail-archive.com/dev@iceberg.apache.org/msg08114.html — position
  deletes require reading data files to find positions; equality deletes move
  the cost to read time. Frames why H2 (an index lookup) is the lever for a
  key delete in a position-delete format like Lance.
- A. Okolnychyi et al., "Petabyte-Scale Row-Level Operations in Data
  Lakehouses", PVLDB 17(12), 2024,
  https://www.vldb.org/pvldb/vol17/p4159-okolnychyi.pdf — copy-on-write vs
  position/equality deletes, runtime filtering and caching of delete files.
- Apache Hudi indexing docs, https://hudi.apache.org/docs/indexes —
  per-file bloom filters and key-range interval trees to rule out files on
  upsert and delete. Precedent for item 3 of the proposal.
- Kuzu `rel_table.cpp` (`detachDeleteFromRelTable`), kuzudb/kuzu,
  https://github.com/kuzudb/kuzu/blob/master/src/storage/table/rel_table.cpp,
  and X. Feng et al., "KÙZU Graph Database Management System", CIDR 2023,
  https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf — detach delete visits
  every incident rel table with one CSR adjacency read each. Item 4.
- S. Sarkar, T. Papon, D. Staratzis, M. Athanassoulis, "Lethe: A Tunable
  Delete-Aware LSM Engine", SIGMOD 2020,
  https://cs-people.bu.edu/mathan/publications/sigmod20-sarkar.pdf —
  compaction chosen by invalidated entries and tombstone age. Supports a
  reconciler driven by deletion files and uncovered fragments.
- Lance format pages, https://lance.org/format/table/ (deletion files) and
  https://lance.org/format/table/row_id_lineage/ (stable row ids).

Useful, not used this run (start here next time):

- Iceberg v3 position-delete design, apache/iceberg#11122,
  https://github.com/apache/iceberg/issues/11122.
- Delta Lake protocol, deletion vectors,
  https://github.com/delta-io/delta/blob/master/PROTOCOL.md, and design issue
  delta-io/delta#1367.
- DuckLake delete files and deletion vectors,
  https://ducklake.select/docs/stable/specification/tables/ducklake_delete_file.
- S. Héman, M. Zukowski, N. Nes, L. Sidirourgos, P. Boncz, "Positional
  Update Handling in Column Stores", SIGMOD 2010,
  https://ir.cwi.nl/pub/16172/16172D.pdf.
- P. Gupta, A. Mhedhbi, S. Salihoglu, "Columnar Storage and List-based
  Processing for GDBMSs", PVLDB 14, 2021,
  https://www.vldb.org/pvldb/vol14/p2491-gupta.pdf (position-based edge ids
  and deletes).
- Not verified (paywalled or not fetched): "The Lance Lakehouse Format",
  FORMATS 2026; "BACH" (PVLDB 2025) and "LSMGraph" (SIGMOD 2025) on
  updatable CSR; "A+ Indexes" (ICDE 2021); Neo4j store-format docs and
  Memgraph's vertex edge lists (both fetched by the research pass, not
  needed here).

## What could not be verified

- Wall clock on a qualified environment, real S3, and the served (HTTP)
  path. The served handlers call the same `mutate` entry point measured
  here; nothing counts storage under an HTTP request, so the served counts
  are a code-path argument, not a measurement.
- Cold rows at the scale of a production graph: the largest cases are 100k
  rows in one fragment and 1,024 per-commit fragments.
- 1M rows: not run; the 100k cases already show bytes linear in rows, and a
  1M-row seed JSONL (~130 MB) exceeds what the case format handles
  comfortably on this box.
- The `f1024`/`g1024` and `p1024` cases ran once on the baseline and once on
  the final head; H1 and H2 were measured on a subset (table above).
- H2's correctness rests on `__id = derive(key)` for every row of a keyed
  type, the contract the read planner already relies on; I did not build a
  graph from every historical writer to test it.
- Tables forked before v11 (inherited `base_id` files) with deletes: not
  constructed.

## Run record (for the orchestrator)

No graph write access in this box. A `WorkflowRun` for this attempt:

- execution_key: `autoresearch/node-delete @ de776c14473d91130bb55529392102fdce5b5c67 → autoresearch/node-delete-20261102`
- status: succeeded (branch pushed, report written; no pull request opened)
- RunTriggeredBy: the orchestrator's `prompt-box` run (as observed: a
  prompt naming this workflow and its scope change); not verified further
- started 2026-11-02 before 21:50 UTC (box clock), stopped by the orchestrator,
  resumed 21:50 UTC with the wider scope, finished the same night (UTC)
- outcomes: H1 kept (`d1790636`), H2 kept (`0708062d`), H3 kept
  (`e4f40bca`), H4 dropped before building, H5 not pursued; golden
  regenerated (`28608166`); docs (`9912161b`); scripts (`914cc8b8`)
- targeted totals: 51,939 → 29,891 requests over 319 delete steps in 33
  cases

## Workflow feedback

- The scope change mid-run (history only → every dimension) was the right
  call: data size, indexes and incident edge types each showed a different
  mechanism. Naming those dimensions in the brief from the start would have
  saved a restart and a wiped `/tmp` (only `/src` and `/persist` survive a
  box restart; the brief could say to keep work files under `/src`).
- "Warm" on the DST memory store includes Lance caching whole small files.
  The definition could ask runs to report cold or first-touch rows as the
  headline numbers, since warm rows on one-row fragments can look flat for
  reasons that do not carry over to large files.
- GQT's 128 MiB evidence budget bounds a case's total logged requests; a
  history that is quadratic in its own setup (edge inserts validating against
  a growing node table) fails it. Worth a sentence in the definition: build
  history so its setup is linear, or put the history in the seed.
- The bench harness cannot run in these boxes (gVisor lacks
  `Cpus_allowed_list`), so wall clock will stay "not measured" until a
  qualified environment is part of the workflow.
- The brief asks to read prior `WorkflowRun`s; none for this workflow is
  visible in the `personal` graph's read view. If the orchestrator records
  them elsewhere, naming the target and graph would let a run read them.
- `typos` is not installed in the box image; CI runs it.
