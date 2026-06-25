# DST coverage ledger (completeness-critic output)

This is the harness's honest self-assessment: which cells of the morphological
matrix it actually samples, which it does not, and — most importantly — which
**dimensions it had not named** until a bug found by other means forced them
into view.

> **Read this first:** "comprehensive" is always *relative to the dimensions
> below*. A 100% mark here means "100% of the distinctions we thought to draw."
> The only way to find an *unnamed* dimension is to check the harness against a
> bug it didn't catch (see the `#296` row). Re-run this critic whenever that
> happens. This ledger is a process artifact, never a proof.

## Why exhaustive sampling is impossible (so this ledger exists instead)

1. **Cells are path-spaces, several unbounded.** A "cell" is reached by a
   *sequence* of ops; sequence count grows exponentially with length, and
   length, history depth, data values, and fragment/version morphology are all
   unbounded. The grid is a projection of an infinite tree.
2. **Concurrency is non-enumerable.** A concurrent bug lives in a *schedule*
   (interleaving + timing), not a cell. You sample schedules or steer toward a
   named hazard; you never cover them.
3. **The model is open-world.** You can only sample dimensions you've named, and
   you can't prove you've named them all (`#296` was a dimension we hadn't).

So the goal is not coverage; it's **maximize bugs-found per unit cost** —
sample with novelty bias, steer to named hazards, and run this critic to surface
new dimensions.

## Dimension ledger

Legend: ✅ sampled · 🟡 partial · ❌ unsampled · ⏸️ deferred-by-plan (PR-C/PR-D)

### D1 — operations
| op | status | where |
|----|--------|-------|
| insert node (Person/Doc) | ✅ | walk, statemachine |
| insert/delete edge (Knows) | ✅ | walk (`InsertKnows`/`DeleteKnows`) |
| update | ✅ | walk, statemachine |
| delete node (cascade) | ✅ | walk (`DeletePerson`) |
| optimize | ✅ | walk |
| repair | ✅ | walk (`Repair`) |
| read | ✅ | walk, readshape |
| branch create/write/merge | 🟡 | `branch_isolation_and_merge` (scenario, not generic walk) |
| **`open` / recovery sweep** | ❌ | **only a fixture step (`reopen`), never a generated op — the `#296` gap** |
| cleanup (version GC) | ❌ | needs `&mut self`; deferred |
| apply-schema mid-sequence | ❌ | forks the single-branch model; deferred |
| overwrite (`LoadMode::Overwrite`) | ❌ | deferred |

### D2 — latent table morphology
| morphology | status | where |
|------------|--------|-------|
| 1 vs ≥2 fragments | ✅ | readshape (`Single`/`MultiFragment`), walk |
| deletion vectors | ✅ | readshape (`WithDeletions`), walk |
| compacted/reindexed | ✅ | readshape (`Optimized`), walk |
| on-branch | 🟡 | readshape on-branch + branch scenario |
| HEAD>manifest drift | 🟡 | produced by RC-1; not deliberately steered |
| overlapping row-id ranges | ✅ (as invariant target) | `no_duplicate_live_row_ids` |

### D3 — read shapes
| shape | status | where |
|-------|--------|-------|
| scan · @key · indexed · non-indexed · range · order+limit · count · numeric-agg · 1-hop · var-hop · negation · zero-match | ✅ | `readshape::shapes()` × 4 morphologies × on-branch |
| **vector (`nearest`)** | ⏸️ | needs vector data |
| **FTS (`bm25`/`search`/`fuzzy`) / `rrf`** | ⏸️ | needs the inverted index whose builder OOB-panics (finding #5) |

### D4 — oracles
| oracle | status |
|--------|--------|
| HEAD==manifest · `Dataset::validate` · row-id-unique · index-probe · count==model · content==model · edges==model (RI) · @key-unique | ✅ |
| branch isolation · merge correctness · reopen==pre_state | ✅ |
| **replay-equality (bit-identical)** | ⏸️ — blocked on PR-C determinism (Lance internal parallelism unseeded) |
| **linearizability (porcupine)** | ⏸️ |

### D5 — context  *(the dimension with the biggest blind spots)*
| context | status | note |
|---------|--------|------|
| embedded backend | ✅ | the only backend |
| **CLI / long-lived server backend** | ⏸️ | PR-D |
| local FS | ✅ | |
| **S3 (RustFS/MinIO)** | ⏸️ | PR-D |
| single writer | ✅ | |
| concurrent writers (one handle) | ✅ | `concurrent_walk_structural_invariants` |
| **concurrent *opens* / ≥2 recovery sweeps** | ❌ | **the `#296` cell — see below** |
| **cross-process writers/opens** | ❌ | documented engine known-gap; harness can't reach it yet |
| cold vs warm coordinator | 🟡 | reopen exercises cold; not steered |

### Hidden dimensions (named only after a miss)
| dimension | how we learned it | status |
|-----------|-------------------|--------|
| **handle/process multiplicity** | `#296` (concurrent recovery sweeps on one sidecar) | ❌ being closed |
| **`open`/recovery as a first-class op** | `#296` (the sweep is a hidden op) | 🟡 — `Reopen` op added to the walk; concurrent/cross-process still ❌ |
| **schedule/interleaving precision** | `#296` needs one exact classify→publish-CAS race | 🟡 — sampled stochastically (multi-thread), not steered; failpoints would steer it |

### ⚠️ Fault-injection seam is narrower than documented (critic finding)
The Phase-2 `FaultAdapter` wraps `StorageAdapter::write_text_if_match` claiming to
fault "the conditional manifest write." It does **not**: the `__manifest` publish
is a Lance `MergeInsertBuilder` row-level CAS on `object_id`
(`db/manifest/publisher.rs:377`), which never flows through the StorageAdapter.
Verified empirically — a write under `cas_conflict_pct = 100` **succeeds and
leaves no sidecar**. So `seeded_op_loop_with_cas_faults` injects into a *cold*
text-CAS path (schema staging / `omnigraph.rs:2380,2441`), not the manifest hot
path, which is why faults≈no-faults in the walk. **Real manifest-CAS / publish
fault injection needs either (a) wrapping Lance's `object_store` at dataset open
(the deferred "Lance-internal-I/O" seam) or (b) the engine's failpoints**
(`recovery.before_roll_forward_publish`, the per-writer Phase-B publisher
failpoints). This is the prerequisite for the `#296` cell — the StorageAdapter
seam cannot induce a `RolledPastExpected` sidecar.

## Prioritized gap-closure backlog
1. **Widen the fault seam (prerequisite for everything below).** The StorageAdapter
   wrapper misses the manifest publish (see the critic finding above). Add a
   `--features failpoints` harness variant using `recovery.before_roll_forward_publish`
   + the per-writer Phase-B publisher failpoints, and/or wrap Lance's `object_store`
   at dataset open. Until this lands, manifest-CAS / sidecar faults are not reachable.
2. **`#296` cell — concurrent `open` under a fault-induced pending sidecar** (needs #1):
   induce a `RolledPastExpected` sidecar (failpoint at Phase-B publish), then spawn N
   concurrent `Omnigraph::open` + a rendezvous so two sweeps race one sidecar; assert
   every open converges (none returns `ExpectedVersionMismatch`). Caught `#296` on
   0.7.1; guards 0.7.2.
3. **`open`/recovery as a generated op** — ✅ DONE (the `Reopen` op): the walk now
   drops + reopens mid-sequence, sampling the recovery sweep across walk states (not
   only at the end). Concurrent/cross-process variants remain (needs #1/#4).
4. **cross-process** writer/open scenarios (subprocess backend) — the documented
   one-winner-CAS territory.
5. **vector/FTS/rrf read shapes** + **determinism/replay-equality** (PR-C) + **CLI/server/S3 backends** (PR-D).

## The standing rule
When a bug is found *outside* this harness, before closing it: add its row to the
Hidden-dimensions table if it names a new dimension, then add a sampling cell.
That is the only mechanism that grows the model. The ledger is never "done."
