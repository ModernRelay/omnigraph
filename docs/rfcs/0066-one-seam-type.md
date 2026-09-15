---
rfc: "0066"
title: "One seam type for test-time behavior substitution"
track: maintainer
status: draft
implementation: in-progress
authors:
  - azimafroozeh
created: 2026-09-14
updated: 2026-09-14
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0066: One seam type for test-time behavior substitution

> A term set in ***bold italics*** is being defined at that exact spot; it is
> used plain everywhere after.

## Summary

Every place where a test may substitute the engine's behavior becomes one
value of one type. A ***seam*** is a named site in the code whose behavior
a test can choose from outside, without editing the site (Feathers, Working
Effectively with Legacy Code, 2004); its ***slot*** is the cell that holds
the chosen behavior, empty in production. This RFC introduces the crate
`omnigraph-seams` with the type `Seam<B>`, where `B` is the trait describing
what a test may put in the slot, and moves the engine's six families of
substitution onto it: the 87 engine and 8 cluster named sites now reached
through `failpoints.rs`, the clock, the ULID source, the write-gate hook, and
the two decorations of the object store. `fail-parallel` leaves three
manifests (engine, cluster, `omnigraph-dst`) and the workspace dependency
table. Every decision seam is listed in a hand-kept catalog that a guard test
holds complete, so the docs, the DST harness and the GQ logic test runner
(GQT) read one list. GQT gains a
`--- seam` block admitted from that catalog, with delivery proof taken from
the test's own installed behavior rather than from the text of a returned
error.

## Motivation

Three different things share the word failpoint today (file and line
references in this section and in Design name the tree before this PR,
`47abb75f`). A site reached
through `maybe_fail` injects a failure (`crates/omnigraph/src/failpoints.rs:86`).
A site reached through `is_enabled` flips a code path and injects nothing;
its own doc comment calls it a "behavior seam" (`failpoints.rs:100-105`). A
site armed through `ScopedFailPoint::with_callback` pauses for a test
rendezvous (`failpoints.rs:409`). Beside them, three hand-written
thread-local modules do the same job for time, identity and lock order
(`dst_clock.rs`, `dst_ids.rs`, `dst_gate.rs`, each with its own
`install_*`/`uninstall_*` pair), and the storage boundary is substituted in
two further ways: `Omnigraph::init_with_storage` takes a wrapped
`StorageAdapter`, and the DST harness interposes a provider in Lance's
object-store registry (`crates/omnigraph-dst/src/lance_faults.rs:397-408`).
Six families of substitution, four install styles, no unified catalog across
these families.

The cost shows in the GQ logic test runner. It admits five hook names by a
hand-kept list (`crates/omnigraph-gqt/src/dst_runner.rs:179-188`) and one
action word, because its only proof that a fault fired is the text
`injected failpoint triggered:` in the step's error
(`dst_runner.rs:154-177`). A site that returns success when it fires, the
skip kind, can never be admitted. The library's callback cannot serve as
proof: `fail_parallel::cfg_callback` replaces a point's action list with a
callback-only task (fail-parallel 0.6.0, `src/lib.rs:826-838`), and a
callback task evaluates to `None` (`src/lib.rs:520-524`), so under it
`maybe_fail` returns Ok and the point never fires. The stale-sidecar
reproduction for [#602](https://github.com/ModernRelay/omnigraph/issues/602)
could not be written as a logic test for exactly this reason.

## User and operational behavior

Production behavior is unchanged: a slot is empty, `with` returns `None`,
the default arm runs. Every read of a slot compiles out under the two
existing feature flags, `failpoints` and `dst`, as the failpoints do today;
the statics themselves stay, empty and unread. Filling a slot needs a seams
crate feature that follows the reader: `install` (the generic slots, enabled
by `dst`) and `decide` (the decision installers and the catalog clear,
enabled by `failpoints` only), so a build that arms a seam no helper reads,
`dst` without `failpoints` included, fails to compile.

For an engine author, declaring a seam is one static and one call:

```rust
pub static MUTATION_POST_SIDECAR_PRE_FORK: DecideSeam = Seam::decide(
    "mutation.post_sidecar_pre_fork",
    Op::Mutation,
    Effect::Fail,
    Global::new(),
);

crate::seams::fail(&MUTATION_POST_SIDECAR_PRE_FORK)?;
```

For a test author, installing one is one guard:

```rust
let _site = MUTATION_POST_SIDECAR_PRE_FORK.fire_once_at(1);
let _clock = CLOCK.install(Arc::new(LogicalClock::default()));
```

The guard empties the slot on drop, so a panicking test cannot leave a seam
armed for the next test in the binary.

For a logic test author, the fault block that names a hook today becomes a
seam block:

```
--- seam
at: mutation.post_sidecar_pre_fork
occurrence: 1
action: fail
scope: next_step
```

`action` is `fail` or `skip`; `hold` is refused until a case can express two
concurrent steps. The action must match the effect the site declares, so a
mismatch is refused before the case runs. A bare `--- seam` without `action`
is refused: a seam is a place, not an action. `--- fault` keeps its name for
storage-boundary faults, specified in a separate amendment to RFC 0045.

A `--- seam` block inherits the placement rules of `--- fault`: directly
before the step it arms, never inside a loop body, at most 16 per case, and
refused before a `--- restart`, since no installed behavior survives a
restart. Several blocks may precede one step when they name distinct seams,
each with its own delivery record; the same seam twice before one step is
refused (the one-per-step rule of the first draft was lifted by the #602 fix,
whose case loses two independent writes on one mutation). `occurrence: N` counts crossings of the site, retries
included: the installed behavior passes the first N-1 crossings, fires on the
Nth, and passes every later one, with N in `1..=1000000`. The expectation
shape follows the action and the seam's declared effect: a `fail` action on a
seam of effect fail keeps its `--- expect error:` row; on a seam of effect
contention the injected error is retryable, the publisher retries it and the
step carries its healthy expectation, the delivery record being the proof; a
`skip` step carries the healthy expectation the skipped path produces (the
complete mapping is RFC 0045 §Seams at an explicit step). The step report
carries `seam_delivered` with the seam name and the crossing index that
fired, which is the proof the seam was reached.

The first case of the skip kind is the one the motivation names,
`crates/omnigraph-gqt/cases/issue_602_stale_sidecar_heals_on_reopen.gqt`; the
excerpt below omits its `--- runner`, `--- schema` and `--- seed` sections:

```
# issue: 602
--- seam
at: mutation.sidecar_confirm_put
occurrence: 1
action: skip
scope: next_step
--- seam
at: mutation.sidecar_post_publish_delete
occurrence: 1
action: skip
scope: next_step
--- mutate
query add() { insert Person { name: "bob" } }
--- expect affected: nodes=1 edges=0
--- restart
--- query
query all() { match { $p: Person } return { $p.name } }
--- expect unordered
{"p.name":"alice"}
{"p.name":"bob"}
```

The skipped confirm and the skipped delete leave a sidecar the mutation never
acknowledges beside its visible commit. Before the #602 heal the reopen at
`--- restart` refused with `kind: Internal`, and the case was first drafted
carrying a `--- known_failure` marker on that step (the refusal embeds the
operation id of the run, so the marker matched its stable head); the heal
landed in the same change as the two seams, so the case asserts the reopen
and the rows instead.

## Design

**The type.** In `crates/omnigraph-seams`:

```rust
pub struct Seam<B: ?Sized + 'static, S: Storage<B> = Global<B>> {
    name: &'static str,
    op: Op,
    effect: Option<Effect>,
    slot: S,
}

impl<B: ?Sized + 'static, S: Storage<B>> Seam<B, S> {
    pub const fn new(name: &'static str, op: Op, slot: S) -> Self;
    pub const fn decide(name: &'static str, op: Op, effect: Effect, slot: S) -> Self;
    pub const fn name(&self) -> &'static str;
    pub const fn op(&self) -> Op;
    pub const fn effect(&self) -> Option<Effect>;
    pub fn with<R>(&self, f: impl FnOnce(&B) -> R) -> Option<R>;
    // with the `install` feature:
    pub fn install(&'static self, b: Arc<B>) -> Installed<B, S>;
}

pub type DecideSeam = Seam<dyn Decide, Global<dyn Decide>>;
pub enum Op { Mutation, BranchMerge, BranchCreate, BranchDelete, AnyWrite, Unreachable }
pub enum Effect { Fail, Skip, Contention, Hold, Custom }

/// Everything a slot may hold; the one hook every behavior shares.
pub trait Behavior: 'static {
    /// Called with the value already removed from the slot, before it is
    /// dropped, so a behavior that parks a caller can release it.
    fn uninstalling(&self) {}
}
```

`install` needs `B: Behavior`. It fills the slot and returns a guard that
remembers its own installation; dropping the guard removes that installation
only if the slot still holds it, then calls `uninstalling` on the removed
value. Installing over a filled slot panics, so two guards never hold one
seam, and a value installed after a `clear()` is never removed by an older
guard. `with` hands the installed behavior to the caller, or `None`. The type
knows nothing about what `B` does; the call site does.

**The two slot scopes.** `Storage<B>` has two implementations, chosen per seam:
`Global` (a `RwLock<Option<Arc<B>>>` in the static, `B: Send + Sync`; the
lock-free `ArcSwapOption` cannot hold an unsized `Arc<dyn Trait>`) for the
`Decide` seams, `STORAGE` and `OBJECT_STORE`; `ThreadLocal` for `CLOCK`, `IDS`
and `GATE`, installed once per actor thread. The choice follows where each
seam is crossed. 55 of the 157 failpoints tests are `multi_thread` and cross
their site on a spawned worker (`crates/omnigraph/tests/failpoints.rs:983-1009`),
and Lance's calls arrive on pool threads (`crates/omnigraph-dst/src/lance_faults.rs:181`
is a `static RwLock` today), so those seams have to be process-global. The
clock, the id source and the gate are one seeded stream and one hook per writer
thread (`crates/omnigraph-dst/src/concurrent.rs:1296-1320`), so they stay
thread-local and keep the install shape they have today. `with` is one read
lock and one `Arc` clone under `Global`, or one TLS borrow released before
`f` runs under `ThreadLocal`; feature-off builds compile the call away.

A thread-local slot cannot be a struct field, so the thread-local seams are
declared by a macro that emits the `thread_local!` beside the static, while the
global seams are plain statics:

```rust
thread_local_seam! {
    pub static CLOCK: dyn Clock = ("clock", Op::Unreachable);
}

pub static MUTATION_POST_SIDECAR_PRE_FORK: DecideSeam = Seam::decide(
    "mutation.post_sidecar_pre_fork",
    Op::Mutation,
    Effect::Fail,
    Global::new(),
);
```

Under `ThreadLocal` the installed value need not be `Send` or `Sync`, so
`GateHook` keeps the `!Send` shape `TurnHook` has today
(`crates/omnigraph/src/dst_gate.rs:19`), and `Installed<B, ThreadLocal<B>>` is
`!Send` for those seams.

**The behavior traits.** One per shape of substitution:

| Trait | Method | Seams using it |
|---|---|---|
| `Decide` | `decide(&self, name: &'static str) -> Decision` (`Fire` or `Pass`) | the 87 engine and 8 cluster named sites |
| `Clock` | `now_ms(&self) -> u64` | `CLOCK` |
| `IdSource` | `next_ulid(&self) -> Ulid` | `IDS` |
| `GateHook` | the existing `TurnHook` contract from `dst_gate.rs` | `GATE` |
| `DecorateStorage` | `wrap(&self, Arc<dyn StorageAdapter>) -> Arc<dyn StorageAdapter>`, once per handle | `STORAGE` |
| `DecorateObjectStore` | `wrap(&self, Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore>`, once per call | `OBJECT_STORE` |

`decide` runs once per crossing, may block and may have side effects; any
counting lives in the installed value's own state, not in the seam.
`Clock::now_ms` is the single tick that `dst_clock.rs:46-92` exposes through two
entry points today, and both of those readers are derived from it;
`IdSource::next_ulid` takes `&self` because the seeded source advances its own
state (`dst_ids.rs:67-76`).

The crate ships the decision behaviors tests reach for: `FireAlways`,
`FireOnceAt(n)` (passes `n - 1` crossings, fires on the `n`th), `Counted`
(the same, with the count readable: the runner's delivery proof), `PanicAt`,
`Observe(f)` (runs `f`, returns `Pass`, which is what most of the 7
`with_callback` uses want: the cluster race rewrite, the harness probe flags,
the thread-filtered counters) and `Hold` (below). `LogicalClock::default()`
(the fixed epoch, one millisecond per read) and `SeededUlids::new(seed)` stay
in the engine's `dst_clock` and `dst_ids` modules beside their traits.

**Site helpers.** `fail`, `contention` and `skip` are thin functions in each
consuming crate over `Seam::with`, not in the seams crate: the injected error is
the consuming crate's own (`OmniError::manifest` in the engine,
`crates/omnigraph/src/failpoints.rs:91`; the cluster's `Diagnostic`,
`crates/omnigraph-cluster/src/failpoints.rs:21-25`), and the text
`injected failpoint triggered: ` has 40 dependents plus RFC 0045:821, so it is
kept byte for byte. `fail(&SEAM)?` returns the injected error on `Fire`, as
`maybe_fail` does today; `contention(&SEAM)?` returns the retryable CAS error,
as `maybe_fail_retryable_contention` does; `skip(&SEAM) -> bool` is
`is_enabled`. Each helper rejects a seam whose declared effect is not its own:
`fail` admits `Fail`, `contention` admits `Contention`, `skip` admits `Skip`.
The check is a `debug_assert` at the crossing, and the source walker reports
the same pairing statically. A site needing another effect declares
`Effect::Custom` and calls `with` directly; `Custom` seams are never admitted
by a logic test.

**`Hold`.** `Hold` records that the site was reached, parks the first
crossing until `release()` is called, the guard drops, or `HOLD_BOUND` (30 s)
elapses, and returns `Pass` on wake; later crossings pass. A wake by the
bound is recorded by `Hold::timed_out`, so a helper can fail loudly instead of
passing late. `Installed::drop` removes the hold from the slot and then
releases any blocked `decide()` through `Behavior::uninstalling`, so a
panicking test cannot wedge a worker. The rendezvous
in `crates/omnigraph/tests/helpers/failpoint.rs:36-60`, which parks the first
arrival and passes later ones, becomes a `Decide` of this shape. The two
`pause` sites take the same behavior: the library's `pause` parked every
crossing until the rule changed, `Hold` parks the first, and both tests'
assertions hold either way.

**The first skip seam.** No reachable skip seam exists in the engine today: the
only `is_enabled` site is `CHANGE_FEED_SKIP_ETAG_WITNESS`
(`crates/omnigraph/src/db/table_store.rs:1168`), whose op no GQ step reaches.
This PR adds the first one, `mutation.sidecar_confirm_put` (op `Mutation`,
effect `Skip`): read once, through `skip` inside `confirm_occ_sidecar_v9`
(`crates/omnigraph/src/db/manifest/recovery.rs`), to skip the confirm put
while the in-memory sidecar still confirms. Its sibling
`mutation.sidecar_post_publish_delete` skips the post-commit delete inside
`delete_sidecar_after_publish`, the one helper both `exec/mutation.rs` and
`loader/mod.rs` call. The two are independent: a lost confirm alone is
repaired by the delete, a lost delete alone leaves a confirmed residual the
existing roll-forward finalizes, and the pair leaves the stale sidecar that
[#602](https://github.com/ModernRelay/omnigraph/issues/602) reports, each
shape its own logic-test case.

**The two storage seams.** `STORAGE` is consulted at the one point where an
adapter enters a handle, `open_with_storage_and_mode`
(`crates/omnigraph/src/db/omnigraph.rs:681`) and `init_with_storage_for_vintage`
(`:393`), so init, open and reopen all wrap and all handles of a universe share
one decorated instance. It is declared in the engine, not in the seams crate, as
`Seam<dyn DecorateStorage, Global>` with `DecorateStorage::wrap(&self,
Arc<dyn StorageAdapter>) -> Arc<dyn StorageAdapter>`: the trait here is the
engine's `StorageAdapter` (`crates/omnigraph/src/storage.rs:18`, which returns
`OmniError`), not `omnigraph_storage::StorageAdapter`
(`crates/omnigraph-storage/src/lib.rs:1019`, which returns `StorageError`).
`wrap` runs once per handle and a decorator that shares state returns its own
`Arc`: the DST harness builds its `FailingStorage`
(`crates/omnigraph-dst/src/harness.rs:2205`) before installing, keeps the
`Arc<FailingStorage>` in `RustResources`, and its decorator's `wrap` hands
back a clone of that `Arc` for every handle, which is how `suspend` and
`resume` (`harness.rs:5235`) and `damage_events` (`:5424`) keep reaching the
wrapped object.

`OBJECT_STORE` is a per-call hook, not a decoration applied at construction.
Lance's registry caches stores under weak references
(`providers.rs:111-121, 284-323`) and `STORE_REGISTRY` is a permanent
`LazyLock`, so a store built before an install would never be wrapped and a
wrapped store would outlive its guard. The registry exposes no provider
enumeration either, so the schemes are named, exactly as the harness's
`install` did: at `STORE_REGISTRY` construction
(`crates/omnigraph/src/lance_access.rs:12-13`) the engine registers, for the
`shared-memory` and `file` schemes, stores that forward every call through
`OBJECT_STORE.with(..)`, read per call and never captured. `OBJECT_STORE` is
`Seam<dyn DecorateObjectStore, Global>`; its `wrap` runs on every call over
the base store, so installing it evicts nothing. On `file` roots Lance reads
and writes data files through direct local paths that bypass the wrapped
store, so that lane sees manifest, commit-list and listing traffic only, as
the harness's own `file` wrap did. This is the twenty lines of Lance glue now in
`lance_faults.rs:437-462` moved into the engine and made generic over the
installed value, keeping the per-call read of process state that
`lance_faults.rs:133, 294, 490` does today. The seams crate itself has no
dependencies.

**The catalog.** Each crate that declares seams keeps a hand-listed
`catalog.rs` whose `ALL` array a guard test holds complete: membership both
ways, unique names, one reference per static. A generator or `inventory`
would be a second source of truth (and `inventory` is not in `Cargo.lock`;
only `ctor` is). The erased row is
`SeamEntry { name, op, effect() }`. The engine's catalog replaces
`failpoints::names`; the cluster keeps its own, and the logic test runner reads
the engine's catalog only, since no cluster seam is reachable from a GQ step.
`catalog::decide(name: &str) -> Option<&'static Seam<dyn Decide, Global>>` is
the one string-keyed entry point: the DST harness carries seam names as runtime
data (`crates/omnigraph-dst/src/harness.rs:3631, 3698, 4158, 4281, 4423, 5224`)
and the runner arms by a name read from the case file, so both resolve through
it.

`op` is a closed enum, not a string. The runner maps it to a GQT step kind by a
table (`Query`, `Mutate`, `Control{Create,Delete,Merge}`, `List`, `Restart`,
`crates/omnigraph-gqt/src/lib.rs:2291`), which is why `Mutation` and
`BranchMerge` are separate values and why a site no step kind reaches is
`Unreachable`: those are listed by `scripts/seam_corpus.py` and never
admitted by a case. Read-path sites (`read.*`, `classify.*`) are `Unreachable`
too until a read step kind exists.

The catalog is held honest by the existing source walker.
`crates/omnigraph/tests/failpoint_names_guard.rs:19-24`
(`docs/dev/testing.md:43`) is the catalog guard today and is rewritten for the
new call prefixes, so that every `fail(&`, `skip(&`, `contention(&`,
`park_first(` and `catalog::decide(` argument is a catalog static rather than
a literal, every catalog static has at least one reference, and every helper
call is paired with the effect its static declares. A literal cannot reach a
helper otherwise: the helpers take `&'static DecideSeam`, so the one string
path is `catalog::decide`. The scan matches those prefixes only: a bare `with`
cannot be told from `LocalKey::with` (`dst_gate.rs:31`) by a source walker.

**The test guard.** Four methods on `DecideSeam` replace
`ScopedFailPoint::new(name, rule)`, one per rule shape present in the tree:
`SEAM.fire_always()` (today's `return`, 136 uses, firing on every crossing),
`SEAM.fire_once_at(n)` (`1*return` and `N*off->1*return`, 8 uses),
`SEAM.panic_at()` (4 uses) and `SEAM.hold()` (`pause`, 2 uses);
`SEAM.observe(f)` takes the 7 `with_callback` uses and
`SEAM.count_and_fire_at(n)` is the runner's proof. There is no string parser:
the rewrite is one call per use, and `fire_always` keeps the unbounded
semantics that the 136 uses depend on when their site is crossed more than
once.

**GQT.** The `--- seam` block is admitted when `at` names a `Decide` seam in the
engine catalog, `action` matches the seam's declared effect, and the step after
the block is of the kind the seam's `op` maps to. The runner installs a `Decide`
that counts crossings, fires on the declared occurrence and records the hit;
that record, reported as `seam_delivered`, is the delivery proof, so a seam
whose effect returns success is provable without any error text.

Coverage is a listing, not a generated corpus. `scripts/seam_corpus.py` reads
the catalog and every `--- seam` in the corpus and prints one row per seam:
its operation, its effect, and the cases that arm it; `--check` refuses a
case naming a seam the catalog lacks or an action its effect does not admit.
A proof case is written by hand, one per seam, because a step kind alone is
not enough to cross a site (`MUTATION_POST_SIDECAR_PRE_FORK` fires only with
a deferred fork, `crates/omnigraph/src/exec/staging.rs:1320-1324`, so a
case generated from the step kind alone fails `seam_unobserved`). Reachable
seams without a case and seams whose operation no step starts are the two
debts the listing reports; at this RFC's implementation the catalog holds 88
seams, 43 reachable, 6 armed by a case, 45 unreachable (the listing is the
current number).

**Known failures at a restart.** A `--- known_failure` marker may name a
`--- restart` step, and its `match` gains a `reason_prefix` matcher beside
today's exact `reason`. The stale-sidecar case needs both: the reopen refusal
carries `kind: Internal` and embeds the operation id of that run, so no exact
string can match it across runs. `reason_prefix` matches the stable head of the
message and ignores what follows.

## Invariants

- No hard invariant in `docs/dev/invariants.md` is affected and no deny-list
  item applies: every slot is test-only, feature-gated, and never persisted.
- A slot is empty unless a live `Installed` guard for it exists.
- Installing the same seam twice on the same scope is refused, not stacked.
- The seeded ULID and logical-clock streams produce the same values for the
  same seed before and after the move: the same generated ids, the same
  stamps, in the same order, so every pinned DST scenario replays
  byte-identically.
- Every decision seam is in its crate's catalog; every catalog entry has a
  site. The five non-decision seams (`CLOCK`, `IDS`, `GATE`, `STORAGE`,
  `OBJECT_STORE`) are statics the harness names directly.

## Compatibility and reversibility

Behavior: none for production.

Feature flags: two gate the seams today, `failpoints` for the `Decide` seams
and `dst` for the clock, the id source, the gate, `init_with_storage` and the
registry re-export (`crates/omnigraph/Cargo.toml:16,29`). This RFC keeps both
names and both scopes; `omnigraph-seams` carries one feature, `install`,
which both engine flags enable, so `omnigraph-dst` keeps enabling both and
the shipped-shape check in `.github/workflows/dst.yml:71-76` passes unchanged.

Public API: `failpoints::{registry, set_registry, ScopedFailPoint}` go away
with the library, and the clock, id and gate install/uninstall pairs are
replaced by seam statics; `init_with_storage` and `open_read_only_with_storage`
stay and route through the `STORAGE` seam. All callers are in-repo, the DST
harness and the failpoints tests. `FailScenario` is kept: it owns `SCENARIO_GATE`
(`crates/omnigraph/src/failpoints.rs:47-84`), which is the serialization the
global `Decide` slots still need, and its teardown becomes a clear-all over the
`Decide` catalog. `open_with_storage` stays, as the deliberate public exception
it already is, and `dst_lance_store_registry` stays for the census listing
(`crates/omnigraph-dst/src/lance_faults.rs:374-393`): it is a read route, not a
decoration.

`fail-parallel` leaves three manifests (engine, cluster, and `omnigraph-dst`,
where it is an unused optional dependency) and the workspace dependency table.

Reversal: the crate is deleted and the six families go back to their current
modules; nothing durable is written by any of this.

## Alternatives

- **Do nothing.** The runner's hand-kept list of five names stays; a sixth
  hook is admitted by editing it; the skip kind is never admissible, so any
  effect that returns success on firing stays outside logic tests.
- **Fire listener inside `maybe_fail`/`is_enabled`.** Two lines, and the proof
  works for fail and skip alike, so this competitor does answer the motivating
  case. What it leaves unchecked is the action word and the step kind, because
  no site declares its effect or its op; that check is exactly
  `Seam::decide(name, op, effect, slot)`, and it is what the effect/helper
  pairing and the coverage listing are built on.
- **Instance-owned builder injection (RFC 0037 P1-5).** Every seam a
  constructor argument; no flag, no slot. Ruled out: 87 sites reached from free
  functions and Lance's process-wide registry have no instance to hold the
  value; the per-seam scope decision is the compatible half, and this PR amends
  the P1-5 row to record that.
- **Typed catalog with a `kind` enum, `fail-parallel` kept underneath.**
  One catalog, four install paths (sites, clock, ids, gate) and the library
  doing counting only. Superseded by the one-type design, which removes the
  four paths and the dependency.
- **Patch `fail-parallel` so a callback can chain into a return.** A forked
  dependency for one feature, and the four `dst_*` modules stay outside it.
- **Precedent audit.** The three `dst_*` modules are the in-repo pattern for
  a slot (thread-local `Option`, install/uninstall pair) and this RFC keeps
  their shape and their entry-point signatures; it differs from them only by
  writing the slot once as `Storage<B>` instead of three times.
  `tests/failpoint_names_guard.rs` is the in-repo pattern for a source-walk
  catalog guard, and it is extended rather than replaced.

## Evidence and tests

Owners extended (`docs/dev/testing.md:23,43,60`): `crates/omnigraph/tests/failpoints.rs`,
`crates/omnigraph/tests/failpoint_names_guard.rs`,
`crates/omnigraph-cluster/tests/failpoints.rs`,
`crates/omnigraph-dst/tests/scenarios.rs`, and the GQT corpus.

- All three builds green and clippy-clean: the `failpoints` graph
  (`--features omnigraph-engine/failpoints,omnigraph-cluster/failpoints`), the
  `dst` graph (`-p omnigraph-dst`), and the default build; plus the per-package
  shipped-shape build that `.github/workflows/dst.yml` runs.
- Every pinned DST scenario strict-replays unchanged after the move
  (`cargo test -p omnigraph-dst --test scenarios`).
- The source-walk test: every site names a catalog static, every catalog static
  has a site, and every helper call matches its seam's declared effect.
- The 150 `ScopedFailPoint::new` uses and 7 `with_callback` uses across 11 test
  files rewritten to the `DecideSeam` methods; the `FailScenario::setup`
  calls kept and counted; the 46 engine integration binaries green, 2 of them
  feature-gated.
- No test installs a live seam twice: `Seam::install` panics on a filled
  slot, so a re-arm is drop-then-install or a red test.
- GQT: the 7 corpus cases with a hook in `--- fault` rewritten to `--- seam`,
  reports equivalent up to the renamed delivery record (`fault_delivered
  {hook}` became `seam_delivered {at, occurrence, crossings}`);
  `scripts/seam_corpus.py --check` clean over the corpus;
  `issue_602_stale_sidecar_heals_on_reopen.gqt` green with the #602 heal in the
  same change (first drafted as `…_bricks_reopen.gqt`, red with a
  `known_failure` marker, before the heal joined this PR).

The `--- seam` admission rule and the source walker are rules applied to
people, so both tables below are part of the evidence. The walker's own
fixture test covers duplicate listings, the reference boundary and helper
pairing; the GQT refusal corpus covers admission.

| Way to satisfy the rule without doing the work | What stops it |
|---|---|
| Renamed import (`use crate::seams::fail as f;`) | outside the walker's grammar; the helpers take `&'static DecideSeam`, so a renamed helper still cannot take a literal, and each helper asserts the declared effect at every crossing (`assert_eq!`, release builds included) |
| A wrapper function that calls a helper | a wrapper's parameter is typed `&'static DecideSeam`, so it can only carry a catalog static; the walker pairs the call where the static is named (a full path or an imported name), the crossing assert covers the rest |
| A macro that expands to a helper call | a literal macro body is scanned like any other site; a macro that builds the call from a token argument is outside the grammar and left to the crossing assert |
| Two statics sharing one `name` string | the catalog test in `failpoint_names_guard.rs` asserts unique names, and `catalog::decide` would otherwise answer the first |
| Wrong effect/helper pairing (`skip(&A_FAIL_SEAM)`) | the helper rejects a seam whose declared effect is not its own, and the walker reports the pairing |
| A site that calls `with` directly and declares no effect | the seam must declare `Effect::Custom`, which `--- seam` never admits |
| A catalog entry with no site | the catalog test fails on a static that no source or test file names |
| A `--- seam` naming a seam the case's step kind cannot cross | admission compares the seam's `op` to the next step's kind and refuses before the case runs |

| Route the user docs give an honest author | Does the rule accept it |
|---|---|
| Declare a static, call the matching helper at the site, add it to `ALL`, write its proof case | yes; the listing shows the seam with its case |
| Declare a static for a site no step can reach yet | yes; the listing reports it as unreachable, and no case may arm it |
| Call `Seam::with` directly for an effect the three helpers do not cover | yes, with `Effect::Custom`; the seam is armed from a Rust test, never from a case |
| Arm an existing seam from a Rust test | yes, through the `DecideSeam` methods |
| Arm a seam from a logic test with `--- seam` | yes when the action matches the declared effect and the next step's kind matches the `op` |

## Rollout

One PR, carrying:

- the `omnigraph-seams` crate with the two slot scopes;
- the engine and cluster catalogs and the mechanical rewrite of their sites;
- the three `dst_*` moves (clock, id source, gate) and the two storage seams;
- `fail-parallel` removed from the three manifests and the workspace table;
- the `--- seam` block in GQT with catalog-driven admission, the runner's
  `Decide`, the corpus rewrite and the coverage listing;
- the two skip seams `mutation.sidecar_confirm_put` and
  `mutation.sidecar_post_publish_delete`, the `reason_prefix` matcher and
  `--- restart` support in `known_failure`, several `--- seam` blocks before
  one step, and the case `issue_602_stale_sidecar_heals_on_reopen.gqt`, which
  lands green because the #602 heal ships in the same change (its first draft
  carried a `known_failure` marker on the restart).

The harness holds the guards for the run: `InstalledEnvironment`
(`crates/omnigraph-dst/src/environment.rs:66-76`) holds `CLOCK` and `IDS`, each
writer thread holds its own `GATE` guard for the duration of its `block_on`,
and `RustResources` holds the `STORAGE` guard; the `OBJECT_STORE` guard lives
in a process-wide `OnceLock` inside `lance_faults::install`, so it never
drops. `Scenario::run` clears `CLOCK` and `IDS` before its census bottom
listings, which run on the real clock and id source, while the environment's
guards stay live; the later guard drop clears an empty slot.

A new workspace crate also carries the map updates `AGENTS.md:177-180` asks
for: the workspace member list (`Cargo.toml:35-40`), the crate table in
`docs/dev/testing.md:23`, and the RFC README registry row plus the
next-available bump.

The PR amends two RFCs, RFC 0045 (a draft) and RFC 0037 (accepted), each by
naming the sentences it replaces, as `docs/rfcs/README.md:119-122` requires
for an accepted RFC and as the draft's own decision log expects. RFC 0045
§Faults: the hook form of
`--- fault` (the `action: return_error` sentence and the five-hook paragraph,
`0045:806-846` and `:1128-1132`) is replaced by `--- seam`, and the decision
log of RFC 0045 names each replaced sentence. RFC 0037 §Deferred: the P1-5 row
(`0037:603-608`, `:809`) records that the per-seam scope half of instance-owned
injection is taken here and the no-flag half is not.

A later, optional phase merges the two storage decorations into one decorator
behind the scheme; it re-pins scenarios and is its own decision.

## Unresolved questions

- Whether the later phase merges `STORAGE` and `OBJECT_STORE` into one
  scheme-dispatched decorator, or the two stay separate.
- What unlocks `hold` in a logic test: it is refused until a case can express
  two concurrent steps.
- Whether any cluster seam ever becomes reachable from a GQ step, which is the
  only reason the runner would read a second catalog.

## Decision log

- No maintainer decision recorded yet.
