---
rfc: "0066"
title: "One seam type for test-time behavior substitution"
track: maintainer
status: draft
implementation: in-progress
authors:
  - azimafroozeh
created: 2026-09-14
updated: 2026-09-15
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
error. A decision seam declares the set of ***effects*** its site honors,
one for a site between two steps, several for a site that wraps one
operation, and a case names which one fires, so a new action at a known
site is a case, not an engine edit (amendment of 2026-09-15, decision log).

A seam whose behavior is the store's, not the engine's, is written the same
way: a ***store effect*** is an outcome a seam declares in a second set
beside its effects, which the site passes through unchanged and the storage
decoration acts on at the next matching store call, and a ***store place***
is a seam whose site is a store call itself, listed in a table beside the
decoration rather than in the engine catalog. A case selects one call of a
store place through the `subject` field (defined below), a pattern over the object's name
relative to the graph root; which blocks require it and which refuse it is
RFC 0045 §Seams at an explicit step. The first store effect is `misdirect`,
the first case the foreign-named sidecar of issue 601 (amendment of
2026-09-15, decision log).

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

For an engine author, declaring a seam is one `decide_seam!` beside the code
it guards and one call. A site between two steps declares one effect and
calls the helper of that effect:

```rust
use crate::seams::{decide_seam, fail};

decide_seam! {
    /// After the sidecar is armed, before the deferred fork.
    pub static MUTATION_POST_SIDECAR_PRE_FORK = ("mutation.post_sidecar_pre_fork", Mutation, [Fail]);
}

fail(&MUTATION_POST_SIDECAR_PRE_FORK)?;
```

A site that wraps one operation declares every outcome the code after it
survives and runs the operation under `guarded`, which returns the
operation's result, `None` when the decider skipped it, or the injected
error:

```rust
decide_seam! {
    pub static MUTATION_SIDECAR_CONFIRM_PUT = ("mutation.sidecar_confirm_put", Mutation, [Fail, Skip]);
}

guarded(&MUTATION_SIDECAR_CONFIRM_PUT, storage.write_text(&uri, &json)).await?;
```

The macro expands to a `pub static` of type `DecideSeam` built by
`Seam::decide`, so the plain static is still the contract; the macro only
drops the paths, and it is the one declaration grammar: the source walker
refuses a `Seam::decide` written out by hand. The crate's catalog indexes every seam from one list:

```rust
omnigraph_seams::catalog! {
    crate::exec::staging::MUTATION_POST_SIDECAR_PRE_FORK,
    crate::db::manifest::recovery::MUTATION_SIDECAR_CONFIRM_PUT,
    // …
}
```

which re-exports each under `catalog::IDENT` and lists it in `ALL`.

For a test author, installing one is one guard; a seam with several effects
is installed with the effect named:

```rust
let _site = MUTATION_POST_SIDECAR_PRE_FORK.fire_once_at(1);
let _put = MUTATION_SIDECAR_CONFIRM_PUT.skip_once_at(1);
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

`action` is `fail`, `contention`, `skip`, or a store action, the first being
`misdirect`; `hold` is refused until a case can express two concurrent
steps. `fail` selects `Fail` when declared,
otherwise `Contention` for compatibility with existing cases. Explicit
`contention` selects only `Contention`, and `skip` selects only `Skip`.
A seam declaring both failure effects therefore lets a case choose either,
regardless of declaration order. An undeclared effect is refused before the
case runs. A bare `--- seam` without `action` is refused: a seam is a
place, not an action. A store fault is a seam like any other and has no
block of its own: `--- fault` names nothing and is refused with a pointer
to `--- seam`. Two shapes reach the store. A decision seam in engine code
may declare a store effect, the first being `misdirect`: the site passes,
the write it guards proceeds, and the storage decoration installed at
`STORAGE` lands that one call under a different object name, the same
directory with the filename prefixed `dstm-`, and answers success, so the
engine holds a handle naming an object the store does not have. A store
place, `storage.put`, `storage.delete`, `storage.rename`, `storage.cas`,
`storage.get` or `storage.list`, is the store call itself as a seam;
because one place serves every object, a case names the object with
`subject`, a glob whose grammar is §Design **Subjects**, and `occurrence`
counts the calls of that place whose requested name matches. Which blocks
require `subject` and which refuse it is RFC 0045 §Seams at an explicit
step. A store action on a decision seam that does not declare it is
refused before the case runs, and so is an engine action (`fail`,
`contention`, `skip`, `hold`) on a store place: the store's spelling of an
injected error is `error`. `error`, `lose`, `corrupt` and `delay` parse as
store actions and are refused at admission naming the table row until a row
admits them; `misdirect` is the one admitted spelling.

A `--- seam` block keeps the placement rules of the retired `--- fault`
block: directly before the step it arms, never inside a loop body, at most
16 per case, and refused before a `--- restart`, since no installed behavior
survives a
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
carries `seam_delivered` with the seam name, the crossing index that fired
and the effect it fired, which is the proof the seam was reached.

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

The same site honors `fail`, and the second case at it is written without
touching the engine (`mutation_sidecar_confirm_put_failure_rolls_back.gqt`):
the put returns the injected error, the sidecar stays `Armed` beside table
versions nobody published, and the reopen rolls the intent back, so only
`alice` survives:

```
--- seam
at: mutation.sidecar_confirm_put
occurrence: 1
action: fail
scope: next_step
--- mutate
query add() { insert Person { name: "bob" } }
--- expect error: injected failpoint triggered: mutation.sidecar_confirm_put
--- restart
--- query
query all() { match { $p: Person } return { $p.name } }
--- expect unordered
{"p.name":"alice"}
```

The first store-effect case is
`issue_601_foreign_named_sidecar_blocks_branch.gqt`, carried as a known
failure until the [#601](https://github.com/ModernRelay/omnigraph/issues/601)
fix deletes its marker; the excerpt omits `--- runner`, `--- known_failure`,
`--- schema` and `--- seed`:

```
# issue: 601
--- seam
at: recovery.sidecar_write
occurrence: 1
action: misdirect
scope: next_step
--- mutate
query add_row() { insert Person { name: "bob" } }
--- expect affected: nodes=1 edges=0
--- restart
--- mutate
query add_row_after_reopen() { insert Person { name: "carol" } }
--- expect affected: nodes=1 edges=0
```

The arm put behind `recovery.sidecar_write` is the one store call between
the seam and the handle (`crates/omnigraph/src/db/manifest/recovery.rs:1146`
and `:1154`), so the misdirected object is `__recovery/dstm-<opid>.json`
with a valid Armed body; the confirm put and the post-publish delete
address the canonical name. Before the
[#601](https://github.com/ModernRelay/omnigraph/issues/601) fix the reopen
at `--- restart` listed the directory, read the foreign file, classified the
intent as the healer decided and deleted the canonical name, which did not
exist, so the file stayed and the write at the third step was refused with
`RecoveryRequired` (the marker's reason was copied from that red report);
the fix heals a sidecar at the uri it was listed from, so both forms of the
case pass without a marker. The same object placed
by the store place form reads `at: storage.put`, `subject: __recovery/*`,
`occurrence: 1`, `action: misdirect`; both forms produce one bucket state
and one delivery record.

## Design

**The type.** In `crates/omnigraph-seams`:

```rust
pub struct Seam<B: ?Sized + 'static, S: Storage<B> = Global<B>> {
    name: &'static str,
    op: Op,
    effects: &'static [Effect],
    store: &'static [StoreEffect],
    slot: S,
}

impl<B: ?Sized + 'static, S: Storage<B>> Seam<B, S> {
    pub const fn new(name: &'static str, op: Op, slot: S) -> Self;
    pub const fn decide(name: &'static str, op: Op, effects: &'static [Effect], slot: S) -> Self;
    pub const fn decide_with_store(name: &'static str, op: Op, effects: &'static [Effect], store: &'static [StoreEffect], subject: &'static str, slot: S) -> Self;
    pub const fn name(&self) -> &'static str;
    pub const fn op(&self) -> Op;
    pub const fn effects(&self) -> &'static [Effect];
    pub const fn store_effects(&self) -> &'static [StoreEffect];
    pub const fn store_subject(&self) -> Option<&'static str>;
    pub fn with<R>(&self, f: impl FnOnce(&B) -> R) -> Option<R>;
    // with the `install` feature:
    pub fn install(&'static self, b: Arc<B>) -> Installed<B, S>;
}

pub type DecideSeam = Seam<dyn Decide, Global<dyn Decide>>;
pub enum Op { Mutation, BranchMerge, BranchCreate, BranchDelete, AnyWrite, Unreachable }
pub enum Effect { Fail, Skip, Contention }
pub enum StoreEffect { Misdirect }
pub enum Decision { Fire(Effect), Store(StoreEffect), Pass }

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
knows nothing about what `B` does; the call site does. `effects` is empty
for every non-decision seam and non-empty for a decision seam: the
outcomes its site honors, one per site between two steps, several for a
site that wraps one operation. A decision fires with one of them, and
`crossed()` refuses a decider firing an effect outside the set, since the
site has no arm for it.

**Store effects.** A catalog entry may declare a second set beside its
effects, its ***store effects***: `decide_seam!` gains
`store [Misdirect], subject "__recovery/*"`,
`Seam::decide_with_store` holds the set and `store_effects()` answers it,
`effects()` still answers `[Fail]` for `recovery.sidecar_write`, and
`crossed()` accepts a decider firing an outcome from either set, checking
membership per set. A decider fires a store effect as
`Decision::Store(Misdirect)`, never as `Fire`: `Fire` carries an `Effect`,
and the two enums do not convert. A site has no arm for a store effect:
each site helper (`fail`, `skip`, `contention`, `guarded`) gains one arm
that treats `Store(_)` as `Pass`, and the decider that fired it hands the
storage decoration a one-shot instruction for the next `write_text`,
`write_bytes` or `write_text_if_absent`, consumed by the next such call the
decoration sees (the GQT target runs one operation at a time, and the rule
list is empty under the nightly). Nothing else moves: the engine-site
calls, the exact-set checks over `effects()` in the helpers and the source
walker, the plain installers `fire_always` and `fire_once_at`, and the DST
nightly's arming of `recovery.sidecar_write` are untouched. A site declares
a store effect only when exactly one store call of the matching kind
follows it before the next seam, so "the next call" is unambiguous; the
source walk (§Evidence) does not check this. The proof case's report does:
the test in `crates/omnigraph-gqt/tests/runner_dispatch.rs` that runs the
case asserts its delivery record's `hit` (§GQT) names `write_text`, a
`requested` name under `__recovery/`, and a `stored` name equal to
`requested` with `dstm-` prefixed on the file name in the same directory.
The doc comment of the declaring site names that call. The declaration also
names the call's subject, and the decoration consumes the one-shot only on a
put that matches it, so a put of another object between the site and its call
is passed through, not misdirected.

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
    &[Effect::Fail],
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
| `Decide` | `decide(&self, name: &'static str) -> Decision` (`Fire(Effect)`, `Store(StoreEffect)` or `Pass`) | the 89 engine and 8 cluster named sites |
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

The crate ships the decision behaviors tests reach for: `FireAlways(effect)`,
`FireOnceAt(n, effect)` (passes `n - 1` crossings, fires `effect` on the
`n`th), `Counted` (the same, with the count and the effect readable: the
runner's delivery proof), `PanicAt`, `Observe(f)` (runs `f`, returns `Pass`,
which is what most of the 7 `with_callback` uses want: the cluster race
rewrite, the harness probe flags, the thread-filtered counters) and `Hold`
(below). The installers on `DecideSeam` come in two spellings:
`fire_always()`, `fire_once_at(n)` and `count_and_fire_at(n)` fire the one
effect of a single-effect seam and refuse a seam declaring several;
`fire_always_with(effect)`, `fire_once_at_with(n, effect)` and
`count_and_fire_at_with(n, effect)` name the effect and refuse one the seam
does not declare; `fail_once_at(n)`, `skip_once_at(n)`,
`contention_once_at(n)` and the three `_always` siblings are the same,
spelled by effect. `LogicalClock::default()` (the fixed epoch, one millisecond
per read) and `SeededUlids::new(seed)` stay in the engine's `dst_clock` and
`dst_ids` modules beside their traits.

**Site helpers.** `fail`, `contention`, `skip` and `guarded` are thin
functions in each consuming crate over `Seam::with`, not in the seams crate:
the injected error is the consuming crate's own (`OmniError::manifest` in the
engine, `crates/omnigraph/src/seams.rs`; the cluster's `Diagnostic`,
`crates/omnigraph-cluster/src/seams.rs`), and the text
`injected failpoint triggered: ` has 40 dependents plus RFC 0045:821, so it is
kept byte for byte. The first three serve a site between two steps, which
declares exactly one effect: `fail(&SEAM)?` returns the injected error on a
fired decision, as `maybe_fail` did; `contention(&SEAM)?` returns the
retryable CAS error, as `maybe_fail_retryable_contention` did;
`skip(&SEAM) -> bool` is `is_enabled`. Each rejects a seam whose declared set
is not exactly its own effect (`assert_eq!` at the crossing, and the source
walker reports the same pairing statically).

`guarded(&SEAM, op).await -> Result<Option<T>>` serves a site that wraps one
operation and declares every outcome the code after the call survives. It
holds the only match over the decision: `Pass` runs `op` and returns
`Some(op's result)`, and so does `Store(_)`, whose effect the storage
decoration applies, not the site; `Fire(Skip)` returns `Ok(None)` without
running `op`;
`Fire(Fail)` and `Fire(Contention)` return the injected errors. An action
thus has one meaning at every site (`skip` = the wrapped operation did not
run, `fail` = it was replaced by the injected error), the site never
interprets an action itself, and a second action at a known site is a case,
not an edit. What stays a Rust edit is a new place: one edit per location.
The set is held honest by review, not by a type: a site lists `Skip` only
when the code after the call treats `None` as a real outcome
(`confirm_occ_sidecar_v9` updates the in-memory sidecar after a lost put;
the post-commit delete remains independent). The three engine effects are
the site vocabulary; a store effect is passed through at the site and acted
on by the decoration (**Store effects**, **Store places**).

**`Hold`.** `Hold` is a decider, not an effect: it is installable on any
decision seam, records that the site was reached, parks the first
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

**The first skip seam, now the first multi-effect seam.** No reachable skip
seam existed in the engine before this RFC: the only `is_enabled` site was
`CHANGE_FEED_ETAG_WITNESS`
(`crates/omnigraph/src/db/table_store.rs:1168`), whose op no GQ step reaches.
The first one, `mutation.sidecar_confirm_put` (op `Mutation`, effects
`[Fail, Skip]`), wraps the confirm put through `guarded` inside
`confirm_occ_sidecar_v9` (`crates/omnigraph/src/db/manifest/recovery.rs`).
Failure leaves the sidecar Armed with no visible commit and the reopen
rolls back; skipping loses the put while the in-memory sidecar still confirms. Its sibling
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

**Store places.** The decoration installed at `STORAGE` is the DST harness's
`FailingStorage` (`crates/omnigraph-dst/src/harness.rs:2217`), which already
routes every `StorageAdapter` method through a hook: `read_fault` on the
reads and listings and `latent_fault` on the content reads, `write_fault` on
the writes, rename, delete and compare-and-swap, `maybe_misdirect` on the
three puts; `exists` is unhooked and is no place. A store place is one of
those method families given a seam name, listed once in that crate as
`STORE_PLACES` beside the implementation, as the engine catalog sits beside
the engine's sites. The store places cover the control objects the engine
writes through `StorageAdapter` itself: the sidecars under `__recovery/`,
the queues and the markers. Every Lance dataset is outside them, the graph
tables and `__manifest` alike: a publication into a branch's `__manifest`
is a Lance `MergeInsertBuilder` write
(`crates/omnigraph/src/db/manifest/publisher.rs:942-967`) that crosses
`OBJECT_STORE`, which no store place reaches, so no `storage.put` subject
selects a dataset object.

| place | adapter methods | actions the hook implements | admitted by this amendment |
|---|---|---|---|
| `storage.put` | `write_text`, `write_bytes`, `write_text_if_absent` | `misdirect`, `lose`, `error`, `delay`, `corrupt` | `misdirect` |
| `storage.delete` | `delete`, `delete_prefix` | `lose`, `error`, `delay` | none |
| `storage.rename` | `rename_text` | `lose`, `error`, `delay` | none |
| `storage.cas` | `write_text_if_match` | `error`, `delay`, `corrupt` | none |
| `storage.get` | `read_text`, `read_text_if_exists`, `read_text_if_exists_bounded`, `read_bytes_if_exists_bounded`, `read_text_versioned` | `error`, `delay`, `corrupt` | none |
| `storage.list` | `list_dir`, `list_dir_bounded` | `error`, `delay` | none |

A cell is admitted when the rule list has an arm for it and every adapter
method of its row honors it; the random plan's own verbs do not count as an
arm, and an action a hook implements for part of a row names its method
subset: today the write corruption hook is text-only (`write_text`,
`write_text_if_absent`) and the read one skips `read_bytes_if_exists_bounded`;
`StorePlaceEntry.honors` is per row, and a method subset gets a field when an
admitted action needs one. This amendment admits `misdirect` on `storage.put` and the store
effect on `recovery.sidecar_write`; every other cell is a later change that
touches the hook and the table.

A store place is not a `DecideSeam`: `STORAGE` is a wrap seam declared
`Op::Unreachable` (`crates/omnigraph/src/storage.rs:324-331`), so a store
place lives in `STORE_PLACES`, not in the engine catalog. The `storage.`
prefix selects nothing: `storage.local_create_if_absent_probe` is a
decision seam of the engine catalog
(`crates/omnigraph/src/db/omnigraph.rs:3989`, `Op::AnyWrite`), so the GQT
runner resolves `at` by exact name, the engine catalog first and
`STORE_PLACES` second, and refuses a name both answer, naming both rows; a
`store_places` unit test asserts the two registries share no name. A store
place is admitted before any mutate or branch step as `AnyWrite` admits; a
store place before a `--- restart` or before a query step stays refused in
this amendment (§Unresolved questions). Which steps put is a property of the
engine path, not the grammar: a mutation and a merge write their sidecar
through `StorageAdapter`; a branch create writes nothing through it, so a
store place before one fails `seam_unobserved` with no matching call. A step admits at most one store
action, as a store place or as a store effect on a decision seam; a second
is refused at admission with `unsupported_environment: one store action
per step`, naming both directives. This is the one exception to distinct
seams coexisting before one operation (RFC 0045 §Seams at an explicit
step): a catalog entry declares the subject of its call but no call
ordinal, so
admission cannot tell whether two store actions reach one store call, and
the conservative rule needs no such map. A case's directive becomes one rule on the
decoration, `(place, subject glob, occurrence, action)`, consulted before
the decoration's own enable gate, so the decoration stays inactive under
GQT and draws nothing from its generator; the count advances once per
adapter call, at the top of the method before the decoration's own gates,
and the action is applied once the gates have let the call proceed; today
the three put methods consult the rule, and a method of another place
consults nothing until an action of its row is admitted. A hit reuses the random-plan verbs (`misdirect_uri`,
`crates/omnigraph-dst/src/store_places.rs`; the persisted-damage ledger,
`harness.rs:2568-2578`) and records
`(method, requested, stored)` for the runner. The rule and any one-shot are
installed and removed with the step's decisions, before the next operation
or restart. A pair the table lists but the hook does not yet implement is
refused at admission as not implemented, naming the row, so the grammar
never admits what the decoration cannot do.

**The decoration under GQT.** Before this amendment the GQT DST target
handed the engine the environment's bare adapter, so `STORAGE` was crossed
with nothing installed. Under it the memory environment
(`omnigraph-dst`) builds the `FailingStorage` with `FaultPlan::none()`,
installs it at `STORAGE` inside the scenario run after setup and holds the
guard to the end of the case, the decorator returning that one `Arc` for
every handle as the harness's own `FailingStorageDecorator` does
(`harness.rs:5310-5320`), so init, open and every `--- restart` reopen share
the decoration. `omnigraph-dst` exports the rule-list handle and the hit
record; the runner installs its own decider, counting as `Counted` does,
which on a store effect arms the one-shot through that handle. The
decoration is a property of the target, never of a case: a case without
store seams runs through it unchanged, and a seeded replay stays
byte-identical because the inactive decoration draws no random numbers.

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
only `ctor` is). Since the 2026-09-15 amendment the static itself is
declared beside the site it guards, above the item that crosses it (the
file of its most-crossed site when several do), through the `decide_seam!`
macro, and the catalog is one `catalog!` list of paths that expands to a
`pub use` per seam plus `ALL`, so `catalog::IDENT` stays the one path every
test, case and harness string resolves through and a seam is listed exactly
once. The point of
the placement is location: `Seam::new` and `Seam::decide` are
`#[track_caller]` const fns that record `Location::caller()`, so
`SeamEntry::site()` is the file and line of the declaration as the compiler
saw it, not a string anyone typed, and the site helpers are `#[track_caller]`
too, so a fired decision records the helper call whose crossing fired
(`last_fired()`, one of possibly several for one seam; a passing crossing
records nothing, so a later pass at another helper cannot displace it).
Private modules
on the path from the crate root to a declaring file are widened to
`pub(crate)` for the re-export; nothing becomes `pub`. The erased row is
`SeamEntry { name, op, effects(), site(), last_fired() }`. The engine's catalog replaces
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
`guarded(`, `park_first(` and `catalog::decide(` argument is a catalog
static rather than a literal, every declared static is re-exported by the
catalog and listed in `ALL` and has at least one reference beyond its own
declaration, no `Seam::decide` remains in a catalog or under a test module,
and every single-effect helper call names a static declaring exactly that
effect (`guarded` takes any declared set). A literal cannot
reach a helper otherwise: the helpers take `&'static DecideSeam`, so the one
string path is `catalog::decide`. The scan matches those prefixes only: a
bare `with` cannot be told from `LocalKey::with` (`dst_gate.rs:31`) by a
source walker.

**Subjects.** A catalog entry or a `STORE_PLACES` row may declare a
***subject***, the value a crossing carries that tells two crossings of one
place apart: for a store place the object's root-relative name; for a
decision seam that declares store effects, the subject of the one store call
the site precedes, declared beside the store set
(`store [Misdirect], subject "__recovery/*"`) and never restated by a case. A
subject is matched whole and
case-sensitively against the object's name with the environment root and its
`/` removed (`__recovery/<opid>.json`), with `globset` semantics fixed the
same on every host: `literal_separator(true)` (`*` within one segment, `**`
across), `case_insensitive(false)` and `backslash_escape(true)`, so an
escaped pattern selects the same objects on a Unix host and on one where
the backslash is a path separator; it is 1 to 2048 bytes, and an empty or
unparsable glob is refused at parse. `subject` in a case is admitted only against an
entry or row that declares one, and `occurrence` counts the crossings whose
subject matches. A decision seam that is crossed once per table or once per
intent may declare a subject in a later amendment without a change to the
case grammar, and a place that ever needs two-part selection takes a small
map under the same name.

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

**GQT.** The `--- seam` block is admitted when `at` names a `Decide` seam in
the engine catalog or a store place in `STORE_PLACES`, `action` is among the
seam's declared effects or its store effects (`fail` admits
a set holding `Fail` or `Contention`, explicit `contention` requires
`Contention`, and `skip` requires `Skip`), and the
step after the block is of the kind the seam's `op` maps to. The runner
installs a `Decide` that counts crossings, fires the admitted effect on the
declared occurrence and records the hit; that record, reported as
`seam_delivered` with the effect that fired, is the delivery proof, so a seam
whose effect returns success is provable without any error text. For a store
place or a fired store effect that record carries `subject` (the glob as
written for a store place; the site's declared subject for a store effect)
and `hit`, an object with
`method`, `requested` (the name the engine asked for) and, for `misdirect`
only, `stored` (the name the store used); `requested` and `stored` are
root-relative, the domain `subject` is matched in.

Coverage is a listing, not a generated corpus. `scripts/seam_corpus.py` reads
every declared seam under the engine's sources, every store place in
`STORE_PLACES` under `omnigraph-dst`, and every `--- seam` in the corpus, and
prints one row per seam and per store place: its macro invocation or table
row (`file:line`), its operation, its effects and store effects, and the
cases that arm it; `--check` refuses a case naming a seam neither the catalog
nor `STORE_PLACES` holds, or an action none of its sets admits. Coverage
stays one listing.
The `seam_delivered` record carries the same two locations, `declared_at`
and `fired_at`, so a report says which crate and file a seam lives in and
which helper call fired. The known-failure classifier requires the record's
`effect` to equal the one the case's action admits, by the same rule the
runner arms with; it resolves the name by exact membership, the engine
catalog first and `STORE_PLACES` second, compares the subject when one is
declared, and validates each store delivery's `hit`: present and complete,
`method` among the row's methods, `requested` matched by the subject, and
for `misdirect` `stored` equal to `requested` with `dstm-` prefixed on the
file name in the same directory. A report failing any of these is not a
known failure, so a replayed report cannot pass on a name and an occurrence
alone, nor on a tuple with a missing or foreign hit; the malformed-hit
reports are cases of the classifier's tests
(`crates/omnigraph-gqt/src/dst_runner/known_failure/tests.rs`).
A proof case is written by hand, one per seam, because a step kind alone is
not enough to cross a site (`MUTATION_POST_SIDECAR_PRE_FORK` fires only with
a deferred fork, `crates/omnigraph/src/exec/staging.rs:1320-1324`, so a
case generated from the step kind alone fails `seam_unobserved`). Reachable
seams without a case and seams whose operation no step starts are the two
debts the listing reports; at this RFC's implementation the catalog holds 89
seams, 44 reachable, 8 armed by a case, 45 unreachable (the listing is the
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
- Every decision seam is declared beside a site and indexed by its crate's
  catalog; every catalog entry has a site, and the entry's `site()` is the
  declaration the compiler recorded. The five non-decision seams (`CLOCK`, `IDS`, `GATE`, `STORAGE`,
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
  has a site, and every single-effect helper call names a seam declaring
  exactly that effect.
- The seams crate's own tests: a multi-effect seam fires the effect named at
  install, refuses the plain installer, refuses an undeclared effect at
  install, and refuses at the crossing a decider firing outside the set.
- GQT: the second case at `mutation.sidecar_confirm_put`,
  `mutation_sidecar_confirm_put_failure_rolls_back.gqt`, green with no engine
  change beyond the seam's set; the 602 case unchanged but for the name.
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
- GQT: `issue_601_foreign_named_sidecar_blocks_branch.gqt` red on the
  unfixed engine with its `known_failure` marker on the third step, the
  marker's reason copied from the red report; the same bucket state reached
  through `storage.put` with `subject: __recovery/*` in the sibling case
  `issue_601_foreign_named_sidecar_via_store_place.gqt`, carrying the same
  marker (both markers deleted by the #601 fix, which heals a sidecar at
  the uri it was listed from; both cases green since).
- `omnigraph-dst`: the decoration's rule list fires on the declared
  occurrence of the matching subject and on no other call.
- DST: the pinned scenarios strict-replay unchanged, the empty rule list
  consulted at the top of every put.
- GQT: the corpus green with the decoration installed under every DST run.

The `--- seam` admission rule and the source walker are rules applied to
people, so both tables below are part of the evidence. The walker's own
fixture test covers duplicate listings, the reference boundary and helper
pairing; the GQT refusal corpus covers admission.

| Way to satisfy the rule without doing the work | What stops it |
|---|---|
| Renamed import (`use crate::seams::fail as f;`) | outside the walker's grammar; the helpers take `&'static DecideSeam`, so a renamed helper still cannot take a literal, and each single-effect helper asserts the declared set at every crossing (`assert_eq!`, release builds included) |
| A decider firing an effect the seam does not declare | `crossed()` panics naming the seam, the effect and the declared set; the `_with` installers refuse it earlier |
| A site listing an effect its code does not survive (`Skip` where `None` is not handled) | no type catches it; the review rule in §Site helpers is the guard, and the proof case is the evidence |
| A wrapper function that calls a helper | a wrapper's parameter is typed `&'static DecideSeam`, so it can only carry a catalog static; the walker pairs the call where the static is named (a full path or an imported name), the crossing assert covers the rest |
| A macro that expands to a helper call | a literal macro body is scanned like any other site; a macro that builds the call from a token argument is outside the grammar and left to the crossing assert |
| Two statics sharing one `name` string | the catalog test in `failpoint_names_guard.rs` asserts unique names, and `catalog::decide` would otherwise answer the first |
| Wrong effect/helper pairing (`skip(&A_FAIL_SEAM)`, `fail(&A_FAIL_OR_SKIP_SEAM)`) | the helper rejects a seam whose declared set is not exactly its own effect, and the walker reports the pairing; a multi-effect seam reaches a site only through `guarded` |
| A site that calls `with` directly on a decision seam | the walker's grammar lists no such helper, so the static has no recognised crossing and the catalog test reports it as dead weight |
| A catalog entry with no site | the catalog test fails on a static that no source or test file names |
| A `--- seam` naming a seam the case's step kind cannot cross | admission compares the seam's `op` to the next step's kind and refuses before the case runs |
| `subject` on a seam or row that declares none | admission refuses it naming the entry; on a seam that declares its own, admission refuses the restatement naming the declared subject |
| A store action on a decision seam whose set lacks it, or an engine action on a store place | admission refuses it by membership, as for any undeclared effect |
| A store place and action the table lists but the hook does not implement | admission refuses it as not implemented, naming the row |
| A store effect declared at a site that is followed by two store calls of the kind | no type catches it; the review rule in §Design **Store effects** is the guard, and the `runner_dispatch.rs` test over the proof case, which asserts the hit's method and both names, is the evidence |
| Two store actions before one step (a store place beside a store effect, or two store places) | admission refuses the second with `one store action per step`, naming both directives |

| Route the user docs give an honest author | Does the rule accept it |
|---|---|
| Declare a static, call the matching helper at the site, add it to `ALL`, write its proof case | yes; the listing shows the seam with its case |
| Declare a static with several effects around one operation, call `guarded` at the site, write one proof case per effect | yes; the listing shows the seam with its cases, and any further case at that site needs no engine edit |
| Declare a static for a site no step can reach yet | yes; the listing reports it as unreachable, and no case may arm it |
| Need an outcome the three effects do not cover | add the `Effect` variant and its `guarded` arm, in the seams crate and the engine, so cases can name it too, or a store effect, see **Store effects** |
| Arm an existing seam from a Rust test | yes, through the `DecideSeam` methods; the `_with` spelling for a multi-effect seam |
| Arm a seam from a logic test with `--- seam` | yes when the action is among the declared effects and the next step's kind matches the `op` |
| Arm a store fault from a logic test at an engine seam that declares a store effect | yes; the delivery record carries the effect and the store's own hit |
| Arm a store fault from a logic test at a store place with a `subject` | yes when the pair is in `STORE_PLACES` and implemented |

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

A second PR (2026-09-15) carries the amendment: the effect set on
`Seam::decide` and `Decision::Fire(Effect)` in the seams crate, the `_with`
installers, `guarded` in the engine, the 97 catalog entries represented by
effect sets, `mutation.sidecar_confirm_put` with the set `[Fail, Skip]`, membership
admission and the effect in `seam_delivered` in GQT, the walker and
`seam_corpus.py` reading sets, and the second case at the put.

A third PR (2026-09-15) carries the store half: the decoration installed
under the GQT DST target, the store-effect set with its pass-through at the
site helpers and the one-shot handoff in the decoration,
`recovery.sidecar_write` declaring `store: [Misdirect]` beside its unchanged
`[Fail]`, the `STORE_PLACES` table with the rule list and the `misdirect`
cell of `storage.put`, `SeamAction` and `admitted_effect` in the GQT runner
reading store actions, `seam_corpus.py` reading `STORE_PLACES` and admitting
them, the `subject` field with admission by entry or row, `seam_delivered`
carrying the subject and the hit, the
[#601](https://github.com/ModernRelay/omnigraph/issues/601) case as a known
failure, the GQT README's four-fields and reserved `--- fault` paragraphs
(`:64`, `:100-105`), and this amendment to RFC 0066 and RFC 0045.

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
- Whether a seam's kind (decision site, decorator, value source, hook) becomes
  listed metadata beside `op` and `effects`, so one listing covers the five
  non-decision seams and the runner refuses a `--- seam` naming a decorator
  by rule rather than by absence from `ALL`. Today the kind is the behavior
  type parameter only; the reader that would justify the field is a unified
  listing, and the `dst`/`failpoints` feature split between the catalog and
  those five statics is the cost to settle first.
- Store effects are a second set on the catalog entry, not variants of
  `Effect`; whether they join `Effect` once more than one exists is left
  open, and `crossed()` accepts either shape.
- Whether a store place may precede a `--- restart` or a query step: until it
  may, the reopen's own listing and reads are unreachable from a case, and
  widening the placement rule is its own change.
- Which decision seams declare a subject first: the candidates are those
  crossed once per table in a merge and once per intent in a heal.
- Which other sites followed by exactly one store call declare a store effect
  next (`recovery.sidecar_confirm`, `mutation.sidecar_confirm_put`,
  `recovery.orphan_discard_audit_append`, `recovery.record_audit`,
  `storage.local_create_if_absent_probe`): each is its own review under the
  §Store effects rule.

## Decision log

- 2026-09-15, effect sets (one seam, several actions): a decision seam
  declares `&[Effect]` instead of one `Effect`; `Decision::Fire` carries the
  effect; `guarded` wraps an operation and holds the only match; the runner
  admits by membership and reports the effect. Replaced sentences: in
  §User and operational behavior, "The action must match the effect the site
  declares, so a mismatch is refused before the case runs" and the
  `seam_delivered` sentence; in §Design, the `Seam` listing (`effect:
  Option<Effect>`, `decide(.., effect: Effect, ..)`, `effect()`, the `Effect`
  enum with `Hold`), the `Decide` row of the behavior traits table, the
  shipped-deciders paragraph, the whole **Site helpers** paragraph, the first
  sentence of **`Hold`**, the **first skip seam** paragraph, the `SeamEntry`
  row, the walker sentence "every helper call is paired with the effect its
  static declares", and the **GQT** and coverage paragraphs; in §Evidence,
  the walker bullet and the three table rows on pairing, `Custom` and
  `--- seam` admission. `Effect::Hold` is removed: `Hold` is a decider that
  passes after the park and was installable on any seam already
  (`Rendezvous::park_first` installs it on fail seams). Motivation recorded
  in the doc workspace, task 0196: a `.gqt` author could not write a new
  action at a known site without an engine edit, and the confirm put already
  needed `fail` beside `skip`.
- 2026-09-15, same PR, seam location: a seam static is declared beside the
  site it guards and the catalog indexes it (`pub use` + `ALL`); the
  constructors and the site helpers are `#[track_caller]`, so every entry
  knows its declaration (`site()`) and its last crossing
  (`last_fired()`), and `seam_delivered` reports both as `declared_at`
  and `fired_at`. Replaced sentences: in §Design **The catalog**, "keeps a
  hand-listed `catalog.rs`" now describes the index and the re-exports, and
  the erased row gains the two locations; the walker sentence on "every
  catalog static has at least one reference"; the coverage paragraph; in
  §Invariants, "Every decision seam is in its crate's catalog". Motivation:
  a name says the area, not the crate or file, and nothing hand-written
  stays true; the compiler's record does.
- 2026-09-15, same PR, naming: a seam name is a place (`area.position`, the
  position spelled `post_`/`pre_`/`before_`/`after_`/`between_` or the
  operation it wraps); the action is the event, so a name never carries an
  outcome or an effect word, which under an effect set would read as a lie
  for the second action. Five names migrated: `init.manifest_create_ack_lost`
  → `init.manifest_create_post_native`, `init.table_create_ack_lost` →
  `init.table_create_post_native` (the `branch_create.post_native`
  precedent), `change_feed.skip_etag_witness` → `change_feed.etag_witness`,
  `optimize.inject_reindex_conflict` → `optimize.post_compact_pre_reindex`,
  `publish.load_state_retryable_contention` → `publish.load_state`. The
  fourteen operation-named seams without a position (`recovery.sidecar_*`,
  `cleanup.*`, `classify.fresh_read`, the two probes, `init.schema_cleanup_delete`,
  `mutation.sidecar_confirm_put`) stay: they name the operation they guard,
  and the honest shape for such a site is `guarded` around that operation.

- 2026-09-15: expose `action: contention` to select `Contention` even when a
  seam also declares `Fail`. Preserve the `fail` fallback on contention-only
  seams. The corpus and runtime both report the declaration macro's invocation
  line, including when documentation separates the invocation from the static.
- 2026-09-15, store faults as seam places: a seam reached at the store is a
  `--- seam`, never a `--- fault`; a catalog entry may declare a second set
  of store effects beside its effects, the first being `Misdirect`, passed
  through at the site as `Decision::Store` and acted on by the storage
  decoration, with `effects()` unchanged and each site helper gaining one
  pass-through arm; store places are admitted from `STORE_PLACES` in
  `omnigraph-dst` by exact name, never by the `storage.` prefix, selected by
  `subject`, at most one store action per step; the GQT DST target installs
  the decoration on every run. Replaced sentences: in §Design, "`pub enum
  Decision { Fire(Effect), Pass }`", the `Decide` row of the trait table
  ("`Fire(Effect)` or `Pass`") and, in the `guarded` match, "`Pass` runs
  `op` and returns `Some(op's result)`" (now `Store(_)` runs `op` too); in
  §User and operational behavior, "`action` is `fail`, `contention`, or
  `skip`", "`--- fault` keeps its name for storage-boundary faults, specified
  in a separate amendment to RFC 0045" and "A `--- seam` block inherits the
  placement rules of `--- fault`"; in §Design, "The three effects are the
  whole vocabulary: a site needing another outcome adds an `Effect` variant
  and its arm in `guarded`", "The `--- seam` block is admitted when `at`
  names a `Decide` seam in the engine catalog", the `seam_corpus.py` coverage
  paragraph and the known-failure classifier sentence; in §Evidence and
  tests, the honest-routes row "Need an outcome the three effects do not
  cover". Motivation: the foreign-named sidecar of issue 601 is a valid body
  under a wrong object name, which no engine line produces and no `fail` or
  `skip` can plant, and the DST target ran without the decoration the nightly
  runs with, so a store fault the nightly can inject had no case form.
