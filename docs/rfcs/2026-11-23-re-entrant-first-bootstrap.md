---
rfc: "2026-11-23-re-entrant-first-bootstrap"
title: "Re-entrant first bootstrap with classified effects"
track: maintainer
status: draft
implementation: in-progress
authors:
  - ragnorc
created: 2026-11-23
updated: 2026-11-23
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Re-entrant first bootstrap with classified effects

> A term set in ***bold italics*** is being defined at that exact spot.

## Summary

`bootstrap_serving` initializes a fresh S3 cluster root under a retained
bootstrap lock. Today any failure after that lock is written leaves the root
unusable: the lock is retained by design, nothing short of the exact-id
emergency unlock may delete it, and every rerun refuses with
`bootstrap_root_not_fresh`. This RFC makes the first
bootstrap resumable by the process that started it, and by no one else.

- The bootstrap lock records its ***bootstrap identity***, the caller's cluster
  incarnation and optional request id, and an attempt number. A rerun of the
  same identity takes over its own retained lock by a conditional replace of
  the exact current lock version, and only when its request asserts that every
  earlier attempt is ***excluded***: terminated, sending nothing more. A lock of
  another identity, a serving lock, a legacy lock without an identity, or a
  version that changed is refused exactly as today. Bootstrap deletes and
  releases no lock.
- Bootstrap runs as named steps: `lock`, `freshness`, `ledger`, `deployment`
  and `receipt`. Its progress is read from the two records it already has,
  `lock.json` and the ledger `state.json`, and from nothing else: no receipt
  or other object is written for it. A rerun continues from the one ledger
  state of each kind that this bootstrap's own steps leave, and refuses every
  other state without changing anything.
- Every failure carries a fixed-vocabulary outcome `{code, step, effects,
  retryable}`, where `effects` is `none`, `confirmed` or `unconfirmed`,
  derived from the storage requests the attempt actually sent.
- The effect-free freshness probes retry transient and unclassified read
  errors with backoff. Every probe of one attempt shares one 240-second
  deadline. An error is never read as absence.

The graph format, the serving receipt, `claim_bootstrap_serving`, the ledger
protocol and the lease are unchanged. Containment still rests on conditional
writes: every bootstrap write is content-addressed, created only if absent, or
replaced only from its exact predecessor version.

Contract: keeps Boundary (The store, Failures covered); cluster
initialization is owned by [control plane](../dev/control-plane.md) (Boundary,
One graph).

## Motivation

On 2026-11-23 a managed first bootstrap failed in production. The sequence in
`bootstrap_in_store` was a freshness check, `acquire_bootstrap_lock`, a second
freshness check, the empty ledger, the initial policy deployment and the
receipt. The second freshness check's `exists()` stalled for about 30 seconds
and failed with "couldn't read the response body", after the lock was written.
The bucket held `__cluster/lock.json` and no ledger. Every other bootstrap that
week took one to two seconds, so the failure was one transient read.

The read had no effect. The process nevertheless exited reporting possible
effects, because the error carried no account of what had been written, and
the retained lock made every retry fail with `bootstrap_root_not_fresh`. The
only remaining action was to abandon the root. Two faults combine here:

1. **One transient read was terminal.** `require_absent_native_paths` mapped
   any `exists()` error to `bootstrap_freshness_unknown` on its first attempt.
2. **A retained lock had no owner a rerun could prove.** The lock record named
   an operation and a process id, not the bootstrap it belonged to, so no rerun
   could tell its own lock from a stranger's. Deleting the lock would be
   unsafe: a late request of the first attempt could then recreate it or land
   on a successor's state.

The fix must keep the property that made the lock retained in the first place:
nothing a failed attempt left behind can be adopted by a different caller, and
nothing a late request of the failed attempt sends can change state a later
attempt wrote.

## User and operational behavior

The library entry point gains a required request:

```rust
pub async fn bootstrap_serving(
    config_dir: impl AsRef<Path>,
    caller: &DeploymentCaller,
    request: &BootstrapRequest,
) -> Result<BootstrapServingReceipt, BootstrapFailure>;

pub struct BootstrapRequest {
    pub identity: BootstrapIdentity,     // BootstrapIdentity::new(incarnation, request_id)
    pub prior_attempts: PriorAttempts,   // Excluded | NotExcluded
}
```

- **`BootstrapIdentity`** is the cluster incarnation and an optional request
  id, each 1 to 128 bytes of ASCII letters, digits, `.`, `_`, `:` and `-`;
  anything else is refused with `bootstrap_identity_invalid` before any
  request is sent. Omnigraph does not interpret them; a managed control plane
  passes the incarnation and creation request it already signs.
- **`PriorAttempts`** has no default. `Excluded` asserts that every earlier
  process of this identity has terminated and sends nothing more; the caller
  proves it (a managed operator observes the old process exit). `NotExcluded`
  says the caller cannot. The engine never infers exclusion from time, a
  missing heartbeat or a process id. On a fresh root the value changes
  nothing. On a root holding this identity's retained lock, `NotExcluded`
  refuses with `bootstrap_exclusion_required` and changes nothing.
- **`BootstrapFailure`** carries the diagnostic and its outcome.
  `failure.outcome()` serializes as
  `{"code": "...", "step": "...", "effects": "...", "retryable": bool}`:
  - `step` is one of `input`, `lock`, `freshness`, `ledger`, `deployment`,
    `receipt`: the step that failed.
  - `effects` is `none` when the attempt sent no write or every write it sent
    was answered as not applied; `confirmed` when every write it sent has a
    known outcome and at least one applied (the acquired lock with its returned
    version, a created ledger); `unconfirmed` when any write it sent has an
    unknown outcome: a conditional write the storage layer reports
    `Unresolved`, a request still unanswered when the attempt ended, or a
    storage scope left uncertain.
  - `retryable` is true only when `effects` is not `unconfirmed`, the attempt
    left no accepted initial deployment incomplete (a rerun refuses that
    state), and the failure is a storage request whose class is transient or
    unclassified: a read, a probe (including the probe deadline), or a write
    the store refused without applying it. It says a rerun of the same
    request, with `PriorAttempts::Excluded` once the failed process is gone,
    can make progress. It is advice: the rerun's own checks decide.
- **A rerun after a failure** continues from what `lock.json` and
  `state.json` show (the decision table below). A write that landed resumes
  whether or not its answer arrived, so a lost answer on the lock, the ledger
  create or the terminal ledger revision resumes too. The one state a rerun
  cannot attribute to a finished step, an accepted initial deployment whose
  outcome the ledger has not recorded, refuses with
  `bootstrap_deployment_outstanding`. A refused rerun changes nothing: it
  reads the ledger before it replaces the lock.
- **The caller reruns the same request**, with the same identity and
  incarnation, within the admission authority of its original request. Core
  has no separate resume authority and no resume verb: a rerun is an ordinary
  `bootstrap_serving` call, and the lock and the ledger decide what it does.
- **Codes added:** `bootstrap_identity_invalid`, `bootstrap_exclusion_required`,
  `bootstrap_attempts_exhausted` (the attempt counter would overflow),
  `bootstrap_lock_changed`, `bootstrap_deployment_outstanding`.
  `bootstrap_root_not_fresh`, `bootstrap_freshness_unknown` and
  `bootstrap_state_mismatch` keep their meaning.

Everything after the serving receipt is unchanged: the same 16 KiB receipt,
the same `--bootstrap-handoff` claim. A receipt returned by an earlier attempt
names that attempt's lock version, so once a rerun has replaced the lock, the
earlier receipt's claim fails its exact-version CAS and claims nothing.

## Design

### The bootstrap lock, version 4

`__cluster/lock.json` gains a held version 4, written only by
`bootstrap_serving`:

```json
{
  "version": 4,
  "lock_id": "01K…",
  "operation": "bootstrap_serving",
  "created_at": "2026-11-23T02:15:13Z",
  "pid": 4242,
  "incarnation": "inc_…",
  "request_id": "req_…",
  "attempt": 1
}
```

`request_id` is omitted when the identity has none. Version 4 requires the
bootstrap operation, an incarnation, an attempt of at least 1, and no release
id or lease; every other version refuses the three new fields. The attempt is
an unbounded counter, not a budget: the caller's admission window bounds how
often it reruns, and the only refusal is the counter's own `u32` overflow,
checked once, where the re-entry computes the next attempt. Older binaries
refuse version 4 as an unsupported version, so they neither claim nor take it
over. Version 1 bootstrap locks written by earlier releases still parse, and a
receipt of theirs can still be claimed.

`lock_id` is the attempt's own id, a fresh ULID per attempt. The admission's
current-owner check compares it, so after a rerun replaces the lock, an earlier
attempt that is somehow still running fails its next deployment admission
check. That check is not a fence; the conditional writes below are.

The lock step:

1. Read `lock.json` with its version.
2. Absent: run the full freshness check (below), then create a version 4
   record with attempt 1 if absent, and read its version back. A create that
   finds a lock refuses with `bootstrap_root_not_fresh`.
3. A record naming this identity: refuse with `bootstrap_exclusion_required`
   unless the request says `Excluded`; read the ledger (below) and refuse if
   the rerun would refuse; then replace the record by if-match on the exact
   version read, with a new `lock_id`, the attempt plus one and the same
   identity. A precondition failure refuses with `bootstrap_lock_changed`. An
   attempt at `u32::MAX` refuses with `bootstrap_attempts_exhausted`.
4. A record naming another identity, or none, refuses with
   `bootstrap_root_not_fresh`, as today. Only version 4 can name an identity
   (parsing refuses the fields on every other version), so a version 1
   bootstrap lock is simply an identity mismatch: there is one refusal path,
   not a separate legacy check. An unparseable record, a serving lock and a
   released marker refuse the same way.

### Steps and progress

Bootstrap records its progress in no object of its own. The lock step's
record is `lock.json`, which names the identity. Every later step's effect is
on the ledger, and the ledger already records it: its id, its revision, and
the deployment's id and `input_digest`, first in `outstanding` and then in
`deployment_results`. A second copy of those facts, written after its effect,
would leave a gap between the effect and the copy in which a killed process
or a lost answer makes the root terminal, and would add one more such gap for
every new step. So nothing is written under `__cluster/bootstrap/`, and a
rerun ***inspects*** `lock.json` and `state.json` only.

| Step | Effect | Recorded by |
|---|---|---|
| `lock` | create, or if-match re-entry, of `lock.json` | `lock.json` |
| `freshness` | probes only | nothing: repeated while `state.json` is absent |
| `ledger` | create-if-absent of `state.json` at revision 1 | `state.json` revision 1 |
| `deployment` | content-addressed input and payloads; `state.json` revisions 2 and 3 by if-match | `state.json` `outstanding`, then `deployment_results` |
| `receipt` | none | the returned serving receipt |

With this identity's lock read, a rerun decides from `state.json` alone:

| `state.json` | Decision |
|---|---|
| absent | repeat the read-only freshness check, then create the ledger |
| exactly an empty v2 ledger at revision 1 (`next_sequence: 1`, no `outstanding`, no results) | deploy |
| revision 3 holding exactly one converged result, sequence 1, whose `input_digest` is this input, with nothing outstanding | go to `receipt` |
| revision 2 with sequence 1 of this input outstanding | refuse `bootstrap_deployment_outstanding` |
| anything else | refuse `bootstrap_state_mismatch` |

Each row that continues is a state only this bootstrap's own steps can
leave:

- **Absent.** Freshness passed before the lock was created, and has nothing to
  remember: it is read-only, so the rerun repeats it.
- **The empty ledger.** With this identity's lock held, freshness proved
  `state.json` absent. The lock key then stays present, and only this
  identity can re-enter it. So an empty v2 ledger at revision 1 can only be
  this bootstrap's own create, whichever attempt sent it. The rerun uses it.
  This needs no deterministic ledger id.
- **The completed deployment.** The existing deployment lookup answers that
  sequence 1 of this ledger is complete, and the terminal check
  (`validate_terminal`) then requires every field of the serving receipt from
  `state.json`: one converged result of this input, `next_sequence: 2`,
  nothing outstanding, the applied policy equal to the immutable input. The
  rerun rebuilds the serving receipt from them and the lock it now holds.

The decision runs before a lock re-entry, so a rerun that would refuse
changes nothing, and again under the lock, where its answer is the one acted
on. After the deployment step, the same decision on the ledger it left must
answer `receipt`.

**The outstanding deployment.** A ledger at revision 2 whose `outstanding` is
sequence 1 of this input is the one state whose outcome is genuinely unknown:
the acceptance landed and the completion did not. The deployment protocol
already owns it: `reconcile_deployment` under exclusion. First bootstrap does
not reconcile it, because that would run a second owner of the ledger
protocol under the bootstrap admission; it refuses with
`bootstrap_deployment_outstanding`, and changes nothing. Recovery of that root
is the managed control plane's, as for any other root it cannot attribute.

The freshness check requires the root free of graphs, `__manifest`,
`__history`, recovery records, approvals, `state.json` and
`__cluster/resources`, with this identity's lock present. The pre-lock check
also requires the lock absent. The deployment step checks the graph and
recovery paths again; bootstrap never writes them.

### Effects are what the attempt sent

The attempt's store routes every control request through a recording adapter.
It forwards reads, counts each conditional write when it is sent and when it
is answered, and refuses every unconditional write, delete and rename before
sending it: first bootstrap never issues one, and now cannot. The answer
classes follow the storage contract of
[Storage outcomes per operation and restartable ownership](2026-10-19-storage-outcomes-and-restartable-ownership.md):
an applied create or update is applied; a precondition failure, or any error
other than `StorageError::Unresolved`, applied nothing; `Unresolved`, or a
request with no answer, is unknown. A storage scope left uncertain also makes
the attempt `unconfirmed`. The outcome is computed once, at failure, from those
counts; no step reports its own effects.

The adapter is the single owner of `effects` only while it overrides every
`StorageAdapter` method: a method it inherited would bypass it unseen. A test
reads the trait's methods from its source and requires each one to be placed
as a forwarded read, a recorded conditional write or a refused mutation, the
adapter to override all of them, and a default body only on the two
effect-free accessors.

**The class of a failure** comes from one place per kind of request. A failed
control-object read gets its storage class where the cluster store converts
the storage error into a diagnostic, once, for every read; no call site adds
it. A write's error never gets a class there, because the served routes read a
classified diagnostic as "this request applied nothing", which a write error
cannot say for the operation around it. First bootstrap stops at its first
write error, and the recording adapter keeps the class of a write the store
refused without applying it; `retryable` uses that.

### Freshness probes retry inside one deadline

Every effect-free `exists()` probe of one attempt shares one deadline, 240
seconds after the attempt starts. The attempt creates it once and passes it to
each freshness check and each graph-and-recovery check it runs; no check
starts a budget of its own. A probe whose error is transient or unclassified
is retried after a pause of 250 milliseconds, doubling to at most 8 seconds,
and each probe is itself cut off at the deadline. Errors of every other class
(configuration, permission, precondition, permanent) fail at once. At the
deadline the probe fails with `bootstrap_freshness_unknown`. Each retry is
logged with its path and the error class. A probe that answers is never
second-guessed, and an error is never taken for absence.

So an attempt's probes end within 240 seconds of its start, however many
checks it runs, leaving at least 360 seconds of a managed operator's
600-second attempt bound for the conditional writes, whose re-issue policy is
bounded on its own (four re-issues within about 3 seconds per request), and
the policy deployment. The other reads, of the lock and the ledger, fail at
once; the rerun that follows them resumes. A claim of the serving receipt is
not an attempt and has its own deadline of the same length.

### Containment

Every write bootstrap sends is one of:

- **content-addressed**: the deployment input and policy payloads, whose late
  copy writes the same bytes;
- **create-if-absent**: the lock and the ledger, whose late copy cannot land
  once the key exists;
- **if-match on an exact version**: lock re-entry and ledger revisions 2 and 3,
  whose late copy names a predecessor version that a later attempt has already
  replaced.

So a late request of an earlier attempt can land only where no later attempt
has written, and the inspection refuses to continue past any state it cannot
attribute. If an excluded attempt were in fact still running, its ledger or
deployment write would race the rerun's on the same key and precondition, and
exactly one would apply; the loser refuses, and the next inspection refuses
the unattributed state. Re-entry never deletes or releases the lock, and the
lock key stays present from the first create to the server's claim.

## Invariants

- **Invariant 11, bounded and observable failures.** The probe retry is
  bounded by one deadline per attempt and a backoff cap, applies to
  effect-free reads only, and is logged per retry; the outcome makes every
  failure's effects explicit. The deny-list's "silent retries" hide outcomes;
  this retry hides none.
- **Invariant 12, one source of truth.** Bootstrap progress is `lock.json`
  and `state.json`. No second record copies what the ledger records, so none
  can drift from it, and a new step adds no record of its own.
- **Invariant 8, loud integrity failures.** An unattributable ledger, an
  outstanding initial deployment and a foreign or legacy lock all refuse;
  nothing is inferred.
- **Deny-list, process-local locks as fencing.** Re-entry is not a fence and is
  not presented as one. Exclusion of the earlier attempt is the caller's
  asserted obligation, as process exclusion is for every other ownership
  transfer in [control plane](../dev/control-plane.md); the conditional writes
  keep a violated assertion from corrupting state.

## Compatibility and reversibility

- **Lock records.** Version 4 is new; older binaries refuse it with
  `unsupported_state_lock_version` and never claim it. Version 1 bootstrap
  locks keep their behavior: they are claimable with their receipt and never
  re-entrant, so a root left by the 2026-11-23 failure stays retained. Its
  recovery is the managed control plane's, for example a new incarnation on a
  new root.
- **No new objects.** Bootstrap writes nothing beyond the lock, the ledger and
  the deployment's own content-addressed input and payloads.
- **The emergency path.** Bootstrap never deletes or releases its lock, and
  nothing in it removes one. `force_unlock_storage_root` with the exact
  `lock_id` (from `cluster status`) remains the documented emergency path, and
  it can remove a held version 4 lock, as it can any held lock. Its
  precondition is unchanged: the operator first excludes every owner and
  establishes that no request of one can still land.
- **API.** `bootstrap_serving` takes a `BootstrapRequest` and returns
  `BootstrapFailure`; callers add both. The serving receipt and its claim are
  unchanged.
- **Reverting** to the previous binary leaves version 4 locks unclaimable by
  it, which is the safe direction: an older binary cannot adopt what it cannot
  read.

## Alternatives

- **Retry the read only.** Closes the incident's trigger, not its class: any
  later failure, a lost response or a killed process, still makes the root
  terminal.
- **Delete or release the retained lock on failure.** Rejected: a late request
  of the failed attempt can land after the delete, and the lock key would no
  longer be continuously present, which is what keeps an older bootstrap create
  from recreating its owner.
- **Expiry or lease takeover.** The serving lock's lease exists because a server
  stops its own writes when fenced. A bootstrap process has no such stop, and an
  expiry would make clock behavior part of correctness. The caller's exclusion
  proof is stronger and already exists in the managed operator.
- **Keep the same `lock_id` across re-entry.** Simpler records, but the
  admission's current-owner check compares `lock_id`, so a still-running
  earlier attempt would pass it. A fresh id per attempt costs nothing.
- **Deterministic ledger ids**, so the ledger create is idempotent and a lost
  ledger response is resumable. It would derive a ULID from the identity,
  giving the ledger id a meaning it does not have. Not needed: under this
  identity's lock an empty revision 1 ledger can only be this bootstrap's own
  create, whatever its id.
- **Per-step receipts under `__cluster/bootstrap/`.** The first draft wrote a
  create-once receipt after each step's effect and resumed from them. Each
  receipt was a second copy of a fact the ledger records, written after the
  effect, so a process killed, or a receipt write refused, between the two
  left the root terminal; two of those failures even reported
  `retryable: true`. Every new step would add a receipt type and a gap.
  Reading progress from the ledger removes the copies and the gaps together.
- **Reconcile an outstanding initial deployment inside bootstrap.** It would
  make the last uncertain state resumable, but it would add a second owner of
  the ledger protocol, running under the bootstrap admission, beside
  `reconcile_deployment`. Refusing keeps one owner; the state needs a process
  killed in the window between two ledger writes.
- **An attempt cap.** The first draft refused after 16 attempts, checked in
  two places with two codes. The caller's admission window already bounds
  reruns, so the cap only duplicated that bound.

## Evidence and tests

The owner is `crates/omnigraph-cluster/src/deployment/bootstrap_fault_tests.rs`,
whose in-memory backend applies real create and if-match semantics, injects
lost answers only after a write committed and refusals that applied nothing
only before. A lost answer is injected as `StorageError::Unresolved`, as the
storage layer reports a conditional write whose re-issues and read-back did
not settle it.

- A same-identity rerun after a lock-then-freshness failure resumes and
  completes, with one lock lineage: one create, one if-match from the exact
  created version, no delete. The failed attempt's probes end exactly at the
  attempt's one deadline.
- A foreign incarnation, a foreign request id, a version 1 bootstrap lock and a
  serving lock refuse re-entry with the same code and leave the lock
  unchanged; a rerun without `Excluded` refuses and changes nothing.
- Transient probe errors are retried to success; a persistent probe error
  before the lock fails at the deadline with `effects: none` and
  `retryable: true` and writes nothing.
- A lost answer on, or a process killed right after, the lock create, the
  ledger create and the terminal ledger revision: an excluded rerun completes
  and its serving receipt is claimed.
- A transient refusal that applied nothing on the ledger create and on the
  deployment's acceptance reports `effects: confirmed, retryable: true`, and
  the rerun completes.
- A transient read after the deployment completed, of the ledger or of the
  bundle in final verification, reports `retryable: true`, and the rerun goes
  to the receipt without writing the ledger.
- A lost answer on the accepted ledger revision, or a transient refusal of the
  terminal one, leaves the initial deployment outstanding: the first attempt
  reports `retryable: false`, and the rerun refuses with
  `bootstrap_deployment_outstanding`, `effects: none`, and changes nothing.
- A rerun re-enters an attempt-1000 lock as attempt 1001; an attempt at
  `u32::MAX` refuses with `bootstrap_attempts_exhausted` and changes nothing.
- No completed bootstrap leaves an object under `__cluster/bootstrap/`.
- The recording adapter's override of every storage adapter method is
  enforced by a test that reads the trait's source.
- The existing lost-response, cancellation, claim and release tests pass.

Live S3 qualification of a resumed bootstrap is the managed control plane's.

## Rollout

1. This RFC and the native implementation. `implementation` is
   `in-progress` until the managed control plane passes identity and
   exclusion.
2. The managed control plane passes its signed incarnation and request id,
   asserts exclusion only after it observed the failed process exit, persists
   the outcome, and reruns the same request, a `retryable` failure, within
   the admission authority of the original request.

## Unresolved questions

None.

## Decision log

- 2026-11-23: Drafted after the 2026-11-23 bootstrap failure.
- 2026-11-23: Revised after review. Retitled from "Re-entrant first
  bootstrap with per-step receipts and classified effects". Progress is read
  from `lock.json` and `state.json` only, and the step receipts are removed:
  each was a second copy of a ledger fact, and the gap between an effect and
  its receipt left the root terminal. An outstanding initial deployment
  refuses with `bootstrap_deployment_outstanding`. The attempt cap is removed;
  a legacy lock refuses as an identity mismatch; every probe of an attempt
  shares one 240-second deadline; failed reads are classified once, at the
  store boundary, and refused writes by the recording adapter.
