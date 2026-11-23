---
rfc: "2026-11-23-re-entrant-first-bootstrap"
title: "Re-entrant first bootstrap with per-step receipts and classified effects"
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

# RFC: Re-entrant first bootstrap with per-step receipts and classified effects

> A term set in ***bold italics*** is being defined at that exact spot.

## Summary

`bootstrap_serving` initializes a fresh S3 cluster root under a retained
bootstrap lock. Today any failure after that lock is written leaves the root
unusable: the lock is retained by design, nothing may delete it, and every
rerun refuses with `bootstrap_root_not_fresh`. This RFC makes the first
bootstrap resumable by the process that started it, and by no one else.

- The bootstrap lock records its ***bootstrap identity***, the caller's cluster
  incarnation and optional request id, and an attempt number. A rerun of the
  same identity takes over its own retained lock by a conditional replace of
  the exact current lock version, and only when its request asserts that every
  earlier attempt is ***excluded***: terminated, sending nothing more. A lock of
  another identity, a serving lock, a legacy lock without an identity, or a
  version that changed is refused exactly as today. No lock is deleted or
  released.
- Bootstrap runs as named steps: `lock`, `freshness`, `ledger`, `deployment`
  and `receipt`. Each completed step with a storage effect writes a strict,
  bounded, versioned JSON ***step receipt*** in the native root, once, by
  create-if-absent. A rerun verifies the receipts, skips completed steps and
  replays a step only when the step is idempotent by construction. A step
  whose receipt is missing while its effect may exist is refused with
  `bootstrap_step_unconfirmed`.
- Every failure carries a fixed-vocabulary outcome `{code, step, effects,
  retryable}`, where `effects` is `none`, `confirmed` or `unconfirmed`,
  derived from the storage requests the attempt actually sent.
- The effect-free freshness probes retry transient and unclassified read
  errors with backoff inside a 120-second deadline per check. An error is
  never read as absence.

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
  - `retryable` is true only when `effects` is not `unconfirmed` and the
    failure is a storage read or probe whose class is transient or
    unclassified, including the freshness deadline. It says a rerun of the same
    request, with `PriorAttempts::Excluded` once the failed process is gone,
    can make progress. It is advice: the rerun's own checks decide.
- **A rerun after a failure** with `effects: confirmed` resumes at the first
  step without a receipt. A rerun after `effects: unconfirmed` is refused with
  `bootstrap_step_unconfirmed` naming the step, unless the uncertain write has
  since become a receipted fact. A refused rerun changes nothing: it verifies
  the receipts before it replaces the lock.
- **Codes added:** `bootstrap_identity_invalid`, `bootstrap_exclusion_required`,
  `bootstrap_attempts_exhausted`, `bootstrap_lock_changed`,
  `bootstrap_step_unconfirmed`,
  `bootstrap_receipt_invalid` (a step receipt that does not decode, is out of
  order, or names another identity or input), `bootstrap_receipt_conflict` (a
  receipt create found other bytes). `bootstrap_root_not_fresh` and
  `bootstrap_freshness_unknown` keep their meaning.

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
bootstrap operation, an incarnation, an attempt from 1 to 16, and no release
id or lease; every other version refuses the three new fields. Older binaries
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
3. A version 4 record of this identity: refuse with
   `bootstrap_exclusion_required` unless the request says `Excluded`; inspect
   the receipts (below) and refuse if a step is unconfirmed; then replace the
   record by if-match on the exact version read, with a new `lock_id`, the
   attempt plus one and the same identity. A precondition failure refuses with
   `bootstrap_lock_changed`. Attempt 16 refuses with
   `bootstrap_attempts_exhausted`.
4. Anything else, including an unparseable record, a version 1 bootstrap
   lock, a serving lock, a released marker or another identity's record:
   refuse with `bootstrap_root_not_fresh`, as today.

### Steps and receipts

Step receipts live under `__cluster/bootstrap/`, one fixed name per step,
created only if absent, at most 4 KiB, strict JSON with unknown fields
refused. Each carries the step, the identity and the input digest of the
bundle being bootstrapped:

```json
{"version":1,"step":"freshness","bootstrap":{"incarnation":"inc_…","request_id":"req_…","input_digest":"<64 hex>"}}
{"version":1,"step":"ledger","bootstrap":{…},"ledger_id":"01K…","state_cas":"sha256:<64 hex>"}
{"version":1,"step":"deployment","bootstrap":{…},"ledger_id":"01K…","deployment_id":"01K…:1:01K…","config_digest":"<64 hex>","result_revision":1,"state_revision":3,"state_cas":"sha256:<64 hex>"}
```

A receipt is written only after its step's effect was confirmed, so a receipt
always states a fact. Two steps have no file of their own. The lock step's
record is `lock.json` itself: a second copy would drift on every re-entry. The
`receipt` step writes nothing; its output is the serving receipt, rebuilt on
every attempt from the deployment receipt and the current lock, because it
binds the lock version the attempt holds.

| Step | Effect | Receipt | Replay when the receipt is missing |
|---|---|---|---|
| `lock` | create, or if-match re-entry, of `lock.json` | `lock.json` | always; it is the re-entry itself |
| `freshness` | probes only | `freshness.json` | always; the probes are reads and the receipt bytes are deterministic, so a late copy is the same bytes |
| `ledger` | create-if-absent of `state.json` at revision 1 | `ledger.json` | only while `state.json` is absent; the create decides one ledger |
| `deployment` | content-addressed input and payloads; `state.json` revisions 2 and 3 by if-match | `deployment.json` | only while `state.json` holds exactly the receipted revision 1 bytes |
| `receipt` | none | the returned serving receipt | always |

A rerun first ***inspects***: it reads the three receipts and `state.json` and
finds the first step without a receipt. The same inspection runs before a lock
re-entry, so a rerun that would refuse changes nothing, and again after it,
where its answer is authoritative.

- A receipt that names another identity or input, or that exists without the
  receipts before it, refuses with `bootstrap_receipt_invalid`.
- No ledger receipt while `state.json` exists refuses with
  `bootstrap_step_unconfirmed` at `ledger`: a ledger exists that no receipt
  attributes to this bootstrap.
- A ledger receipt, no deployment receipt, and a `state.json` other than the
  receipted revision 1 bytes refuses with `bootstrap_step_unconfirmed` at
  `deployment`: the deployment advanced the ledger and its outcome is not
  receipted.
- A deployment receipt whose state CAS no longer matches refuses with
  `bootstrap_state_mismatch`.

The freshness step, when it runs, requires the root free of graphs,
`__manifest`, `__history`, recovery records, approvals, `state.json`,
`__cluster/resources` and `__cluster/bootstrap/`, with this identity's lock
present. The pre-lock check also requires the lock absent. A resumed attempt
whose freshness receipt exists checks the graph and recovery paths again;
bootstrap never writes them.

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

### Freshness probes retry inside a deadline

A freshness check is a set of `exists()` probes sharing one deadline of 120
seconds. A probe whose error is transient or unclassified is retried after a
pause of 250 milliseconds, doubling to at most 8 seconds, and each probe is
itself cut off at the deadline. Errors of every other class (configuration,
permission, precondition, permanent) fail at once. At the deadline the check
fails with `bootstrap_freshness_unknown`. Each retry is logged with its path
and the error class. A probe that answers is never second-guessed, and an
error is never taken for absence.

A fresh attempt runs two checks, before and after the lock, so its probes take
at most 240 seconds, leaving 360 seconds of a managed operator's 600-second
attempt bound for the conditional writes, whose re-issue policy is bounded on
its own (four re-issues within about 3 seconds per request), and the policy
deployment. The other reads, of the lock, the receipts and the ledger, fail at
once; the rerun that follows them resumes.

### Containment

Every write bootstrap sends is one of:

- **content-addressed**: the deployment input and policy payloads, whose late
  copy writes the same bytes;
- **create-if-absent**: the lock, the ledger and the step receipts, whose late
  copy cannot land once the key exists;
- **if-match on an exact version**: lock re-entry and ledger revisions 2 and 3,
  whose late copy names a predecessor version that a later attempt has already
  replaced.

So a late request of an earlier attempt can land only where no later attempt
has written, and the inspection refuses to continue past any state it cannot
attribute. If an excluded attempt were in fact still running, its ledger or
deployment write would race the rerun's on the same key and precondition, and
exactly one would apply; the loser refuses, and the next inspection refuses
the unattributed state. A receipt cannot lie: it follows a confirmed effect,
and its key is written once. Re-entry never deletes or releases the lock, and
the lock key stays present from the first create to the server's claim.

## Invariants

- **Invariant 11, bounded and observable failures.** The probe retry is
  bounded by a deadline and a backoff cap, applies to effect-free reads only,
  and is logged per retry; the outcome makes every failure's effects explicit.
  The deny-list's "silent retries" hide outcomes; this retry hides none.
- **Invariant 12, one source of truth.** The lock record is the lock step's
  record; the ledger keeps its own revisions; receipts record only the facts
  that bind an effect to this bootstrap (its identity and input) and are
  written once, never updated.
- **Invariant 8, loud integrity failures.** An unattributable state, a foreign
  receipt and a legacy lock all refuse; nothing is inferred.
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
- **Receipts** are new objects under `__cluster/bootstrap/`. Serving,
  deployment and status ignore them. They stay after the claim; nothing reads
  them later.
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
  giving the ledger id a meaning it does not have, to save one rare manual
  recovery. Not taken.
- **Receipts in `state.json`.** The ledger is overwritten by every later
  deployment and has a strict schema owned by the deployment protocol; mixing
  bootstrap progress into it couples two protocols.

## Evidence and tests

The owner is `crates/omnigraph-cluster/src/deployment/bootstrap_fault_tests.rs`,
whose in-memory backend applies real create and if-match semantics and injects
lost answers only after a write committed. A lost answer is now injected as
`StorageError::Unresolved`, as the storage layer reports a conditional write
whose re-issues and read-back did not settle it.

- A same-identity rerun after a lock-then-freshness failure resumes and
  completes, with one lock lineage: one create, one if-match from the exact
  created version, no delete.
- A foreign incarnation, a foreign request id, a version 1 bootstrap lock and a
  serving lock refuse re-entry and leave the lock unchanged; a rerun without
  `Excluded` refuses and changes nothing.
- Transient probe errors are retried to success; a persistent probe error
  before the lock fails at the deadline with `effects: none` and
  `retryable: true` and writes nothing.
- A lost answer on the ledger create and on the accepted ledger revision is
  `unconfirmed`, and a rerun refuses with `bootstrap_step_unconfirmed` without
  touching the lock; a lost answer on a step receipt that landed resumes.
- The existing lost-response, cancellation, claim and release tests pass.

Live S3 qualification of a resumed bootstrap is the managed control plane's.

## Rollout

1. This RFC and the native implementation. `implementation` is
   `in-progress` until the managed control plane passes identity and
   exclusion.
2. The managed control plane passes its signed incarnation and request id,
   asserts exclusion only after it observed the failed process exit, persists
   the outcome, and reruns a `retryable` failure within its admission window.

## Unresolved questions

- Should the attempt bound of 16 be configurable? The managed admission
  window allows about two attempts, so the bound is not reached in practice.

## Decision log

- 2026-11-23: Drafted after the 2026-11-23 bootstrap failure.
