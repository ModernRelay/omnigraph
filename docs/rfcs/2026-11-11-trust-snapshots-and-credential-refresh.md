---
rfc: "2026-11-11-trust-snapshots-and-credential-refresh"
title: "Trust snapshots and credential refresh"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-11-11
updated: 2026-11-11
discussion: https://github.com/ModernRelay/omnigraph/issues/434
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Trust snapshots and credential refresh

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

The server admits a bearer token against one ***trust snapshot***: an
immutable, generation-stamped value derived from one configured credential
source, installed atomically, and bound once per request at admission. Every
credential surface the server has (static tokens, signed data-token trust,
OIDC identity trust) is held this way, through one holder type, one
installation protocol and one failure rule. A ***freshness policy*** is a
property of each source: boot-only, or polled at an interval. What differs
between surfaces is the policy; the mechanism is shared.

The first consequence is the one issue #434 asks for: the static token file
named by `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` can be polled, opt-in, so a
token added to or removed from a mounted Secret takes effect without a
restart. The second is structural: the server's authentication mode is fixed
at boot from what was *configured*, never re-derived from what a snapshot
currently *holds*, so no refresh can weaken authentication. The third is
that the OIDC poller and the data-token trust become instances of the same
holder in later phases rather than a second and third mechanism.

What does not change: the external store stays the only source of truth;
the server keeps hashes in memory and persists nothing; tokens still resolve
to actors at the server boundary; `--unauthenticated` remains the only way to
run without authentication.

## Motivation

The server reads its static bearer tokens once, in `serve_config`, hashes
them into an immutable `Arc<[(BearerTokenHash, Arc<str>)]>` on `AppState`
and never consults the source again
(`crates/omnigraph-server/src/lib.rs`, the `resolve_token_source().load()`
call and the `bearer_tokens` field). Adding a token answers 401 until the
process restarts; removing one keeps admitting it. This holds for every
static source: `OMNIGRAPH_SERVER_BEARER_TOKEN`, `_JSON`, `_FILE` and
`_AWS_SECRET`. The `TokenSource` trait carries a `supports_refresh()` hook
that nothing calls (`auth.rs`). Issue #434 reproduced this on 0.8.1, 0.13.0
and main.

The reporter runs a shared multi-tenant server on Kubernetes with a per-user
token map synced from Vault into a mounted Secret; the map changes on every
onboarding and offboarding. Policy already changes live through
`cluster apply --target`, so the token file is the last change that forces a
restart. Their data tier is single-replica with a `Recreate` strategy
because the storage format is strict single-writer, so each restart is a
short outage of the graph. A restart also interacts with the serving lock:
SIGHUP is not handled, kills the process with exit 129 and leaves
`__cluster/lock.json` behind, so the next start needs `cluster force-unlock`
(#983).

An issue-level fix, polling one file, would be the fifth freshness model on
one `AppState`:

| Surface | Today |
|---|---|
| Static tokens (`bearer_tokens`) | boot-only |
| Signed data-token trust (`--data-token-trust`) | boot-only; retiring a key needs a restart ([RFC 0053](0053-offline-data-token-verification.md)) |
| OIDC identity trust (`--oidc-identity-trust`) | a dedicated 5 s poller with its own revision gate, expiry, warning and no observability (`oidc_identity.rs`) |
| Policy | live, versioned and audited, through the cluster ledger |

Three of the four fail the same property: **admission is a function of
process start time, not of the configured source's current content.** The
pipeline of further sources is visible (Vault, the Kubernetes API, OAuth and
managed access in #1025, delegated credentials in #978, live data-token key
retirement). Each would otherwise bring its own trigger and its own failure
rules. The decision lens in [invariants](../dev/invariants.md) asks what the
single source of truth is after five more changes of this kind; the answer
has to be one holder with per-source policy, not one mechanism per source.

The runtime RFC
([Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md))
keeps credential and trust configuration fixed and lists live
credential/trust replacement as needing a separate decision. This RFC is
that decision. It does not amend the runtime RFC, which only defers the
question.

## User and operational behavior

### Static tokens

Static-token behaviour is unchanged until an operator opts in:

```sh
OMNIGRAPH_SERVER_BEARER_TOKENS_FILE=/etc/omnigraph/tokens/tokens.json \
OMNIGRAPH_SERVER_BEARER_TOKENS_REFRESH_SECONDS=5 \
omnigraph-server --cluster s3://customer/root
```

`OMNIGRAPH_SERVER_BEARER_TOKENS_REFRESH_SECONDS` enables refresh of the
static source and sets its interval. Unset or `0` keeps today's boot-only
contract. The accepted range for a file is 1 to 3600. A value with no
`OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` configured refuses startup, in the
same way an AWS secret id refuses a binary built without `--features aws`:
process environment cannot change in a running process, so there is nothing
to refresh. When refresh is on, startup logs
`static credential refresh enabled` with the source name and interval.
Older binaries ignore the variable silently, so that line is how an operator
confirms a binary refreshes before relying on it.

With refresh on, the server applies a change to the file within one interval
of the change becoming visible at the path it reads:

- an actor added to the file is admitted by its token on every request
  admitted after the change is applied;
- an actor removed from the file is refused with 401 and
  `WWW-Authenticate: Bearer error="invalid_token"` on every request admitted
  after the change is applied; requests the server admitted before that
  finish, which is the request-admission rule signed tokens already follow
  ([RFC 0053](0053-offline-data-token-verification.md));
- a token whose value changed behaves as a removal of the old value and an
  addition of the new one.

A file that fails to read or validate never changes the installed snapshot.
That covers a missing or unreadable file, a non-regular file, a file over the
size bound, invalid JSON, a blank actor or token, and a duplicate actor or
token value. The last good snapshot stays in force until a valid file is
read. There is no expiry on it: a token file is state, not a time-limited
assertion. The consequence is stated plainly to operators: **while the file
is invalid, revocation stops.** The server logs one warning per distinct
invalid content, naming the source and the error class and never token text,
instead of one per tick.

A valid file holding no entries is a valid state. It is installed, refuses
every static token, and is audited as a change that removed every actor,
with a warning that the static principal set is empty. Authentication is
not turned off by it (next section).

Every applied change is logged once at `info`: the new generation, the first
eight hexadecimal digits of the snapshot digest, the actor count, and the
actor ids added and removed. Actor ids are not secrets; tokens never appear.

Confirmation needs no new API. An operator's hook calls `GET /access` with
the newly added token and expects 200, or with the removed token and expects
401; both are decided by the installed snapshot at admission.

`OMNIGRAPH_SERVER_BEARER_TOKEN` and `_JSON` are process environment and are
never refreshed. When `_TOKEN` is set beside `_FILE`, the `default` actor is
fixed for the process and the file's entries refresh around it, matching how
`server_bearer_tokens_from_env` combines them today (`settings.rs`).

`OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` takes the same variable in a
later phase, with a minimum interval of 60 s: each poll is one
`GetSecretValue` request, which is billed and shares a region-wide request
quota with every other caller in the account. A throttled or failed request
keeps the last good snapshot like any other refusal.

### Kubernetes

The documentation gains three facts an operator must know:

- mount the Secret as a directory, never with `subPath`; a `subPath` mount
  never receives updates, so polling cannot see a change there;
- the kubelet applies a Secret update after its own delay (its sync period
  plus cache propagation), which adds to the one-interval bound above;
- write a complete file and swap it atomically; the kubelet's projected
  volume already does this through a `..data` symlink swap. A partial write
  reads as invalid content and is refused, which keeps the last good set.

### Authentication mode

The server decides at boot whether it requires bearer authentication, from
what was configured: a credential source, an applied policy, or neither with
`--unauthenticated` (`classify_server_runtime_state`, `settings.rs`). That
decision does not move. A refreshed snapshot can change *which* tokens are
admitted; it can never change *whether* a token is required. Today
`requires_bearer_auth` re-derives the answer per request from the live set
(`lib.rs`), which is correct only because the set is immutable. Under this
RFC it consults the boot classification together with live policy presence,
which is the ledger's door and can only add a requirement. No existing
deployment observes a difference: the server already refuses to boot with a
configured source that yields no tokens, so the live set is non-empty
whenever the boot classification says a source exists.

### Other credential surfaces

OIDC identity trust keeps its contract from the
[identity RFC](2026-09-09-identity-credentials-and-applied-policy.md): a
5 s poll, a monotonic `revision`, a fixed binding, and a last good snapshot
usable only until its own `expires_at`, after which OIDC requests get 503.
Only its mechanism moves onto the shared holder. Data-token trust stays
boot-only until the phase that gives it a polled policy; that phase amends
the RFC 0053 sentence that retiring a key requires a restart.

### What stays out

No new HTTP route or field. No SIGHUP handler: polling solves the mounted
Secret case without anything signalling the pod, and a signal or an
authenticated endpoint can be added later as another trigger that calls the
same installation function. Until one exists, SIGHUP kills every released
binary; the documentation says so.

## Design

### One holder, per surface

```text
TrustHolder<S: TrustSet>
  installed: ArcSwap<Installed<S>>
  source:    Box<dyn TrustSource<S>>         // load() -> raw bytes
  policy:    Freshness                        // BootOnly | Polled { interval }
  refused:   Option<Digest>                   // raw digest of the last refused content

Installed<S>
  set:        S                               // hashed entries, no plaintext
  generation: u64                             // 1 at boot, +1 per installed change
  digest:     Digest                          // canonical digest of `set`
  raw_digest: Digest                          // digest of the bytes it was parsed from
  applied_at: SystemTime
```

Each credential surface on `AppState` becomes a `TrustHolder` over its own
set type: `StaticTokens` (actor → token hash), `DataTokenTrust` and
`OidcSnapshot`. A request calls `holder.load()` once, at admission, and
authenticates against that `Arc<Installed<S>>` for its lifetime. The constant
time scan in `authenticate_bearer_token` is unchanged; it iterates the loaded
set instead of the field.

The authentication mode is a separate immutable field on `AppState`, set
from `classify_server_runtime_state` at boot. `requires_bearer_auth` is
`mode.requires_credential() || registry.snapshot_ref().any_policy()`.
`has_credential_source` reads the mode.

### One installation protocol

Every refresh, on every surface, runs the same steps in `TrustHolder::refresh`:

1. **Read.** Open the configured path again (so a symlink swap is seen), require
   a regular file, read at most `MAX_BYTES + 1` bytes. The static source
   adopts the OIDC reader's bounds (`oidc_identity::read_bytes`); its file
   reader today has neither bound (`settings::read_bearer_tokens_file`).
2. **Raw short-circuit.** SHA-256 the bytes. If it equals the installed
   snapshot's `raw_digest` or the `refused` digest, stop. Same bytes imply
   the same validated set, so this is a sound short-circuit, and it is the
   common case: one small read and one hash per tick, no parse, no log line.
3. **Validate.** Parse and run the surface's validator. Static tokens use
   `validate_bearer_tokens` for every source, which closes the gap that the
   AWS payload parser skips the duplicate-token check that the env/file path
   performs (`auth::parse_json_secret_payload` against
   `settings::validate_bearer_tokens`).
4. **Hash and digest.** Hash each token; discard the plaintext. The canonical
   digest is the SHA-256 of the sorted `(actor, token_hash)` pairs.
5. **Policy gate.** The surface's freshness policy may refuse the candidate:
   OIDC refuses a changed binding, a lower or replayed revision. Static
   tokens have no gate beyond validation.
6. **Install.** If the canonical digest equals the installed one, record the
   new `raw_digest` and stop: a cosmetic rewrite (whitespace, key order)
   bumps nothing and logs nothing. Otherwise compare-and-swap a new
   `Installed` with `generation + 1`, clear `refused`, and log the audit
   line. A failed compare-and-swap (a concurrent installer won) is retried
   from step 1 once and then refused.

A failure at any step before 6 leaves `installed` untouched, records the
raw digest in `refused`, and logs the warning once for that digest. There is
one place where a trust set changes, and it is this function. Triggers, now
the interval timer and later a signal or an endpoint, call it and nothing
else.

### Why two digests

The raw digest is the cheap path and is sound: equal bytes cannot validate to
different sets. The canonical digest is the identity: it decides whether a
change is a change, so that a rewrite that moves no token produces no
generation and no audit noise. Neither is `stat`. A token rotated to another
value of the same length gives a file of the same size, and a copy that
preserves mtime makes it invisible to a stat-based detector; the issue's
triage measured that case. A stat pre-check would save reading a few
kilobytes every few seconds, which does not buy a detector that can miss a
rotation.

### Freshness as a property of the source

A source declares its policy when it is constructed, and the constructor is
where configuration is refused: `EnvSource` admits only `BootOnly`;
`FileSource` admits `BootOnly` or `Polled` within `[1 s, 3600 s]`;
`SecretsManagerSource` admits `Polled` within `[60 s, 3600 s]`. Setting an
interval on a source that cannot refresh does not construct, which is the
startup refusal described above. `supports_refresh()` is removed; the policy
replaces it.

The poller is one task per polled holder, `tokio::time::interval` with
`MissedTickBehavior::Skip`, running `refresh` on the blocking pool, exactly
as `oidc_identity::start_refresh` does today. It holds a `Weak` to the holder
and exits when the holder drops.

### What is inherited and what is added

Nothing here touches Lance or the engine. Everything is in
`omnigraph-server`: the holder, the installation function, the source
constructors, the mode field, and the timer. The `TokenSource` trait
narrows to `load()`.

## Invariants

- **3, one coherent accepted view.** A request binds one `Installed`
  snapshot at admission and uses it for its lifetime. Revocation applies at
  admission; admitted work finishes. This is already the rule for signed
  tokens and OIDC; the holder makes it the rule for static tokens too.
- **8, loud integrity failures.** Invalid content is refused with a logged,
  classed warning, never installed and never silently dropped. A
  misconfigured interval refuses startup.
- **10, trust at the boundary, enforced at the engine.** Tokens still
  resolve to actors at the server; the client supplies nothing. Plaintext
  is discarded after hashing in step 4 and never retained. The mode is
  fixed at boot, so a refresh cannot make the boundary admit an actor-less
  request that it refused before.
- **11, bounded and observable.** Reads are size-bounded; polls are
  interval-bounded and skip missed ticks rather than queueing them; the
  AWS source has a floor that bounds billed requests; refusals log once per
  distinct content. Per-request cost is one atomic load.
- **12, one source of truth, cheaply derived.** The external store (file,
  Secret, Secrets Manager) is authoritative. The snapshot is a derived
  in-memory view; nothing is persisted, so nothing can drift or need
  reconciliation.

Deny-list: the poller is an idempotent reconciler over an external source,
the shape the list permits in place of a queue; there is no retry that is
silent, no partial installation, and no parallel truth. Storing tokens or
their hashes in the cluster ledger is rejected below precisely because it
would create one.

## Compatibility and reversibility

No storage, wire or protocol change. The only new configuration is one
environment variable, off by default. A deployment that does not set it
observes nothing new. A deployment that sets it and later downgrades to a
binary without this RFC returns silently to boot-only behaviour; the
documentation names the startup log line as the check, and operators who
depend on refresh should gate rollout on it.

The mode change has no observable effect on existing deployments because the
boot classifier already refuses a configured source that yields no tokens.

Reverting is cheap: delete the holder and the timer, restore the immutable
field. There is no persisted state to migrate either way.

## Alternatives

| Option | Why not |
|---|---|
| Restart (status quo) | An outage per onboarding under single-writer `Recreate`, and the lock interaction in #983. |
| Poll the one file, leave the rest | Fixes the reported shape and leaves the property broken on two surfaces; the fifth freshness model on one struct. |
| `SIGHUP` as the trigger | Needs permission to signal the pod (`shareProcessNamespace` or an exec hook); the stock image runs the server as PID 1, where an unhandled signal is dropped; kills every released binary today. Reasonable as a later trigger onto the same function, wrong as the contract. |
| An authenticated admin endpoint | New public API, OpenAPI and HTTP-contract surface, and an operator whose own token was just rotated cannot call it. Also a later trigger at most. |
| `inotify`/file watchers | Platform-specific, miss symlink swaps unless watching the directory, and still need a poll as fallback. |
| `stat` (mtime/size) as the detector | Misses a same-length rotation copied with preserved mtime; measured in the issue. |
| Default-on polling | Changes the revocation-latency contract of every existing deployment at upgrade. |
| Tokens or hashes in the cluster ledger, activated by `cluster apply` | One activation door, but it puts credential material at rest in graph storage, breaks the Vault, Secret and Secrets Manager workflows, and makes a second source of truth against the external store. Rejected in the issue's triage. |
| OIDC for people (shipped) | Live admission already, but it needs an RS256 issuer with tokens of at most 300 s; it does not replace a static per-user map. |
| Short-lived delegated credentials (#978, #1025) | The right direction; years of other people's infrastructure away. This holder is what makes them another `TrustSource`, not another mechanism. |

## Evidence and tests

Extend existing owners; add no fixture. The `Fix Regression Gate` keys on
`issue_434` in a test name.

- **`crates/omnigraph-server/src/auth.rs` tests** (the token-source owner).
  Replace `env_or_file_source_does_not_support_refresh` with holder tests
  that call `refresh` directly, in the shape of `oidc_identity_tests.rs`:
  invalid, missing, non-regular, oversized, empty-bytes and duplicate files
  keep the installed snapshot and record `refused`; unchanged bytes do
  nothing; a cosmetic rewrite bumps no generation; a rewrite with the same
  size and mtime is installed; a `..data` swap is installed; a valid empty
  object is installed and audited; the AWS payload goes through
  `validate_bearer_tokens`; an interval on an env source does not construct;
  intervals outside each source's range do not construct.
- **`crates/omnigraph-server/src/settings.rs` tests.** The mode is fixed
  from the classifier and `requires_bearer_auth` does not read the live set:
  install an empty snapshot on a `DefaultDeny` server and assert that an
  actor-less request is still refused.
- **`crates/omnigraph-server/tests/boot_settings.rs`, `owned_shutdown`
  module.** One process test, `static_token_file_refresh_issue_434`, which
  runs the real binary with a 1 s interval and a `..data` swap and probes
  `GET /graphs/{id}/access`: the added token is admitted, the removed one
  gets 401, an invalid file keeps the last good set and a valid file after it
  is installed. This is the cheapest tier that runs the real timer, a file
  change and bearer authentication together; the timer starts in
  `serve_config`, which in-process tests do not run, and GQT cannot express
  any of it.
- **`oidc_identity_tests.rs`**, at the phase that moves OIDC onto the
  holder: its existing cases pass unchanged, which is the proof that the
  policy is preserved.
- **Docs.** `scripts/check-docs.py` for the RFC and registry; the
  `omnigraph skill export` byte-identity owner in `cli_schema_config` for
  the skill line.

## Rollout

1. **Holder, mode, static file refresh.** `TrustHolder`, the installation
   function, the boot-fixed mode, `FileSource` with `Polled`, the variable,
   the audit line, the tests above, and the documentation: per-source reload
   behaviour in `docs/user/operations/server.md` (which also stops saying
   "Choose one static token source", since `_TOKEN` and `_FILE` combine),
   the Kubernetes notes in `docs/user/deployment.md`, the sentence in
   `docs/user/clusters/index.md` that credential configuration stays fixed,
   and one line in `skills/omnigraph/references/server-policy.md`. A
   `changelog.d/<slug>.added.md` note. `implementation: in-progress`.
2. **AWS Secrets Manager on the holder.** `SecretsManagerSource` with
   `Polled` at a 60 s floor, throttle handling as refusal.
3. **OIDC onto the holder.** Mechanism only; `revision`, binding and expiry
   policy unchanged. `implementation: partial`.
4. **Data-token trust refresh.** `Polled` for `--data-token-trust`, a
   disposition note under RFC 0053 replacing the sentence that retiring a
   key requires a restart. `implementation: complete`.

Each phase ships on its own. Nothing is unavailable between them that is
available today.

## Unresolved questions

- Should an invalid static file ever time out its last good snapshot? This
  RFC says no: a token file is state, and the reporter asked for indefinite
  last-good. The alternative is an optional
  `..._MAX_STALE_SECONDS` after which static admission fails closed as OIDC
  does. Not needed for acceptance of the holder; it would be another
  freshness policy on the same mechanism.

## Decision log

- 2026-11-11: drafted from the issue #434 triage of 2026-10-17 and its
  follow-up of 2026-11-10, verified against main `b125d97a`.
