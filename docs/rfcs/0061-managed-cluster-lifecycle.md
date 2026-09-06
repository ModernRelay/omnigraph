---
rfc: "0061"
title: "Managed cluster lifecycle and config preparation"
track: maintainer
status: accepted
implementation: complete
authors:
  - Andrew
created: 2026-09-06
updated: 2026-09-06
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0061: Managed cluster lifecycle and config preparation

## Summary

Extend the managed CLI with service cluster creation, deletion, undo and
referenced-file config preparation. The Intent API owns these workflows and
their authorization; the CLI sends exact requests and preserves the response's
identity, canonical operation outcome and separate lifecycle progress.

This extends the deliberately deferred provisioning and managed-store editing
scope of [RFC 0052](0052-managed-control-plane-cli.md). It changes no engine,
Lance, direct cluster execution, data credential or authentication contract.

## Motivation

A managed customer can operate an adopted cluster but cannot request a fresh
one, update its managed source, or retire it through the native CLI. Administrator
provisioning scripts do not constitute a customer interface. These operations
need exact identity, recoverable uncertain responses and durable service
operations rather than local storage effects or unbounded polling.

## User and operational behavior

`cluster create NAME --api ORIGIN --config DIR` requests an empty managed
cluster and binds the config directory to the returned identity. An existing
context is never overwritten. `--no-wait` returns the accepted operation;
otherwise the command waits up to 300 seconds, configurable from 1–3600 using
`--timeout`. Readiness means the API's ordinary plan/apply and serving witness
completed; acceptance alone does not mean ready or network reachable.

`cluster push --expected-revision COMMIT --message MESSAGE` captures only
`cluster.yaml` and referenced schema, stored-query and policy files from the
selected folder and uploads them with a conditional managed Git revision.
It does not apply. The caller next uses `cluster plan --rev COMMIT` and exact
`cluster apply --plan RUN`. No Git provider credentials or bucket access are
required by the CLI.

`cluster delete --incarnation INCARNATION` requires exact targeting and current
scoped delete permission. `--retention-seconds` accepts 0–2,592,000 and defaults
to 86,400. With a nonzero interval, ordinary waiting stops at the confirmed
tombstone and reports its deadline and operation ID while preserving canonical
state `running`; zero waits toward final purge. `--no-wait` and `--timeout`
provide the same bounded alternatives as creation. There is no additional
approval or confirmation workflow.

`cluster undo-delete --incarnation INCARNATION --deletion-id OPERATION` requests
undo of that exact deletion. The service requires current delete, plan and apply
permissions, checks the retained undo window, and restores through the normal
plan/apply workflow. The CLI does not recreate a retired identity.

`cluster status --operation OPERATION` reads a service operation. `--api ORIGIN`
allows recovery before any folder context exists; otherwise the exact folder
context selects the origin and cluster. `--wait --timeout SECONDS` explicitly
polls to a canonical outcome. A local deadline returns exit 5 with the latest
response and operation ID; it does not cancel service work. Existing canonical
exit mappings remain those of RFC 0052. A deleted folder keeps its context for
history and operation status; deletion does not select direct access.

## Design

Creation uses `POST /v1/clusters`; delete and undo use the cluster's `:delete`
and `:undo` routes; upload uses `/config`; operation polling uses
`GET /v1/operations/ID`. Returned cluster, incarnation and operation IDs must
agree with the request and provenance metadata before being accepted.

Before each lifecycle submission, the CLI persists a bounded versioned
`.omnigraph/pending-lifecycle.json` containing origin, request path, exact JSON
request SHA256, principal/account identity and idempotency key. Identity comes
from `GET /v1/auth/session` using the same captured bearer as the submission.
Renewed credentials for the same principal can recover; another principal or
account cannot replay that pending request. A matching retry reuses it; an incompatible
pending request or explicit key refuses. A caller may supply
`--idempotency-key`. Keys are printed to stderr, never mixed into JSON stdout.
Only a recognized definitive API refusal on the first attempt clears pending
state. Timeouts, unknown responses and every unresolved retry retain the key;
a later permission refusal cannot disprove earlier acceptance.
After a validated response, `.omnigraph/last-lifecycle.json` durably records its
identity and operation before pending state is cleared. These records contain
no credential and grant no authority. They are recovery aids, not service truth.

Upload reads are bounded to 4096 files, 2 MiB per file and a 32 MiB serialized
request, with normalized relative paths of at most 512 bytes and a 120-second
capture deadline. Query-directory discovery considers only immediate `.gq`
files. Symlinks, special files, traversal, missing references and invalid UTF-8
refuse before submission. The API independently captures and validates the
same referenced set and owns config meaning and the expected-head CAS.
This release qualifies capture on Unix through descriptor-relative traversal;
other platforms refuse managed upload before submission.

All calls reuse the existing origin-bound credential and HTTP transport: HTTPS
or exact loopback HTTP, no redirects, 10-second request and 8 MiB response
bounds. Unsupported API, permission and malformed-context failures never fall
back to direct storage. Lifecycle and operation-only flags refuse with
`--direct`; no direct Core verb is changed.

## Invariants

[Invariants](../dev/invariants.md) 3, 5, 8, 10, 11, 12 and 13 remain intact:
identity is exact, uncertain effects retain recovery identity, responses remain
typed and bounded, authority stays with the authenticated service, and local
records do not become a second authority. There is no new graph publication,
Lance transaction, maintenance rule, writer fencing or online data-plane check.
The Lance domain map therefore has no changed substrate surface.

## Compatibility and reversibility

Folder context remains version 1. Older clients retain plan/apply/status and
direct behavior; they cannot execute the new commands. Service refusal does
not trigger a legacy fallback. Removing this CLI surface does not cancel any
accepted service operation. Private routing still needs its explicit connection;
public routing and a console are outside this RFC.

## Alternatives

Administrator scripts keep the customer path incomplete. Uploading a whole
directory would send unrelated credentials and files; explicit referenced-file
capture bounds the payload. Reusing local delete or direct access would cross
the service's authority and cleanup boundary. Polling until a default 24-hour
retention expires would hide useful accepted progress behind an impractical
local wait.

## Evidence and tests

The existing bounded HTTP fixture in `omnigraph-cli/tests/cli_cluster.rs`
covers exact create/delete/undo requests, operation identity mismatches,
uncertain create replay across timeout and gateway responses, principal/account
changes and session renewal, context preservation, pending-key conflicts,
bounded waits, managed-file capture and no Core effects on refusal.
The clean baseline managed suite passed 15 tests. The completed implementation
passed all 99 CLI unit and 57 cluster process tests on 2026-09-06, including ten
lifecycle process tests, plus strict CLI Clippy, formatting and documentation
checks. The host linker emits the same unwind-size warning as the baseline.
The companion control-plane repository owns authorization, lifecycle storage,
operator cleanup and the real local cell proof; HTTP fixtures do not qualify
cloud provisioning or isolation.

## Rollout

The decision precedes implementation. Ship the bounded CLI commands, public
reference and release notes together after the owned regression suites pass.
Live cloud qualification remains separately owned service evidence.

## Unresolved questions

None for this bounded client increment.

## Decision log

2026-09-06: Accepted the maintainer-requested lifecycle/config increment and
its explicit service authority boundary before code. The control-plane
DEC-08-28 owns the companion service workflow; this RFC owns only its public
CLI contract and compatibility.

2026-09-06: Completed the bounded client implementation and owned local
regressions. Managed upload is qualified on Unix; other platforms refuse it.
The companion service owns cell qualification and cloud rollout separately.
