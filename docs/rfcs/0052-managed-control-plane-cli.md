---
rfc: "0052"
title: "Managed control-plane CLI"
track: maintainer
status: accepted
implementation: complete
authors:
  - andrew
created: 2026-09-05
updated: 2026-09-05
discussion: https://github.com/ModernRelay/omnigraph/pull/629
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0052: Managed control-plane CLI

## Summary

Add an HTTP client for the managed Intent API to the existing CLI. A folder's
`.omnigraph/context` selects managed cluster operations; absence preserves
the existing direct Core path. Managed authentication establishes identity,
and current permissions authorize execution of an exact saved plan. There is
no separate managed approval workflow. Engine behavior and storage formats
are unchanged.

## Motivation

The managed executor, operator, and Intent API can plan and apply a bound
external repository, but users currently need raw HTTP requests. The CLI has
direct cluster commands and per-server token login, neither of which can
substitute for managed identity, durable runs, leases, and signed offers.
Routing and credential ownership are public contracts, so this is an RFC
rather than a local command refactor. The companion control-plane decision
is DEC-08-22 in `ModernRelay/og-control-plane/specs/02-DEC-Decisions.md`.

## User and operational behavior

```sh
omnigraph login --api https://control.example
omnigraph use CLUSTER_ID --api https://control.example
omnigraph cluster plan --config . --json
omnigraph cluster apply --config . --plan PLAN_RUN_ID --json
omnigraph cluster status --config . --json
omnigraph cluster history --config . --json
omnigraph cluster cancel RUN_ID --config . --json
omnigraph logout --api https://control.example
```

Users commit and push external config with git before planning. `plan` uses
the bound head unless `--rev` selects a revision the API accepts. A saved plan
retains the service's change lease. Apply requires that exact `--plan`; it
does not commit files, upload a bundle, silently replan, or ask for another
approval. Current permissions, binding, digests, and lease are checked by the
API and execution authority is independently checked by the executor.

`--direct` explicitly selects existing Core behavior for `--config`; its
`cluster.yaml` still owns the root. There is no root substitution. Without
that flag, malformed context, unknown context version, unsupported managed
commands, provider/API errors, and permission refusals fail closed. No such
failure may invoke Core or consult graph-root credentials.

`--no-wait` returns the accepted run envelope. Otherwise the CLI polls every
two seconds for up to 300 seconds; `--timeout` selects 1–3,600 seconds. Timeout
ends the local wait, not the run. A stable `--idempotency-key` permits explicit
plan/apply replay; an omitted key is generated once and printed to stderr.
Retries within an invocation reuse that key. Cancellation and abandonment
retry by the same run id, without a separate key. Run JSON is the API's envelope,
with its requested/effective/observed and service provenance labels intact.
Progress is stderr. Managed exit codes are 0 success, 1 failed/transport,
2 refused/blocked, 3 partially converged, 4 recovery required, 5 stalled or
wait timeout, and 6 cancelled. Direct exit codes do not change.

`cancel` uses the existing pending-run cancellation API. For a converged
unused plan it calls `:abandon`, which retains the converged result, marks it
unusable, and releases only its lease. Apply reservation and abandonment are
serialized by the API. It never cancels accepted effects through lease expiry.

## Design

The versioned, secret-free context is at most 16 KiB and lives only in the
selected config directory; parent directory search is not performed. Context
must be a regular file and neither it nor `.omnigraph` may be a symbolic link;
a dangling link is a refusal, never absence selecting the Core:

```yaml
version: 1
cluster: CLUSTER_ID
api: https://control.example
```

`use` first verifies access to the cluster, then atomically writes this file.
Unknown fields and malformed values refuse. API addresses are canonical
origins, without userinfo, path, query, or fragment. HTTPS is mandatory except
exact loopback hosts for local integration. Redirects are refused; each
request has a 10-second deadline and 8 MiB body limit.

`login --api` calls the Intent API's device authorization and polling routes.
The service brokers WorkOS's native device flow and reuses its browser login
JWT, membership, enrollment, and grant checks. The CLI prints the verification
URL/user code, never the device secret. It respects expiry and polling
interval, including slowdown, and saves only the returned opaque session in
the OS keychain under the canonical API origin. Sessions live at most 15
minutes and no longer than the provider token. No provider access/refresh
token is stored by the CLI. An unavailable keychain refuses without a
plaintext fallback. Logout revokes the service session and removes the local
entry; session expiry requires a new login. Accepted runs continue normally.

Scoped automation may supply `OMNIGRAPH_CONTROL_TOKEN` only together with
`OMNIGRAPH_CONTROL_API` matching the selected canonical origin. Data-plane
credentials, existing server token files, and actor flags cannot supply
managed human authority. The CLI has no private managed-crate dependency;
the public HTTP envelope is the boundary. No Lance domain is changed or
relied on by this HTTP path.

## Invariants

Invariants 3 and 5 remain owned by the Core/executor: the CLI submits exact
plan identity rather than inventing state or recovery authority. Invariant 8
requires typed context and API refusals instead of fallback. Invariant 10
keeps trusted actor resolution at the API; the client cannot assert human
identity. Invariant 11 supplies numeric request/wait/body bounds. Invariant
12 keeps durable runs and leases service-owned; context is routing, not a
shadow cluster ledger. No deny-list item is introduced: there is no cloud
fork, cloud-only correctness fix, duplicated lock, or new writer fence.

## Compatibility and reversibility

Existing profiles/servers/clusters config and `login SERVER --token` /
`logout SERVER` remain intact. The new `--api` mode is mutually exclusive
with legacy server login. Existing direct commands without context retain
their flags and behavior. Managed-only flags without context refuse.
Unsupported managed commands refuse before direct execution.

No stored graph or server API format changes. Removing the context or using
`--direct` restores the existing direct path intentionally. Older CLI builds
do not understand this context contract and must not be presented as safe
managed clients; deploying the new CLI and API together is the pilot boundary.
The server SDK and Python SDK gain no new authority or dependencies.

## Alternatives

- Raw HTTP keeps the current gap in bounded waits, identity storage, and
  context isolation and makes every script reproduce the managed contract.
- A second top-level verb family avoids dispatch integration but contradicts
  the existing cluster command model and duplicates discoverability/docs.
- Inferring managed mode from storage roots or existing server aliases risks
  forwarding the wrong credential; explicit context and origin binding win.
- Refresh-token persistence adds rotation/revocation/storage behavior beyond
  the bounded pilot. Re-login after expiry keeps the browser contract.
- Plaintext fallback helps headless setup but silently weakens human secret
  storage. Explicit scoped automation credentials cover unattended execution.

## Evidence and tests

The existing CLI unit and cluster process owners cover exact saved-plan and
idempotency bodies, envelope and outcome parity, device pending/slowdown/expiry,
credential-origin isolation, malformed context, redirect and size limits, and
API-down refusal without Core effects. Unit tests inject a credential store;
process tests do not touch user keychains or operator configuration. The
focused baseline passed 91 unit tests and 45 cluster process tests, including
the existing direct behavior.

The control-plane repository owns transactional permission/revocation,
device replay, and abandon/apply race tests plus a kind proof driven by this
actual CLI binary. The passing proof compares CLI/API plan and bundle digests,
terminal outcome, receipts and readiness witness, and tests cancellation,
idempotency and denied access. A separate live WorkOS pilot passed native
device login, real OS keychain storage, exact-plan apply through readiness,
permission removal and restoration for the same session, and logout. This
live evidence covers one identity and a nondestructive plan; it does not
replace deterministic provider fixtures or claim a full browser/two-user
qualification matrix.

## Rollout

Land the public contract and implementation together with CLI user docs and
release notes. The companion API adds device sessions and plan abandonment
without changing executor or engine gates. Existing CLI/direct and managed
HTTP tests must pass before recommending the managed client. Data-plane
tokens, managed-store editing, provisioning, console, SSE, and persistent
refresh tokens remain out of scope.

## Unresolved questions

None for this bounded increment.

## Decision log

2026-09-05: Recorded the requested managed CLI increment before code. Reused
the existing config-directory addressing contract with explicit `--direct`
rather than introducing a root-URI override the Core does not currently expose.

2026-09-05: Accepted after maintainer-authorized review of the implementation,
compatibility, bounded HTTP and credential handling, focused regression
suites, the full native CLI kind proof, and the separate live WorkOS pilot.
No engine or storage contract changes are required. Data access remains a
separate increment; this decision authorizes control-plane operations only.
