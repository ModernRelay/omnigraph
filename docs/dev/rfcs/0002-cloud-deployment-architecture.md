# RFC 0002 — Cloud Deployment Architecture (Staged)

**Type:** design proposal
**Status:** draft — not accepted, not implemented
**Audience:** maintainers reviewing the cloud offering and the OSS/Cloud boundary
**Date:** 2026-05-17
**Depends on:** [RFC 0001 — Federated Authentication](0001-federated-authentication.md)

> This is a proposal, not current truth. Until accepted and implemented, the
> authoritative deployment story remains [docs/user/deployment.md](../../user/deployment.md).

## Summary

Defines how OmniGraph is deployed as a managed cloud offering, in **three
stages** of increasing complexity. Each stage wins one irreducible property
and pays only the complexity that property earns:

1. **Managed single-region** — a customer can sign up, get a repo, and
   authenticate against a managed OmniGraph. Wins: *managed + authenticated +
   multi-tenant*.
2. **Elastic data plane + worker tier** — write scale, and maintenance
   (indexing, compaction, recovery) moved off the request path. Wins: *scale +
   off-path maintenance + no recovery-on-open race*.
3. **BYOC / VPC / air-gapped** — data plane in the customer's VPC, only a
   thin orchestrator in the vendor cloud. Wins: *data sovereignty*.

The same OSS binary runs in every stage and in a customer VPC; deployment mode
is configuration. The auth design from RFC 0001 threads through unchanged —
each stage only moves *where the token issuer lives*, never the validation
path.

## Motivation

OmniGraph's durable state lives entirely in object storage (Lance datasets +
the `__manifest` commit log); concurrency is optimistic CAS on `__manifest`.
That is already the object-storage-native architecture that turbopuffer,
LanceDB, Neon, and WarpStream converged on. The task is not to adopt it but to
*lean into it* — and to do so in stages so that a managed offering can ship and
collect real load before the expensive pieces (the reconciler, BYOC) are built.

## Goals

- One OSS binary; deployment mode is configuration only.
- A managed offering reachable in Stage 1 without building the reconciler.
- Object storage as the only request-path dependency.
- The control plane is dispensable: it holds only soft, derivable state.
- Auth (RFC 0001) is identical across stages and across VPC/on-prem.

## Non-goals

- A new storage substrate, WAL, or metadata database (deny-list).
- Changing the engine crates — transport/auth stay at the server boundary
  (Invariant 11).
- Multi-region active/active. Regions are independent stacks.
- Browser SSO / login UX (a control-plane concern, out of scope here).

## Foundational principles (hold across all stages)

These are decided once and constrain every stage.

### P1 — Object-storage-only commit

OmniGraph writes are **batch-shaped** (`mutate_as`, `load`, merges,
`schema_apply`) — not OLTP `COMMIT`s. Neon adds a Safekeeper quorum tier in
front of object storage *because* OLTP commit cannot wait ~100-300ms for an S3
PUT. OmniGraph has no such requirement, so it takes the turbopuffer path:
**commit straight to object storage, accept ~100-300ms write latency, run no
fast durable tier.** This is a large, deliberate simplicity win. It is
hard to reverse — if a low-latency single-row write path ever becomes a product
requirement, *that* is when a fast durable tier earns its complexity, via a new
RFC.

### P2 — The control plane holds only soft state

WarpStream keeps an authoritative metadata store (file→offset mappings) in its
cloud. OmniGraph does **not** need this: the `__manifest` table already *is* the
authoritative, strongly-consistent metadata, and it lives in object storage.
The control plane therefore stores only **soft, derivable state** — tenant
directory, billing counters, routing hints, compaction schedules, recovery
leases. Everything it knows is rebuildable by scanning object storage. The
control plane is never on the request path; if it is down, existing tenants
keep serving (the turbopuffer 99.99%-uptime property).

### P3 — One binary, config-driven

The `omnigraph-server` container is identical in Stage 1, Stage 2, a customer
VPC, and air-gapped on-prem. A "cloud build" is configuration plus *additive,
optional* control-plane services — never a fork (deny-list: no Cloud fork;
correctness is always OSS).

### P4 — Auth validation never makes a network call

Per RFC 0001: tokens are validated offline against cached JWKS. The token
*issuer* may be cloud-hosted, but the data plane never calls it on the request
path. This is what lets the identical data plane run in Stage 3's customer VPC.

## Architecture primitives

```
        CONTROL PLANE (vendor cloud; soft state only; off request path)
        - provisioning / tenant directory   - billing / metering
        - identity issuer (RFC 0001; may wrap WorkOS)
        - orchestration: compaction schedule, recovery leases, routing hints
                              │  (async, never on request path)
   ┌──────────────────────────┼──────────────────────────────────────┐
   │  DATA PLANE — omnigraph-server (identical OSS binary everywhere)  │
   │    read replicas  ·  writer(s)  ·  worker tier (Stage 2+)        │
   └──────────────────────────┬──────────────────────────────────────┘
                              ▼
                       OBJECT STORAGE
        Lance datasets + __manifest  (the only request-path dependency)
```

Tiers, introduced progressively:

- **Read replicas** — open `OpenMode::ReadOnly` (skips the recovery sweep),
  snapshot-isolated, fan out freely.
- **Writer(s)** — open `ReadWrite`; route by repo so CAS contention and cache
  stay local.
- **Worker tier** (Stage 2+) — background indexing, compaction, cleanup,
  recovery. Off the request path.

## Stage 1 — Managed single-region

**Property won:** a customer can sign up, get a repo, authenticate, and use a
managed OmniGraph.

**Architecture.** Single region. One object store (prefix-per-tenant or
bucket-per-tenant — see open decisions). Data plane = a pool of **read
replicas** plus a **single writer replica** per region. The single writer is
deliberate: it sidesteps both `__manifest` CAS contention *and* the
recovery-on-open race **without building the worker tier**. Recovery runs on the
writer's `open`, as today. Reads fan out across read replicas.

**Control plane.** Thinnest viable: provisioning (`open-or-create` a repo —
largely doable by the data plane itself on first request), a tenant directory,
billing counters, and the RFC 0001 identity issuer. All soft state (P2).

**Auth (RFC 0001).** `mode = static` remains the default for M2M / CI;
`mode = oidc` available, validated offline. The control plane runs the issuer
(its own, or wrapping WorkOS for human SSO). `hybrid` lets both coexist.

**Branching as product surface.** OmniGraph already has Git-style graph
branches with lazy fork — the same zero-copy, metadata-pointer design Neon
sells. Stage 1 exposes this directly: instant per-PR / dev / staging branches at
near-zero storage cost. No new engine work — a product packaging of an existing
capability.

**Deliberately not done.** No autoscaling of writes, no worker tier, no
reconciler, no BYOC. **Accepted limitation:** per-region write throughput is
bounded by one writer; a writer restart briefly pauses writes for that region.

**Exit criteria → Stage 2.** Single-writer throughput, write-pause blast
radius, or maintenance load (inline index builds / compaction) becomes the
binding constraint.

## Stage 2 — Elastic data plane + worker tier

**Property won:** horizontal write scale, and maintenance moved off the request
path — which also eliminates the recovery-on-open race.

**Architecture.**

- **Multiple writers** with **consistent-hash routing by repo URI**. A repo's
  writes land on one node, so CAS contention is bounded and the Lance page cache
  / warm `Omnigraph` handle stay local.
- **Per-repo write coalescer** — concurrent `mutate_as`/`load` commits to one
  repo batch into one manifest publish (the turbopuffer WAL-batching lesson:
  beat contention with batching, not locks).
- **Three-tier cache** made explicit: object storage → NVMe SSD → in-process
  (Lance page cache + warm handle), with routing affinity keeping a repo warm.
- **Worker tier** — background workers own index building (the deny-list
  reconciler mandate), compaction (`optimize`), cleanup, **and recovery**.
  Recovery moves from "every `open` runs the sweep" to "one leased worker per
  repo owns recovery." This *is* the long-deferred background reconciler;
  cloud is its forcing function.
- **Per-tenant resource bounds** — close the `invariants.md` resource-bounds
  gap: enforced per-query memory/time budgets, plus `WorkloadController`
  admission control, so multi-tenant compute has no noisy-neighbor failure.
- **Scale-to-zero** for cold tenants — evict idle handles, re-warm on first
  request, bill by the second (the Neon model).

**Control plane.** Gains orchestration: routing-hint distribution, compaction
scheduling, recovery-lease coordination. Still soft state (P2), still off-path.

**Auth.** No change to the validation path. The control plane's config-bundle
sync (RFC 0001 `ControlPlaneSync`) may now feed a SCIM-sourced actor allowlist.

**Optional consistency knob.** With warm caches, a per-query `stale-ok` read
becomes viable (turbopuffer's sub-10ms eventual mode). Invariant 6 permits it
**only** as explicit, read-only, non-default — exposed as opt-in, never the
default.

**Deliberately not done.** Data still resides in vendor-managed object storage.

**Exit criteria → Stage 3.** A customer requires data sovereignty (data may not
leave their account) or air-gapped operation.

## Stage 3 — BYOC / VPC / air-gapped

**Property won:** data sovereignty — the customer's graph data never leaves
their cloud account.

**Architecture.** The WarpStream BYOC split. The data plane (read replicas,
writers, worker tier — the Stage 1 *or* Stage 2 shape) and the customer's object
store run **inside the customer's VPC**. The vendor cloud keeps only the
soft-state orchestrator and the identity issuer. No customer graph data crosses
the boundary; no cross-account IAM into the customer's bucket. Air-gapped is the
same packaging with the control plane absent and config supplied as static
files.

**Auth.** This is where RFC 0001's P4 pays off fully: the in-VPC data plane
validates tokens **offline** against cached JWKS. The vendor identity issuer is
the only cloud touchpoint and it is off the request path. Air-gapped: point at
the customer's own IdP, or `mode = static`, with JWKS/policy pre-seeded.

**Why this is mostly packaging.** Because P2/P3/P4 were honored from Stage 1 —
control plane thin and off-path, one config-driven binary, auth validated
offline — Stage 3 is boundary hardening and deployment templates (Helm /
Terraform), not an architectural change.

## Why three stages (first-principles)

- **Not one stage.** The managed offering must not wait on the reconciler — a
  large build. Stage 1's single-writer design wins a real, sellable, managed
  product with bounded complexity, and the load it collects is the evidence
  that justifies Stage 2 (reversible-change discipline: ship, measure, then
  invest).
- **Not collapsing 2 and 3.** *Scale* (Stage 2) and *sovereignty* (Stage 3) are
  independent axes — a customer may demand BYOC before single-region scale runs
  out, or the reverse. They share only the thin-control-plane prerequisite,
  which is foundational (P2) anyway. **Stage 3 can therefore ship on the Stage 1
  data-plane shape**; the 1→2→3 numbering is the expected-demand order, not a
  hard dependency. If enterprise/sovereignty demand arrives first, do 1→3→2.
- **Not more than three.** The natural seams are exactly *managed+auth*,
  *elastic+maintenance*, *sovereignty*. Finer splits would be invented
  complexity, not earned.

## Open decisions

1. **Tenancy isolation model** — bucket-per-tenant vs prefix-per-tenant vs
   account-per-tenant. Strongest lever and effectively irreversible; the
   control plane should vend short-lived per-tenant scoped credentials
   regardless. Decide before Stage 1.
2. **Recovery ownership** — Stage 1 leans on the single writer; Stage 2 needs a
   per-repo recovery lease. Confirm the lease mechanism (object-store-based
   lease vs control-plane-issued).
3. **Commit-latency model (P1)** — ratify object-storage-only commit, or
   identify a concrete low-latency write requirement that would justify a fast
   durable tier.
4. **RFC 0001 carry-overs** — degraded-mode JWKS grace window, revocation
   strategy, whether VPC customers can override cloud-pushed Cedar policy.
5. **Stale-read knob** — ship the optional eventual-consistency read in
   Stage 2, or defer.

## Invariant analysis

| Invariant / deny-list item | Outcome |
|---|---|
| 2 — manifest-atomic graph visibility | ✅ unchanged; `__manifest` CAS is the commit point in every stage |
| 5 — recovery part of commit protocol | ✅ Stage 1 = open-time sweep; Stage 2 = leased worker; never weakened |
| 6 — strong consistency default | ✅ stale-read knob is explicit, read-only, non-default |
| 11 — transport/auth at the boundary | ✅ engine crates untouched; auth in `omnigraph-server` |
| 13 — failures bounded/observable | ✅ Stage 2 closes the per-query resource-bounds gap |
| Deny: custom WAL / metadata store | ✅ P1/P2 — object storage + `__manifest` only |
| Deny: cloud-only correctness / fork | ✅ P3 — one OSS binary, additive control plane |
| Deny: job queue for manifest-derivable state | ✅ worker tier is a reconciler, not a queue |

## Testing notes

- Stage 1: extend `omnigraph-server` tests for multi-replica read fan-out and
  single-writer routing; reuse `failpoints` for writer-restart behavior.
- Stage 2: per-repo coalescer and routing need engine/storage-boundary tests
  (`runs.rs`, `recovery.rs`); recovery-lease coverage belongs in `recovery.rs`.
- Stage 3: a deployment-template smoke test (data plane against an in-VPC-style
  object store); confirm no control-plane call on the request path.
- Update [docs/user/deployment.md](../../user/deployment.md),
  [docs/user/server.md](../../user/server.md) as each stage lands.
