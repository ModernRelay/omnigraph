# RFC: Unify the CLI's Embedded and Remote Access Paths

**Status:** Proposed
**Date:** 2026-06-12
**Audience:** engine/CLI/server maintainers
**Builds on:** [rfc-007-operator-config.md](rfc-007-operator-config.md)
(landed — `--server` targeting and operator aliases are remote-addressing
surfaces the unified client must treat as first-class),
[rfc-008-deprecate-omnigraph-yaml.md](rfc-008-deprecate-omnigraph-yaml.md)
(stages 1–4 landed — the config-authority demotion this RFC's earlier
drafts promised as a "companion" already happened; the remaining sliver,
removing the yaml-mode server boot source, is RFC-008 stage 5 and is
*eased* by Phases 4–5 here), [rfc-002-config-cli-architecture.md](rfc-002-config-cli-architecture.md)
(umbrella; see Prior Art for what is salvageable from its parked
implementation).
**Sequencing:** post-v0.7.0 (the release cut comes first).

## Summary

Collapse the CLI's per-command `is_remote` forks into one execution path coded
against a `GraphClient` trait with two implementations (embedded engine, HTTP),
sharing one wire-DTO crate with the server. Establish an executable parity
referee *before* the refactor. This is the same cure, in the same order, that
fixed the storage layer: one contract, one implementation where semantics are
one thing, an executable contract where two implementations must exist.

## Motivation — validated facts

Graph **semantics** cannot drift between paths today: both converge on the same
engine `_as` entry points (verified: `omnigraph-server/src/handlers.rs` calls
`mutate_as`, `apply_schema_as_with_catalog_check`, `load_as`,
`branch_create_from_as`, `branch_delete_as`, `branch_merge_as` — the same
functions the CLI's embedded arm calls), and Cedar enforcement lives inside
those writers. What *can* drift is everything around them:

1. **15 `is_remote` forks** in `crates/omnigraph-cli/src/main.rs`, each
   duplicating request shaping and output mapping per command. (RFC-007 PR 3
   threaded `apply_server_flag` through exactly these sites — the duplication
   is measured, not estimated.)
2. **Triple DTO construction.** "The result of a load" is built in three
   places: the server handler (engine result → HTTP response), the CLI remote
   arm (HTTP response → `LoadOutput` via `load_output_from_tables`), and the
   CLI local arm (engine result → hand-built `LoadOutput`). Three mappings
   that agree only by discipline — the exact shape of the storage-adapter bug
   class (one prose contract, N implementations, no referee).
3. **The remote `load` arm rides the deprecated `/ingest` route.** A
   non-deprecated CLI verb coupled to a deprecated endpoint turns the
   endpoint's eventual removal into a surprise CLI breakage.
4. **Plane restrictions are accidental, not declared.** `init` / `optimize` /
   `repair` / `cleanup` / `cluster *` are storage-only and `graphs list` is
   server-only by code shape; pointing `optimize` at an `https://` target
   fails with whatever `Omnigraph::open` says about an https URI. Per Hyrum,
   that accidental error text is already someone's dependency.
5. **Parity pinning is thin.** One explicit parity test
   (`cli_schema_config.rs::schema_plan_parity_cli_and_sdk`), flow coverage in
   `system_local.rs`, and the OpenAPI drift test. No systematic
   per-verb embedded-vs-remote comparison exists. Two bugs from the current
   cycle argue the referee's value concretely: the operator-alias positional
   bug (the hidden `legacy_uri` positional swallowed the first arg — local
   and remote disagreed until a live smoke caught it) and the
   `write_text_if_absent` flush bug (one of N implementations of an
   unwritten contract) would both have failed a parity matrix.

## Design

Ordered so each phase is independently shippable and the referee exists before
anything moves — mirroring the storage collapse, where the pinned contract
tests gated the swap, and the test-monolith modularization (#192/#193), which
makes Phase 3 tractable: the CLI dispatch is 1,184 lines today, not 4,200.

### Phase 1 — Parity matrix (the referee; do first, no refactor) *(landed)*

A CLI integration test (extend the `system_local.rs` harness, which already
spawns both binaries): one fixture graph; for every forked verb, run the
command once against the local URI and once against a spawned server with
identical inputs; diff the `--json` outputs against an explicit allowlist of
transport-only fields (e.g. resolved URI). Assert identical exit codes for the
shared error cases.

This pins today's behavior so Phase 3 can't silently change it, and catches
every future fork drift. It also incidentally covers utoipa annotation↔route
mismatches (a lying `#[utoipa::path]` makes the remote leg 404).

**Phase 1 outcome (landed):** `crates/omnigraph-cli/tests/parity_matrix.rs`
— 11 rows green with an **empty divergence ledger**: with matched Cedar
policy on both arms, embedded and remote agree on every forked verb's
scrubbed JSON and exit codes. Two findings along the way: like-for-like
requires the same policy bundle on both arms (a tokens-only server is
default-deny by design — the harness encodes this), and inline execution's
unbound-param matches-all vs the invoke path's hard error is a cross-path
asymmetry, filed as #207 and pinned (not repaired) by the matrix.

### Phase 2 — One wire-DTO crate *(landed)*

Move the HTTP request/response types and the single `engine result → DTO`
mapping per verb into a shared crate (working name `omnigraph-api-types`),
carrying serde + utoipa `ToSchema` derives:

- `omnigraph-server` handlers serialize these types; utoipa derives
  `openapi.json` from them (the existing `openapi.rs` regeneration test stays
  the spec referee).
- The CLI embedded path constructs them via the shared mapping.
- The CLI remote path deserializes the literal same types.

The mapping then exists once, next to the type — it cannot fork. Spec codegen
remains exactly where it belongs: foreign-language clients (the TS SDK
pipeline). Generating a Rust client from the spec is explicitly rejected — it
would round-trip Rust types through a lossy intermediate when compile-time
type sharing is available.

**Prior art to salvage:** PR #139's review explicitly found the
`omnigraph-api-types` extraction *clean* ("the crate extraction itself is
clean and the openapi.json byte-identical claim holds" —
[pr-139 findings](rfc-002-config-cli-architecture.md)); it was the behavior
changes bundled alongside that killed the PR. Seed this phase from the
extraction commits on `ragnorc/scrutinize-rfc-002` rather than rebuilding —
cherry-picked narrowly, never relanded wholesale.

Boundary note: this does NOT violate "transport/auth stay at the boundary"
(invariants §11). The shared crate holds plain serde DTOs; it depends on
neither axum nor the engine's internals. The engine crate does not depend on
it — the `engine result → DTO` mapping lives in the shared crate (or the CLI/
server side), taking engine result types as input.

**Phase 2 outcome (landed):** `crates/omnigraph-api-types` holds the wire
DTOs + their `engine-result → DTO` mappings; `omnigraph-server::api` is a
`pub use` re-export (so `openapi.json` is byte-identical — the referee
passed with zero diff), and the CLI consumes the crate directly. One
deliberate refinement of the original sketch: `LoadOutput` is a rendered
CLI output type, not a wire DTO, so it stayed CLI-side — both its mappings
(local `LoadResult`, remote `IngestOutput`) now sit together in
`output.rs`. The parity matrix passed textually unchanged.

### Phase 3 — `GraphClient` trait, two implementations

```text
trait GraphClient        // verb-level: load, mutate, query, branch_*, schema_*, export, commit_*
 ├── EmbeddedClient      // wraps Omnigraph + the shared mapping; actor: explicit (--as cascade, RFC-007)
 └── RemoteClient        // reqwest + bearer; actor: resolved server-side from the token
```

Each CLI command body is written once against the trait; the 15 forks become
2 impls × 1 contract. Actor resolution is a constructor-time difference of the
impls, never a per-verb branch — the trust model (storage credentials =
self-declared actor via the RFC-007 actor chain; server = token-resolved
actor via the RFC-007 keyed-credential chain) is a feature, not drift.
`RemoteClient` construction is where RFC-007's addressing lands once:
positional URI, `--target`, `--server <name>`, and operator aliases all
resolve to the same (base URL, credential) pair before the trait is touched.
The Phase 1 matrix becomes the trait's conformance suite, run against both
impls.

### Phase 4 — Declared plane capabilities

Each CLI command declares `Storage | Server | Both`. Dispatch checks the
resolved target against the declaration and fails with one deliberate message
("maintenance commands operate on storage directly; use a storage URI, not a
server target") instead of today's incidental errors. The declaration table is
also documentation: it makes the control-plane/data-plane split (maintenance
and cluster commands must work with the server down) explicit in code.
"Server" targets include operator-config named servers (RFC-007), not only
literal `http(s)://` URIs.

### Phase 5 — Route alignment

Add a canonical `/load` endpoint (the handler already exists behind the
`/ingest` shim); point `RemoteClient` at it; keep `/ingest` on its existing
deprecation path. While here, check whether the server uses `utoipa-axum`'s
router-coupled registration (`OpenApiRouter`/`routes!`); if it hand-mounts
routes beside `#[utoipa::path]` annotations, prefer migrating registration so
path annotations and mount points are the same declaration (the modularization
already hit one orphaned-attribute incident of exactly this class).

## Non-goals

- **No localhost-server funnel for the embedded path.** Routing embedded use
  through a daemon would destroy the embedded/CLI/test story to buy parity the
  trait + matrix already provide.
- **No trust-model unification.** `--as` vs bearer-resolved actors stay.
- **No spec-codegen for the Rust client** (see Phase 2).
- **No change to plane-restricted command availability** — maintenance stays
  storage-direct by design; Phase 4 only makes the restriction explicit.
- **No config-authority work** — that was RFC-008, already landed through
  stage 4; this RFC neither accelerates nor blocks stage 5, though Phases 4–5
  make the eventual yaml-boot removal a smaller diff.

## Compatibility

- CLI `--json` output is observable contract; Phase 1 freezes it before
  Phase 3 moves code. Any field-level unification that *changes* output is a
  deliberate, release-noted decision, not a refactor side effect.
- Error-message text for mis-planed commands changes in Phase 4 — release-note
  it (Hyrum).
- `openapi.json` should be byte-stable through Phase 2 if the DTO move is
  faithful; the regeneration test enforces this.

## Testing

- Phase 1 matrix is the spine; it must stay green, textually unchanged,
  through Phases 2–3 (the storage-collapse playbook).
- Phase 2: `openapi.rs` byte-stability + existing server tests.
- Phase 4: one test per capability class asserting the deliberate error.
- Phase 5: parity matrix leg for `load` flips to `/load`; an `/ingest` shim
  test stays until removal.

## Open questions

1. Crate granularity: one `omnigraph-api-types` crate vs folding into an
   existing one. (Leaning separate: server and CLI both depend on it; the
   engine must not. The #139 extraction already answered this with a separate
   crate that reviewed clean.)
2. Does the `query`/`read` streaming path (NDJSON export) fit the trait, or is
   export a documented per-impl method? (Streaming over HTTP vs an iterator
   over the embedded engine differ in shape, not content.)
3. ~~Whether `graphs list` belongs on the trait.~~ **Answered by the
   two-surface architecture**: the embedded impl enumerates the **cluster
   catalog** (`read_serving_snapshot` exists for exactly this), never
   `omnigraph.yaml` (deprecated, RFC-008). `graphs list` becomes
   `Both`-capability: remote = `GET /graphs` (policy-gated), embedded =
   catalog enumeration from a cluster storage root.

## Relationship to prior work

The third application of the same principle in this lineage: storage adapters
(collapsed to one implementation + an executable contract — and its CAS/flush
bugs were exactly the no-referee class), recovery liveness, and now access
paths. RFC-007 supplied the addressing and credential surfaces `RemoteClient`
consumes; RFC-008 removed the competing config authority; RFC-002 remains the
umbrella whose remaining unimplemented pieces (`GraphLocator`, the State
layer) would build on the trait introduced here rather than on per-command
forks.
