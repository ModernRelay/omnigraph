# Developer guide

**Audience:** anyone changing Omnigraph, an agent included
**Authority:** the index of developer guides; what to read before every change
is in [`AGENTS.md`](../../AGENTS.md#required-reading)

These pages describe the system: its architecture, invariants, change
boundaries, and test ownership. Why a design was chosen is in a
[decision record](../decisions/README.md); user behavior belongs in the
[user guide](../user/).

## Understand the system

| Topic | Guide |
|---|---|
| How to reason about a data system: contract, mechanism, distribution of work | [First principles of data systems](systems.md) |
| What each kind of operation promises, and where the engine differs today | [The contract](contract.md) |
| The workload hypothesis, what it asks for, and its evidence | [The work](workload.md) |
| Components, authority, and read/write/control flows | [Architecture](architecture.md) |
| Non-negotiable guarantees and deny-list | [Architectural invariants](invariants.md) |
| Query planning, traversal, search, and mutation orchestration | [Execution](execution.md) |
| Atomic multi-table publication | [Write path](writes.md) |
| Crash classification and convergence | [Recovery](recovery.md) |
| Three-way branch integration | [Merge](merge.md) |
| Managed and external Blob boundaries | [Blob internals](blob.md) |
| Cluster apply, serving snapshots, and writer ownership | [Control plane](control-plane.md) |
| Bounded graph-batch ingestion | [Ingestion](ingestion.md) |
| Release, wire, storage, and dependency compatibility | [Versioning](versioning.md) |
| Lance behaviors Omnigraph depends on, works around or forbids | [Lance compatibility fences](lance-fences.md) |

## Change and verify it

| Task | Guide |
|---|---|
| Apply the testing rules and find where a test goes | [Testing](testing.md) |
| Find the owning suite, helpers, failpoints, commands and cloud gates | [Test ownership](test-ownership.md) |
| Keep local builds warm across features and worktrees | [Local builds](local-builds.md) |
| Maintain HTTP naming, OpenAPI coverage, and compatibility | [HTTP API conventions](http-api.md) |
| Understand PR and release workflows | [CI and releases](ci.md) |
| Change required checks, repository policy, or who is requested to review | [Branch protection](branch-protection.md) |
| Write or reorganize documentation | [Documentation guide](documentation.md) |

The workflow YAML, code, and tests are authoritative for exact command lines,
serialized fields, constants, and assertions. Developer docs explain ownership
and invariants rather than duplicating those details.

## Decisions and history

- [Decision records](../decisions/README.md): one page for each design chosen
  on purpose, with what was rejected
- [Release notes](../releases/)
- Git history for everything else

A roadmap belongs in an issue, not in `docs/dev/`.
