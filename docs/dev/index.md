# Developer guide

These pages describe the system that exists now: its architecture, invariants,
change boundaries, and test ownership. Design history and proposals belong in
[RFCs](../rfcs/); user behavior belongs in the [user guide](../user/).

## Before every change

1. Read [Architectural invariants](invariants.md).
2. Use the domain map in [Lance alignment](lance.md), then read the complete
   upstream pages relevant to the task.
3. Use [Testing](testing.md) to find existing coverage and establish a focused
   baseline before adding or changing tests.

## Understand the system

| Topic | Guide |
|---|---|
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

## Change and verify it

| Task | Guide |
|---|---|
| Find the owning suite, helpers, failpoints, and cloud gates | [Testing](testing.md) |
| Understand PR and release workflows | [CI and releases](ci.md) |
| Change required checks or repository policy | [Branch protection](branch-protection.md) |
| Write or reorganize documentation | [Documentation guide](documentation.md) |
| Review a parser boundary regression example | [Camel-case filtering case study](case-studies/camel-case-filtering.md) |

The workflow YAML, code, and tests are authoritative for exact command lines,
serialized fields, constants, and assertions. Developer docs explain ownership
and invariants rather than duplicating those details.

## History and decisions

- [RFC process and registry](../rfcs/README.md)
- [Release notes](../releases/)
- Git history for superseded implementation plans and handoff notes

An implemented RFC records why a decision was made; it does not replace the
current guides above. A roadmap belongs in an issue, not in `docs/dev/`.
