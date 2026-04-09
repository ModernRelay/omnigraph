# Sync Notes

This file is not canon.
It is a grounded working note split into:

- confirmed current behavior
- open product questions
- future architecture ideas

Use [omnigraph-canon.md](/Users/andrew/code/omnigraph/docs/dev/omnigraph-canon.md) for the authoritative current-state description.

## 1. Confirmed Current Behavior

## CLI Surface

- `read` is the read-only query path. It has no graph-side effects.
- `change` is the mutation path. It is intentionally separate from `read`.
- current top-level commands include:
  - `init`
  - `load`
  - `schema plan`
  - `branch create`
  - `branch list`
  - `branch merge`
  - `snapshot`
  - `read`
  - `change`
  - `run list/show/publish/abort`
  - `version`
- CLI version is exposed through:
  - `og -v`
  - `og --version`
  - `og version`
- help remains standard:
  - `og -h`
  - `og --help`

## Branching And Merge

- branches can be created from the current default branch or an explicit source branch
- reads can target any branch
- merges take a source branch and a target branch
- CLI default branch behavior is already implemented as `main` when no branch is specified in config or flags
- public writes use hidden run branches and publish the staged result into the target branch

Important clarification:
- Omnigraph branching is its own database mechanism built on the manifest, graph commits, and Lance branches
- S3 does not provide Omnigraph branch semantics

## Snapshot Command

- `og snapshot --json` is a metadata command
- it prints the current snapshot payload for a branch:
  - branch
  - snapshot id
  - manifest version
  - table entries with current table path, table version, table branch, and row count
- it does not dump all nodes and edges in the graph

Important distinction:
- internal `Snapshot` is the frozen, pinned read view queries execute against
- user-facing `snapshot` is an inspection command for snapshot metadata

## Versioning

Current version concepts that really exist:

- CLI and crate semver
- manifest version
- graph commit ids
- run ids
- per-table Lance dataset versions

What does not currently exist as a first-class product surface:

- a public “branch version” command
- one unified version table command

## Schema Migration

- schema migration tooling is still largely missing
- schema branching and schema merge policy are not solved
- there is no production-grade schema migration workflow yet
- phase 1 now persists `_schema.ir.json` and `__schema_state.json` as the accepted schema contract for a repo
- phase 1 schema policy is fail-closed:
  - existing repos are schema-immutable
  - `_schema.pg` edits are rejected unless a future migration flow updates the accepted IR/state in lockstep
  - public non-`main` branches block legacy schema-state bootstrap entirely
- the next implemented step is plan-only:
  - `omnigraph schema plan --schema new.pg <repo>` diffs desired schema against the accepted persisted schema IR
  - it reports supported additive or explicit-rename steps versus unsupported changes
  - it does not apply migrations yet

## SDK And Surface Area

- the core runtime API is in `omnigraph`
- the CLI and server are thin surfaces over that runtime
- there is no separate official SDK package or FFI layer yet

## Storage

Current storage modes that are actually real:

- local filesystem repo root
- `file://...`
- `s3://...`

- RustFS is the canonical on-prem S3-compatible backend used to validate the `s3://` path
- AWS S3 is the cloud target for the same `s3://` path

Important operating rule:

- the system is still single-writer per repo

## 2. Open Product Questions

## CLI Naming

- should `read` and `change` remain the public names, or should they become `query` and `mutate`
- if the names change, is the clarity gain worth breaking current usage
- if aliases are added, which names should be canonical and which should be compatibility shims

My view:
- keeping `read` and `change` is fine for now
- if anything changes later, `query` as an alias is lower risk than renaming the canonical commands immediately

## Snapshot Command Naming

- should the user-facing `snapshot` command be renamed to `state`
- or should `snapshot` remain the metadata command and the docs simply explain the distinction better

My view:
- do not rename it yet
- the current confusion is documentation-level, not implementation-level
- renaming now would create churn without solving a core product problem

## Version UX

- should Omnigraph eventually expose a richer “system versions” view
- if yes, what should it show:
  - CLI version
  - server version
  - repo target URI
  - manifest version
  - current branch head commit
- should that be a new command, or part of `snapshot`

My view:
- keep `og --version` and `og version` for CLI semver only
- if a richer version/status view is needed, add a separate command later

## Schema Migration Policy

- should phase 1 be “no schema migration while branches exist”
- what exactly counts as non-breaking
- how should schema merge interact with data merge
- should schema changes be branch-local at all, or serialized through a dedicated migration flow

My view:
- the notes are right that schema migration is a major unresolved area
- fail-closed immutability is the correct current policy until a real migration workflow exists
- branch-local schema evolution should stay out of scope until accepted-schema state and migration planning are first-class

## SDK Strategy

- do we want an official embedded SDK first
- or a server-first external API first
- do we want FFI at all in the short term

My view:
- FFI is a possible implementation route later, not a current platform decision
- it should not be written down as “the only viable approach” yet

## Storage Strategy

- should Omnigraph remain dual-mode:
  - local filesystem for simple deployments
  - `s3://` for scalable or cloud deployments
- or should the long-term product direction become object-store-first

My view:
- keep both
- local filesystem is still valuable for local dev and simple on-prem installs
- `s3://` should become the primary scalable deployment path, not the only supported path

## 3. Future Architecture Ideas

These are ideas, not current behavior.

## S3-Backed Deploys

- use exact versioned S3 prefixes for live repos
- point runtime at an explicit release URI, not a mutable “current” pointer
- use S3 versioning as infra-level recovery, not as Omnigraph’s branching model

## AWS Runtime Migration

- validate AWS S3-backed Omnigraph on the current EC2 runtime first
- move to ECS/Fargate only after that path is stable
- keep single-writer-per-repo as the operating rule until coordination improves

## Richer Status / Version Command

- add a future status-oriented command that could show:
  - CLI version
  - server version
  - target URI
  - branch
  - manifest version
  - commit head

This should be a separate status surface, not a redefinition of `--version`.

## Schema Evolution Roadmap

- phase 1:
  - explicitly lock schema evolution down
  - require manual coordination
- phase 2:
  - define a constrained set of non-breaking changes
  - make validation and merge behavior explicit
- phase 3:
  - define breaking changes and whether they are ever branch-mergeable

## Recommended Next Step

If these notes are going to continue evolving, the right discipline is:

- put product facts in the canon
- put unresolved decisions here
- avoid recording speculative policy as if it were already chosen
