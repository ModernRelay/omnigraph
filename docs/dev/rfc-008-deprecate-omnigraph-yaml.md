# RFC: Deprecate `omnigraph.yaml` — One Concern per Config Surface

**Status:** Proposed
**Date:** 2026-06-11
**Builds on:** [rfc-007-operator-config.md](rfc-007-operator-config.md) (the
operator layer that absorbs the identity/credential keys),
[rfc-005-server-cluster-boot.md](rfc-005-server-cluster-boot.md) (Landed —
cluster-booted serving), RFC-006 storage roots (landed: #186/#190/#194).
**Supersedes in part:** RFC-007's "project layer" framing (§Relationship
below) and [rfc-002-config-cli-architecture.md](rfc-002-config-cli-architecture.md)'s
assumption that `omnigraph.yaml` remains the project manifest.
**Target release:** staged; final removal at the next major (see Sequencing).

## Summary

Retire `omnigraph.yaml`. It is three unrelated concerns wearing one
filename — server deployment config, project/CLI conveniences, and operator
identity — and the mixture is not a cosmetic wart but the root cause of a
recurring class of problems: operators keeping personal copies of "project"
files, repo checkouts able to carry credential-adjacent keys (the #139
security findings), `omnigraph init` scaffolding config into unrelated
directories, and every config discussion needing a paragraph to establish
which of the three files is meant.

The end state is **two config surfaces with single owners**:

| Surface | Owner | Declares |
|---|---|---|
| **Cluster config** (`cluster.yaml` + catalog) | the team, in a repo | what the system *is*: graphs, schemas, queries, policies, storage |
| **Operator config** (`~/.omnigraph/`) | one person, in `$HOME` | who *I* am: identity, credentials, known servers, ergonomics |

plus **flags/env** for the zero-config tier (one graph, one server, no
control plane) — which already works today with no file at all.

`omnigraph.yaml` has no role left once every key has a better home. This
RFC gives each key that home, and stages the retirement so that no working
setup breaks without a loud warning, a migration command, and a full
deprecation cycle first.

## Motivation

- **It breaks the ownership logic.** A config file must have one owner. A
  file that carries `graphs:` (team-owned, reviewable) next to `cli.actor`
  (one person's identity) and `auth.env_file` (credential loading) can be
  neither safely committed nor sensibly personal. Every real deployment
  this cycle tripped on it: per-operator copies in `~/exp/intel`,
  graph-scoped alias URIs that only make sense per-person, the #139
  findings where a checkout-supplied file could redirect tokens.
- **The cluster made it redundant.** Since RFC-005/006, a cluster
  deployment serves from the applied catalog — `--cluster` mode does not
  read `omnigraph.yaml` *at all*. Stored queries, policies, bindings, and
  graph addressing all have authoritative homes. What remains in
  `omnigraph.yaml` for cluster users is dead weight that can silently
  disagree with what is actually serving.
- **Two declarative dialects is one too many.** `cluster.yaml` and
  `omnigraph.yaml` both declare graphs/queries/policies with different
  schemas, different validation strictness, and different lifecycle
  guarantees. Maintaining, documenting, and testing both — and explaining
  when each applies — is a permanent tax (the "programming integrated over
  time" lens says: this forks on every config-surface change).

## Non-Goals

- **Breaking anyone now.** Every `omnigraph.yaml` that works today keeps
  working through the entire deprecation window, with warnings.
- **Retiring the zero-config tier.** `omnigraph-server s3://bucket/g.omni
  --bind …` plus env vars stays first-class forever — that tier needs *no*
  file, which is the point.
- **Forcing the control plane on single-graph users.** The migration target
  for a multi-graph yaml deployment is a *minimal* cluster (file-rooted,
  no bucket required, `cluster.yaml` barely longer than the `graphs:` map
  it replaces) — but a single graph never needs even that.
- **Touching `cluster.yaml`** — its schema and strictness are unchanged.

## Where every key goes (the complete migration map)

The full `OmnigraphConfig` surface (verified against
`crates/omnigraph-server/src/config.rs:182-207`):

| `omnigraph.yaml` key | Concern | New home |
|---|---|---|
| `graphs.<name>.uri` | what exists / where | `cluster.yaml` `graphs:` (storage-root-derived) — or a flag/env for the zero-config tier |
| `graphs.<name>.queries`, top-level `queries:` | what exists | cluster catalog (`.gq` discovery, RFC-004/#183) |
| `graphs.<name>.policy.file`, top-level `policy.file`, `server.policy.file` | what's enforced | `cluster.yaml` `policies:` + `applies_to` bindings |
| `server.bind` | deployment runtime | `--bind` / env (already authoritative; the key is a default) |
| `server.graph` | deployment runtime | `--target`-style flag / env in the zero-config tier; meaningless under cluster boot |
| `graphs.<name>.bearer_token_env`, `auth.env_file` | credentials | operator credentials chain (RFC-007 §D4) |
| `cli.actor` | identity | `operator.actor` (RFC-007 §D3) |
| `cli.output_format`, `cli.table_*` | personal ergonomics | `defaults:` in operator config (RFC-007 §D2) |
| `cli.graph`, `cli.branch` | personal targeting | operator config: named servers + a per-operator default target (RFC-007 PR 3) |
| `aliases.<name>` | a personal name conflated with a content pointer | **splits in two** (RFC-007 §D2 "bindings, not content"): the referenced `.gq` file's *content* becomes a catalog stored query (team-reviewed); the *binding* becomes an operator alias referencing that name. `config migrate` proposes both halves but cannot publish catalog content itself — that is a `cluster apply` |
| `query.roots` | discovery convenience | obsolete — cluster query discovery (#183) replaced it |
| `project.name` | label | dropped (the cluster's `metadata.name` is the deployment label) |

Two placements worth defending:

- **Aliases are operator config, not cluster config.** The stored query is
  the shared contract (catalog-owned, digest-pinned); an alias is one
  person's shorthand with their favorite default params and target. Putting
  aliases in the cluster would force team review on personal ergonomics;
  leaving them per-directory recreates today's problem. Per-operator,
  keyed by server/graph name, is the AWS-profile shape.
- **Multi-graph serving without a control plane migrates to a minimal
  cluster, not to a new file.** The honest cost: `cluster import` + `apply`
  once, on a `file://` root next to the graphs. The honest benefit: one
  declarative dialect, one validation path, one serving source — and the
  upgrade path to buckets/approvals is a one-line `storage:` change instead
  of a re-platform.

## Deprecation mechanics

Per Hyrum's Law (the repo's own deny-list: shipped observable behavior is
contract), retirement is staged, loud, and tooled:

1. **Warn** *(landed)*. Loading `omnigraph.yaml` emits a one-line deprecation notice
   naming the replacement for each key actually present in the file (not a
   generic banner — the migration map above, applied to *your* file).
   Suppressible per-process (`OMNIGRAPH_SUPPRESS_YAML_DEPRECATION=1`) for
   CI logs during the window.
2. **Migrate** *(landed)*. `omnigraph config migrate` reads an existing
   `omnigraph.yaml` and writes the split: the team half as a ready-to-review
   `cluster.yaml` (+ moves query/policy files into the checkout layout),
   the personal half merged into `~/.omnigraph/config.yaml` — printing a
   diff-style summary and touching nothing without `--write`. The command
   is the test of the migration map's completeness: any key it cannot
   place is a bug in this RFC.
3. **Stop scaffolding** *(landed)*. `omnigraph init` stops generating
   `omnigraph.yaml` (it scaffolded one into cwd — the source of the
   test-pollution bug). **No replacement scaffold**: a minimal
   `cluster.yaml` is five lines; a generator would be a second copy of the
   schema to keep in sync, producing a file that is unusable until
   hand-edited anyway (Terraform has no config scaffolder either). New
   users copy from the cluster quick-start; migrants get a ready-to-review
   `cluster.yaml` from `config migrate`.
4. **Opt-in strict** *(landed — the release gap to stages 1–3 collapsed: no version boundary was crossed between them, so all four ship in the same release)*. `OMNIGRAPH_NO_LEGACY_CONFIG=1` turns the warning into
   an error — for teams that finished migrating and want regressions caught.
5. **Remove at the next major.** Loading the file becomes an error pointing
   at `config migrate`. The `OmnigraphConfig` code path, the dual
   query-registry loaders, and the yaml-mode server boot source are deleted
   — the payoff that makes the whole exercise worth it.

Stages 1–3 can land in one release once RFC-007 PRs 1–2 exist (the operator
layer must exist before anything can migrate *to* it). Stage 4 the release
after. Stage 5 at the major, with the removal listed in release notes from
stage 1 onward.

## What this deletes, eventually

- The `OmnigraphConfig` struct and its 12-key surface, the
  `load_config`/`load_cli_config` pair and its env-side-effect, the
  scaffolder, and the legacy resolution paths (`resolve_cli_graph`'s dual
  modes — finding #11's root cause).
- The yaml-mode multi-graph server boot (`ServerConfigMode::Multi` keeps
  existing — cluster boot constructs it — but its `omnigraph.yaml` source
  goes).
- An entire class of documentation ("which file does X go in?") and the
  #139 security surface (a checkout cannot hijack what no longer loads).

## Relationship to RFC-007 and RFC-002

RFC-007 ships the operator layer this RFC migrates *to*; its "project
layer" language should be read as transitional — after this RFC, the
project layer **is** the cluster checkout, and RFC-007's PR 3 (project
`server:` references) applies to `cluster.yaml`-adjacent operator targeting
rather than to `omnigraph.yaml`. RFC-002's locator/state-layer work, if
resumed, targets the two-surface world directly. RFC-002's file-naming
decisions (`~/.omnigraph/` as the one dir) are unaffected.

## Open questions

- **Window length**: one minor release between warn (stage 1) and strict
  (stage 4), or two? Cookbooks, skills, and the deployment docs all need
  the same pass; the migration command makes a short window defensible.
- **`omnigraph login` vs `config migrate` ordering** — both write
  `~/.omnigraph/`; whichever lands first establishes the file-locking and
  atomic-write helpers the other reuses.
- **Does the MCP server config** (RFC-003) reference `omnigraph.yaml`
  anywhere that needs the same treatment? To be audited in stage 1.
