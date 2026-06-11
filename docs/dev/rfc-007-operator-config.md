# RFC: Per-Operator Config — the Operator Slice of RFC-002

**Status:** Proposed
**Date:** 2026-06-11
**Builds on:** [rfc-002-config-cli-architecture.md](rfc-002-config-cli-architecture.md) (Proposed; implementation parked — PRs #139/#162 closed over review findings), [rfc-005-server-cluster-boot.md](rfc-005-server-cluster-boot.md) (Landed), RFC-006 storage roots (#186/#190/#194, landed). The #139 review record is a normative input: every design rule in §D6 traces to a confirmed finding.
**Target release:** unversioned (staged; see Sequencing).

## Summary

Give OmniGraph the operator half of the Terraform config split. Terraform
separates `~/.terraformrc` (who I am, my credentials, my CLI behavior) from
the working directory's `*.tf` (what the project declares). OmniGraph today
has only the project half: `./omnigraph.yaml` in the current working
directory (or `--config <path>`), and nothing else — no home-level config,
no walk-up, no env override for the CLI. Operator identity and credentials
must be re-declared in every directory an operator works from, and — worse —
they end up in files that live next to repo-committed project config.

This RFC introduces **`~/.omnigraph/config.yaml`** (the operator layer) and
a **keyed credentials chain**, scoped deliberately small:

1. **Operator identity** — a default actor for every `--as` cascade.
2. **Credentials by server name** — no more inventing env-var names per
   server; secrets never inline, never in the project layer.
3. **Named servers** — operator-owned endpoint definitions that project
   configs can reference but not redefine.

It is explicitly a **subset of RFC-002**, sequenced to land. RFC-002 settled
the right long-term decisions (one `~/.omnigraph/` dir, credentials keyed by
server name, `OMNIGRAPH_CONFIG`/`OMNIGRAPH_HOME` env precedence) but its
implementation arrived as one 4,800-line PR mixing a crate extraction with
behavior changes, and died over ten confirmed findings. This RFC adopts
RFC-002's settled decisions verbatim where they apply, defers everything
else (`GraphLocator`, multi-homing, `omnigraph use`, the State layer), and
encodes the #139 findings as design rules so the same failures cannot recur.

## Motivation

Three concrete pains, all hit in real operation this cycle:

- **Identity repetition.** The cluster actor cascade (#180) resolves
  `--as` from the per-operator `omnigraph.yaml` — which means every
  operator hand-maintains a copy in every working directory (the
  `~/exp/intel` setup needed exactly this). A repo-committed
  `omnigraph.yaml` cannot carry `as: act-andrew` without claiming every
  contributor is Andrew.
- **Credential ergonomics.** `bearer_token_env` forces three coordinated
  steps per server (invent a var name, reference it in config, set it in
  the secret store). The peer group — AWS profiles, `gh hosts`, kubeconfig
  users — keys secrets by the server's *name*.
- **Cluster-era working shape.** With clusters on object storage (RFC-006),
  the project directory is a *declaration checkout* — operators run
  `cluster apply --config ./checkout` from anywhere. The things that are
  about the *operator* (who am I, which servers do I know, how do I like
  output formatted) have no home that travels with them.

## Non-Goals

- **`GraphLocator` / multi-homed graph resolution** (RFC-002 §1) — the
  biggest and riskiest part of config-v2; untouched here.
- **`omnigraph use` / the State layer** (`~/.omnigraph/state/`) — deferred
  with it (finding #2 showed its precedence interacts badly with scaffolds;
  that problem belongs to the slice that introduces it).
- **OS keychain integration** — the credentials *chain* (§D4) leaves a slot
  for it; this RFC ships env + file sources only.
- **Project-file walk-up.** Terraform does not walk up from subdirectories
  and neither do we — `--config` (or running in the directory) stays the
  explicit, deterministic story. Rejected, not deferred: walk-up makes "which
  config am I using" a function of cwd depth, the class of surprise this RFC
  exists to remove.
- **Renaming or removing anything.** No flag renames, no key renames, no
  schema-version bumps (findings #1, #3, #10).

## Background (verified against main)

- **Project-config lookup today** (`crates/omnigraph-server/src/config.rs:529-553`,
  shared by CLI and server): `--config <path>`, else `./omnigraph.yaml` in
  cwd, else built-in defaults. Relative paths inside the file resolve
  against the file's own directory (`base_dir`). No env var, no home file,
  no walk-up.
- **Side-effect on load** (`crates/omnigraph-cli/src/helpers.rs:102-108`):
  `load_cli_config` also loads `auth.env_file` into the process env —
  this is how `OMNIGRAPH_BEARER_TOKEN` reaches remote commands today.
- **Actor resolution** (`helpers.rs:170`, #180): `--as` flag, else the
  project config's actor — currently the end of the chain.
- **Existing credential mechanism**: `TargetConfig.bearer_token_env` names
  an env var; `auth.env_file` points at a git-ignored dotenv. Both keep
  working indefinitely (RFC-002 already committed to this; finding #3
  showed what happens otherwise).
- **`OMNIGRAPH_CONFIG`** exists today only as the *container entrypoint's*
  translation to the server's `--config`. The CLI does not read it.

## Design

### D1. Files and discovery

```
~/.omnigraph/config.yaml      # the operator layer (this RFC)
~/.omnigraph/credentials      # keyed secrets, 0600, git-irrelevant (§D4)
./omnigraph.yaml              # the project layer (unchanged)
```

Discovery order for the operator file: `$OMNIGRAPH_HOME/config.yaml` if
`OMNIGRAPH_HOME` is set, else `~/.omnigraph/config.yaml`. Absent file =
empty layer, never an error. `~` is expanded wherever paths are read
(finding #9 — today a literal `./~/...` directory gets created).

`OMNIGRAPH_CONFIG=<path>` becomes a first-class override for the *project*
file in the CLI (highest precedence below the `--config` flag), aligning the
CLI with the container contract that already uses this variable for the
server. One name, one meaning, both binaries.

Per RFC-002 §4 (adopted verbatim): `~/.omnigraph/` is the one canonical
dir — cache/state subdirectories arrive with their own slices; XDG roots are
not part of the mental model (`$XDG_CONFIG_HOME` may be honored as a
fallback read location if set, but is never written to).

### D2. The operator schema (v1 of this layer)

```yaml
# ~/.omnigraph/config.yaml — about the OPERATOR, never about a project
operator:
  actor: act-andrew          # default for every --as cascade

servers:                     # operator-owned endpoint definitions
  intel-dev:
    url: http://127.0.0.1:8080
  prod:
    url: https://graph.modernrelay.ai
    # No token here, ever. Resolution: §D4.

defaults:
  output: table              # read --format default
```

Unknown keys are a **warning, not an error** in this layer (an operator file
written by a newer CLI must not brick an older one; contrast with
`cluster.yaml`, where unknown keys are deliberately fatal because they
change what a *plan* means).

### D3. Precedence and the merge rule

```
flag  >  env  >  project omnigraph.yaml  >  operator config  >  built-in
```

with exactly one principled inversion (§D5): **credentials and endpoint
definitions never come from the project layer when an operator-layer
definition exists for the same server name.**

Merging is **key-level**: scalars override per key; maps (`servers:`,
`graphs:`) merge per *entry*, and entries merge per *field* (finding #13 —
`merge_map` replacing whole entries silently dropped sibling fields). A
project file referencing `server: prod` composes with the operator's
`servers.prod.url`; it does not need to re-declare it and cannot
accidentally clobber half of it.

Concretely for the two flows this slice touches:

- **Actor**: `--as` > project `as:`/actor key (unchanged semantics) >
  `operator.actor` > none (commands that need an actor keep failing loudly).
- **Output format**: `--format` > project default > `defaults.output` >
  `table`.

### D4. Credentials: keyed by server name, by-reference always

Adopted from RFC-002 §5 unchanged, minus the keychain (a later source in
the same chain). For a server named `<name>`, the resolution chain is:

1. `OMNIGRAPH_TOKEN_<NAME>` (uppercased, `-`→`_`) — explicit env, wins.
2. `[<name>]` section in `~/.omnigraph/credentials` (INI-style, `0600`;
   the loader refuses a group/world-readable file).
3. The legacy pair — `bearer_token_env` + `auth.env_file` — exactly as
   today, for configs that already use it.

No inline secrets in any YAML file, operator or project (the existing
invariant 12 posture extended to disk). A future `omnigraph login <name>`
writes/rotates one section of the credentials file via temp + rename
(finding #7: every operator-layer write is atomic), creating it `0600`.

### D5. The trust boundary (the security findings, made structural)

Findings #4, #5, #6 share one root cause: the project layer — a file that
arrives with a *repo checkout* — could redirect where requests go and what
secrets they carry. The rules:

1. **A project file may *reference* a server by name; it may not *redefine*
   an operator-defined server.** If `./omnigraph.yaml` declares
   `servers.prod.url` and `~/.omnigraph/config.yaml` also defines `prod`,
   the operator definition wins and the CLI warns about the shadowed
   project entry. A project-only server name keeps working (legacy compat),
   but the keyed-credentials chain (§D4 steps 1–2) never resolves for it —
   only the legacy explicit `bearer_token_env` does. Net effect: a malicious
   checkout cannot point `prod` at an attacker host and harvest the
   operator's `prod` token.
2. **`auth.env_file` keeps auto-loading (compat), but project-layer
   env-files cannot *override* variables already set in the process or by
   the operator layer** — first-set-wins, operator-before-project (the
   existing real-env-wins rule, extended one layer down). Finding #5's
   injection becomes a no-op against any var the operator actually uses.
3. **A token is sent only to the server it is keyed to.** The legacy
   single `OMNIGRAPH_BEARER_TOKEN` fallback keeps working for the
   single-server shape, but when a request resolves through a *named*
   server, only that name's chain applies (finding #6's broadcast).

### D6. Compatibility rules (the #139 findings as law)

| Rule | Source finding |
|---|---|
| No flag or key is removed or renamed; new behavior is additive | #1, #3 |
| A config that loads today loads identically after this RFC; new validation applies only to new keys | #3, #8, #10 |
| Every operator-layer file write is temp + rename, never in-place | #7 |
| `~` expands wherever a path is read | #9 |
| Map merges are per-entry, per-field — never wholesale replace | #13 |
| One resolution path per concern — the actor chain and the token chain each have exactly one implementation, called by CLI and server alike | #11, #12 |
| Each slice lands as its own PR with the workspace gate green; no slice mixes mechanical moves with behavior changes | #139's disposition |

## Sequencing

Three PRs, each independently useful, each landable without the next:

1. **PR 1 — the operator file + identity.** Loader for
   `~/.omnigraph/config.yaml` (+ `OMNIGRAPH_HOME`, `~`-expansion, warn-only
   unknown keys), `operator.actor` joining the `--as` cascade,
   `defaults.output` joining the format cascade, `OMNIGRAPH_CONFIG` env for
   the CLI's project file. Docs: `cli-reference.md` gains the layer table.
2. **PR 2 — keyed credentials.** `servers:` in the operator layer, the
   §D4 chain (env + credentials file), the §D5 trust rules, and
   `omnigraph login <name>` (atomic write, `0600`). Legacy mechanisms
   untouched and tested-as-untouched.
3. **PR 3 — project references.** `server: <name>` in project
   graph/target entries resolving through operator-defined servers, with
   the shadowing warning. This is the *bridge* toward RFC-002's locator —
   it gives multi-server addressing a safe, minimal form without the
   `GraphLocator` rework.

## Open questions

- Should `operator.actor` apply to *local* (embedded-engine) writes too, or
  only where a server/cluster boundary exists? Leaning yes-everywhere: one
  identity chain (§D6 one-path rule), and local audit rows get better.
- Does `defaults.output` belong in slice 1, or is identity-only an even
  cleaner first PR? (Cost of including it is one cascade hop; value is
  immediate.)
- `omnigraph config view --resolved` (RFC-002 had it; #139 shipped a
  version) — slice 1 or slice 2? It materially helps debugging precedence,
  which argues early.

## Relationship to RFC-002 and RFC-008

**RFC-008 supersedes this RFC's "project layer" framing**: with
`omnigraph.yaml` deprecated
([rfc-008-deprecate-omnigraph-yaml.md](rfc-008-deprecate-omnigraph-yaml.md)),
the project layer *is* the cluster checkout. References to project
`omnigraph.yaml` in §D3/§D5 describe the transitional window only; the
trust-boundary rules apply unchanged to whatever the project layer is at a
given stage. Sequencing couples them: RFC-007 PRs 1–2 must land before
RFC-008's migration stages can begin (the operator layer is what keys
migrate *to*).

RFC-002 remains the umbrella architecture. This RFC implements its §2
(layered config, global-first), §4 (file naming / one dir), and §5
(credentials) in their minimal load-bearing form, and explicitly defers §1
(`GraphLocator`/targets), §3 (roles), and the State layer. If/when the
locator work resumes, it builds on these layers rather than re-landing
them. RFC-002's header should gain a pointer here once this merges.
