# RFC: Per-Operator Config — the Operator Slice of RFC-002

**Status:** Proposed
**Date:** 2026-06-11
**Builds on:** [rfc-002-config-cli-architecture.md](rfc-002-config-cli-architecture.md) (Proposed; implementation parked — PRs #139/#162 closed over review findings), [rfc-005-server-cluster-boot.md](rfc-005-server-cluster-boot.md) (Landed), RFC-006 storage roots (#186/#190/#194, landed). The #139 review record is a normative input: every design rule in §D6 traces to a confirmed finding.
**Paired with:** [rfc-008-deprecate-omnigraph-yaml.md](rfc-008-deprecate-omnigraph-yaml.md) — together they define the two-surface architecture this RFC's operator half belongs to.
**Target release:** unversioned (staged; see Sequencing).

## Summary

Give OmniGraph the operator half of the **two-surface config architecture**
(RFC-008): **cluster config** (team-owned, in a repo — what the system *is*)
and **operator config** (person-owned, in `$HOME` — who *I* am). This is
Terraform's split: `~/.terraformrc` for the operator, the checkout for the
declaration. OmniGraph today has neither half cleanly — `omnigraph.yaml`
mixes both concerns (RFC-008 retires it), and there is no home-level config
at all: identity and credentials get re-declared per working directory, in
files that sit next to repo-committed config.

This RFC introduces **`~/.omnigraph/config.yaml`** (the operator surface)
and a **keyed credentials chain**, scoped deliberately small:

1. **Operator identity** — a default actor for every `--as` cascade.
2. **Credentials by server name** — no more inventing env-var names per
   server; secrets never inline, never in any repo-committed file.
3. **Named servers** — operator-owned endpoint definitions; nothing a
   checkout supplies can redefine them.

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
- **Config-file walk-up.** Terraform does not walk up from subdirectories
  and neither do we — `--config` (or running in the directory) stays the
  explicit, deterministic story for cluster checkouts. Rejected, not
  deferred: walk-up makes "which config am I using" a function of cwd
  depth, the class of surprise this RFC exists to remove.
- **Retiring `omnigraph.yaml`** — that is RFC-008's job, with its own
  staging. This RFC builds the destination; during RFC-008's deprecation
  window the legacy file keeps loading exactly as today.
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
~/.omnigraph/config.yaml      # the operator surface (this RFC)
~/.omnigraph/credentials      # keyed secrets, 0600, git-irrelevant (§D4)
./cluster.yaml + checkout     # the team surface (unchanged; RFC-004..006)
./omnigraph.yaml              # legacy, loads as today through RFC-008's window
```

Discovery order for the operator file: `$OMNIGRAPH_HOME/config.yaml` if
`OMNIGRAPH_HOME` is set, else `~/.omnigraph/config.yaml`. Absent file =
empty layer, never an error. `~` is expanded wherever paths are read
(finding #9 — today a literal `./~/...` directory gets created).

`OMNIGRAPH_CONFIG=<path>` becomes a first-class override for the `--config`
argument in the CLI (highest precedence below the flag itself), aligning the
CLI with the container contract that already uses this variable for the
server. One name, one meaning, both binaries — it points at whatever the
command's `--config` would (a cluster checkout for cluster commands; the
legacy file during RFC-008's window).

Per RFC-002 §4 (adopted verbatim): `~/.omnigraph/` is the one canonical
dir — cache/state subdirectories arrive with their own slices; XDG roots are
not part of the mental model (`$XDG_CONFIG_HOME` may be honored as a
fallback read location if set, but is never written to).

### D2. The operator schema (v1 of this layer)

```yaml
# ~/.omnigraph/config.yaml — about the OPERATOR, never about the system
operator:
  actor: act-andrew          # default for every --as cascade

servers:                     # operator-owned endpoint definitions
  intel-dev:
    url: http://127.0.0.1:8080
  prod:
    url: https://graph.modernrelay.ai
    # No token here, ever. Resolution: §D4.

aliases:                     # personal shorthand over CLUSTER-owned queries
  triage:
    server: intel-dev        # required: names an operator server above
    graph: spike             # optional (omit for single-mode servers)
    query: weekly_triage     # STORED query name on that server — never a file
    args: [since]            # positional CLI args -> params, in order
    params: { limit: 20 }    # optional fixed defaults (positionals/--params win)
    format: table            # optional; feeds the format cascade

defaults:
  output: table              # read --format default
```

Unknown keys are a **warning, not an error** in this layer (an operator file
written by a newer CLI must not brick an older one; contrast with
`cluster.yaml`, where unknown keys are deliberately fatal because they
change what a *plan* means).

#### Aliases are bindings, not content

Three things must not be conflated:

- **Stored queries (the cluster catalog)** are *content plus its canonical,
  team-owned name* — reviewed, digest-pinned, invocable by name over HTTP.
- **Legacy `omnigraph.yaml` aliases** conflate a personal name with a
  pointer to query *content in a local file* — which is why they break
  across directories and can drift from the catalog. RFC-008 retires them.
- **Operator aliases** are pure **bindings, zero content**: a personal name
  → (server, graph, stored-query *name*, arg mapping, defaults). An alias
  that carries content competes with the catalog; an alias that references
  a name composes with it.

The three senses of "global", resolved by this split:

1. **Across graphs/servers** — preserved and strengthened: today's aliases
   are "global" only within one per-directory config file; operator
   aliases live in one `$HOME` file, each binding self-contained, usable
   from any cwd.
2. **Across operators (team-shared shorthand)** — deliberately *no alias
   mechanism*: the shared name IS the stored query's catalog name. A team
   that wants a shorter shared name renames the query in `cluster.yaml`
   (reviewed, one name). A parallel team-alias namespace would be two
   shared names for one thing — pure drift surface.
3. **Across machines** — dotfile the one operator file; bindings carry no
   local-file dependencies.

Collision rule during the RFC-008 window: a legacy file-alias with the
same name **wins**, with a warning naming both definitions — consistent
with §D3's legacy-outranks-operator ordering.

### D3. Precedence and the merge rule

The end-state cascade is short, because the team surface (cluster config)
deliberately carries **no operator-resolvable keys** — no actor, no tokens,
no output preferences. Identity can never come from a checkout:

```
flag  >  env  >  operator config  >  built-in
```

During RFC-008's deprecation window, a legacy `omnigraph.yaml` slots in
between env and operator config (its keys win over operator defaults,
preserving today's behavior for unmigrated setups) — with the §D5
credential inversion: **credentials and endpoint definitions never come
from a legacy/checkout file when an operator-layer definition exists for
the same server name.**

Merging is **key-level**: scalars override per key; maps (`servers:`,
`aliases:`) merge per *entry*, and entries merge per *field* (finding #13 —
`merge_map` replacing whole entries silently dropped sibling fields).

Concretely for the two flows this slice touches:

- **Actor**: `--as` > legacy `cli.actor` (window only, unchanged semantics)
  > `operator.actor` > none (commands that need an actor keep failing
  loudly).
- **Output format**: `--format` > legacy default (window only) >
  `defaults.output` > `table`.

### D4. Credentials: keyed by server name, by-reference always

Adopted from RFC-002 §5 unchanged, minus the keychain (a later source in
the same chain). For a server named `<name>`, the resolution chain is:

1. `OMNIGRAPH_TOKEN_<NAME>` (uppercased, `-`→`_`) — explicit env, wins.
2. `[<name>]` section in `~/.omnigraph/credentials` (INI-style, `0600`;
   the loader refuses a group/world-readable file).
3. The legacy pair — `bearer_token_env` + `auth.env_file` — exactly as
   today, for configs that already use it.

No inline secrets in any YAML file, anywhere (the existing invariant 12
posture extended to disk). A future `omnigraph login <name>`
writes/rotates one section of the credentials file via temp + rename
(finding #7: every operator-layer write is atomic), creating it `0600`.

### D5. The trust boundary (the security findings, made structural)

Findings #4, #5, #6 share one root cause: a file that arrives with a
*repo checkout* could redirect where requests go and what secrets they
carry. In the end state this is closed by construction — cluster config has
no server/credential keys at all, and the operator surface never comes from
a checkout. The rules below therefore govern the **RFC-008 window** (while
legacy `omnigraph.yaml` still loads) and stand as the permanent law for any
future checkout-supplied surface:

1. **A checkout-supplied file may *reference* a server by name; it may not
   *redefine* an operator-defined server.** If a legacy `./omnigraph.yaml`
   declares `servers.prod.url` and `~/.omnigraph/config.yaml` also defines
   `prod`, the operator definition wins and the CLI warns about the
   shadowed entry. A legacy-only server name keeps working (compat), but
   the keyed-credentials chain (§D4 steps 1–2) never resolves for it —
   only the legacy explicit `bearer_token_env` does. Net effect: a
   malicious checkout cannot point `prod` at an attacker host and harvest
   the operator's `prod` token.
2. **`auth.env_file` keeps auto-loading (compat), but checkout-layer
   env-files cannot *override* variables already set in the process or by
   the operator layer** — first-set-wins, operator-before-checkout (the
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

1. **PR 1 — the operator file + identity** *(landed: #196)*. Loader for
   `~/.omnigraph/config.yaml` (+ `OMNIGRAPH_HOME`, `~`-expansion, warn-only
   unknown keys), `operator.actor` joining the `--as` cascade,
   `defaults.output` joining the format cascade, `OMNIGRAPH_CONFIG` env for
   the CLI's `--config`. Docs: `cli-reference.md` gains the two-surface
   table.
2. **PR 2 — keyed credentials** *(landed)*. `servers:` in the operator layer, the
   §D4 chain (env + credentials file), the §D5 trust rules, and
   `omnigraph login <name>` (atomic write, `0600`). Legacy mechanisms
   untouched and tested-as-untouched.
3. **PR 3 — operator targeting.** `--server <name>` on remote-capable
   commands and `aliases:` in the operator layer (server + graph + query +
   default params), resolving through operator-defined servers. This is
   the *bridge* toward RFC-002's locator — multi-server addressing in a
   safe, minimal form without the `GraphLocator` rework — and the
   replacement RFC-008 needs before legacy aliases can migrate.

RFC-008's deprecation stages begin only after PRs 1–2 are on main: the
operator surface must exist before `config migrate` has somewhere to move
keys to.

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

**RFC-008 is the other half of this design**: this RFC builds the operator
surface; RFC-008 retires the mixed-ownership file
([rfc-008-deprecate-omnigraph-yaml.md](rfc-008-deprecate-omnigraph-yaml.md)),
leaving exactly two config surfaces — cluster (team) and operator (person).
Every mention of `omnigraph.yaml` in this RFC describes the deprecation
window only. Sequencing couples them: RFC-007 PRs 1–2 land first, then
RFC-008's migration stages run against them.

RFC-002 remains the umbrella architecture. This RFC implements its §2
(layered config, global-first), §4 (file naming / one dir), and §5
(credentials) in their minimal load-bearing form, and explicitly defers §1
(`GraphLocator`/targets), §3 (roles), and the State layer. If/when the
locator work resumes, it builds on these layers rather than re-landing
them. RFC-002's header should gain a pointer here once this merges.
