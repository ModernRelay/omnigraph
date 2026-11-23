# Named targets and CLI destination selection

**Decides:** every CLI operation that reaches a backend resolves exactly one destination, `--target NAME|ADDRESS` or else `defaults.target`, into an immutable binding before credentials or effects, and refuses rather than falls back.
**Contract:** keeps E4, E6
**Code:** `crates/omnigraph-cli/src/scope.rs`, `crates/omnigraph-cli/src/planes.rs`, `crates/omnigraph-cli/src/targets.rs`, `crates/omnigraph-cli/src/invocation.rs`, `crates/omnigraph-cli/src/helpers.rs`
**Cited as:** RFC 0059

## Context

A CLI that chooses a destination through separate flags per access kind
(`--server`, `--store`, `--cluster`, `--managed`), URI schemes, environment
profiles, folder context and a bundle's `storage:` field gives each its own
applicability. Two commands that look alike can reach different backends, and a stale folder
or environment can redirect a write. A destination is an authority: a server
resolves the actor from a token, direct storage reads live storage with a
self-declared actor. Choosing between them must be visible, fixed for the run,
and never guessed.

## Decision

- A target is a complete binding in the operator file: its kind (`managed`,
  `server`, `direct`), scope, address and authentication owner. A nickname is
  not an identity.
- Selection is `--target` then `defaults.target`; otherwise `target_required`.
  An empty, unknown or invalid explicit target errors and never falls through
  to the default. Defaults and aliases take registered names only.
- `--target` also takes an inline address: a local path, a `file://` URI, or an
  `s3://` cluster root for deployment, recovery, maintenance and clone
  commands. A value containing `/` or `:` is an address; a bare word is a name.
  Remote endpoints have no inline form. `init URI` is the positional inline
  form and conflicts with `--target`.
- The command line alone decides direct graph versus root scope; nothing probes
  storage to classify an address. No sole-graph discovery fills a missing
  `--graph`.
- `--config DIR` names deployment source files and never a destination. Bundle
  `storage:` is an equality assertion against the selected root. Whether
  `--config` was typed or defaulted is read from Clap's value source.
- `planes.rs` declares what each command requires; an unsupported kind, mode or
  flag refuses before credential acquisition or backend access, never reroutes.
- The resolved target, its selection source and the operator-file path go to
  stderr before backend mutations. `--quiet` is removed so nothing suppresses
  that line.
- A destructive write (`cleanup`, overwrite `load`, `branch delete`) against a
  non-local destination needs `--yes`; without it a TTY prompts and anything
  else refuses.
- A deployment ID without explicit `--config` selects source-free lookup or
  reconciliation, never a fresh apply. `--writers-stopped` attests safety and
  never selects a mode. A managed apply runs the exact preview it names; it
  never replans or pushes.
- Retired selectors (`--server`, `--store`, `--cluster`, `--managed`,
  `--direct`, `--uri`, `--quiet`, `use`, `profile`) refuse at parse time, with
  no compatibility adapter.

## Rejected

- **Keep the overlapping selectors and add managed profiles:** command families
  keep different selection rules, which is the defect.
- **A selection command or current-target state (`use`):** a command would then
  depend on an earlier session; a config default plus a per-run override covers
  it without hidden state.
- **Require registration for every destination:** adds friction to scratch
  graphs and one-off commands without making the named address safer.
- **Infer the access path from a URI scheme or a nickname:** one name could
  switch between served and direct authority; kind and scope are declared and
  announced instead.
- **Use `--writers-stopped` as a mode selector:** conflates a safety
  attestation with intent.
