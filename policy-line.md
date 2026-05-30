# Policy enforcement — reasoning + validation guide

A record of *why* the Cedar policy work was done the way it was, *what* changed,
and *how you can check it*. Written for review; safe to delete once merged.

Date: 2026-05-30. Author: Claude (with Andrew).

---

## 1. The problem

dev-graph had a Cedar `policy.yaml`, but it was enforced **only for local CLI
writes**. The preview server (`graph.modernrelay.ai`) ignored it entirely:

- The deployed `omnigraph-server` is configured by env vars from
  `/etc/omnigraph/server.env`. The host's `omnigraph.yaml` (hardcoded
  `policy: {}`) was **never read** — the container had no volume mount and got
  no `--config`.
- The image entrypoint (`docker/entrypoint.sh`) had two **mutually-exclusive**
  startup branches. When `OMNIGRAPH_TARGET_URI` was set it ran
  `omnigraph-server <uri> --bind …` and **never** passed `--config`, so the
  only path that loads a policy file was unreachable.
- Net result: the server ran in **DefaultDeny** (tokens present, no policy) —
  any valid token could `read`; all writes blanket-403'd. The Cedar rules were
  never consulted.

Goal: make the server enforce the dev-graph policy (the canonical source) for
remote writes, **without** regressing the existing release/cutover mechanism.

## 2. Key findings that drove the design (verified in source)

- `crates/omnigraph-server/src/config.rs` — `resolve_target_uri` returns an
  explicit positional URI *first*, so a positional URI wins over any config
  `graphs:` block. ⇒ a config file can safely carry **policy only**.
- Same file — `policy.file` resolves **relative to the config file's
  directory**; `policy.tests.yaml` is its sibling.
- The server's runtime-state classifier: tokens + a policy file ⇒
  **PolicyEnabled** (every request Cedar-checked, including `read`).
- `docker/entrypoint.sh` was the actual gate (the URI/config branches were
  either/or).
- The instance role already has S3 read on the repo bucket ⇒ **no IAM change**
  needed to fetch a policy file from S3.

## 3. The decision (Strategy B)

Make the entrypoint **compose** `OMNIGRAPH_TARGET_URI` + `OMNIGRAPH_CONFIG`: the
URI keeps coming from the env var, and `--config` is added only to supply the
policy. Chosen over the alternative (migrate the release pointer into a mounted
`omnigraph.yaml`) because it is purely additive, leaves `cutover.sh` / the URI
mechanism untouched, and fixes the entrypoint's real limitation. Cost: one
server-image rebuild.

Other deliberate choices:

- **Policy is server config, not graph data** → published to a *stable* S3
  prefix (`repos/dev-graph/config/`), decoupled from dated data releases, so it
  can change without a reseed (`apply-policy.sh`).
- **Fail-closed**: a missing `policy.yaml` makes the server refuse to start
  rather than silently ship unprotected (with a short S3-fetch retry).
- **`act-aaron` / `act-bruno` / `act-munip`** hold tokens but are left
  **deny-all** by your decision (2026-05-30). Documented inline in `policy.yaml`
  so it doesn't read as an oversight. ⚠️ They are real teammates in DEPLOY.md's
  roster — they get fully locked out the moment the server flips to
  PolicyEnabled. Confirm before cutover.
- Actor IDs are uniformly `act-*` to match the `omnigraph/server/bearer-tokens`
  secret keys. Role comes from group membership, not the id prefix.

## 4. What changed (3 PRs, none deployed)

| Repo | PR | Branch | Contents |
|---|---|---|---|
| dev-graph | [#1](https://github.com/ModernRelay/dev-graph/pull/1) | `feat/cedar-policy` | `policy.yaml`, `policy.tests.yaml`, `omnigraph.yaml` wiring, doc rows |
| omnigraph | [#129](https://github.com/ModernRelay/omnigraph/pull/129) | `feat/server-config-entrypoint` | entrypoint composes URI+config; `entrypoint_test.sh` + CI job; deployment.md |
| omnigraph-platform | [#12](https://github.com/ModernRelay/omnigraph-platform/pull/12) | `feat/server-policy-delivery` | `user_data.sh` (fetch+mount+`OMNIGRAPH_CONFIG`+`policy.file`), `compute.tf` vars, `reseed-graph.sh` publish, `apply-policy.sh`, preview-deploy.md |

The policy bundle is **canonical in dev-graph**; platform delivers it, omnigraph's
entrypoint loads it.

## 5. How to validate

### Now (no deploy needed)

```bash
# Policy is internally consistent and the rules say what we think:
cd ~/code/dev-graph && omnigraph policy validate && omnigraph policy test   # 6 actors, 19 cases

# Spot-check intent:
omnigraph policy explain --actor act-hermes --action change --branch main        # deny
omnigraph policy explain --actor act-hermes --action change --branch review/x     # allow
omnigraph policy explain --actor act-andrew --action change --branch main         # allow
omnigraph policy explain --actor act-aaron  --action read   --branch main         # deny (unknown actor)

# Entrypoint composition is correct + backward-compatible:
cd ~/code/omnigraph && sh docker/entrypoint_test.sh    # 5 cases pass
```

Review the three PR diffs. The load-bearing lines to eyeball:
- `omnigraph/docker/entrypoint.sh` — the `${OMNIGRAPH_CONFIG:+--config …}` add.
- `omnigraph-platform/infra/templates/user_data.sh` — `policy.file`, the S3
  fetch (fail-closed), the `-v /etc/omnigraph:/etc/omnigraph:ro` mount, and
  `OMNIGRAPH_CONFIG` in both `server.env` heredocs.
- `dev-graph/policy.yaml` — group membership + the intentionally-denied note.

### After deploy (the real proof)

Merge #1 + #129 → package the resulting main SHA to ECR → merge #12 →
publish policy to S3 → `terraform apply` → `cutover.sh`. Then, against
`graph.modernrelay.ai` with real bearer tokens:

| Token | Action | Expect |
|---|---|---|
| `act-andrew` (maintainer) | mutate/change on `main` | allow (2xx) |
| `act-hermes` (agent) | change on `main` | 403 |
| `act-hermes` (agent) | change/create on `review/*` | allow |
| `act-aaron` (no rule) | read on `main` | 403 (denied even on read) |
| maintainer / agent | read on `main` | allow (no regression) |

And `journalctl -u omnigraph-server` should show a PolicyEnabled startup with no
policy-compile error.

## 6. Open risks / watch-items

- **Lockout**: `act-aaron`/`act-bruno`/`act-munip` lose all access at cutover.
- **First activation needs `terraform apply`** (instance replacement) so the
  mount + `OMNIGRAPH_CONFIG` land; thereafter `apply-policy.sh` suffices.
- **Package the post-merge SHA**: the entrypoint fix must be in the image you
  cut over to — package #129's merged main SHA, not an older one.
- `act-joan` / `act-devin` are in the policy but have **no token** — they can't
  authenticate remotely (local/seed only).
