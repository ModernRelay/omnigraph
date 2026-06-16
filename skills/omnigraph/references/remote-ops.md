# Remote Graph Operations

## Contents
- What's different about remote
- Verify after every write
- 504 Gateway Timeout
- Fork-branch 504 fingerprint
- Targeting a remote graph (`--server`, `login`)
- Version drift / `sync_branch()`
- `manifest_conflict` 409
- 429 Too Many Requests
- Duplicate risk on blind retry
- Reading large schemas safely
- Prevention checklist

When the graph URI is a remote endpoint (`omnigraph-server` behind ALB / CloudFront, bearer-authenticated) instead of a local S3 path, several CLI behaviors change in ways the local-storage workflow never exposes. This reference covers the failures and operational rituals specific to remote graphs.

## What's different about remote

A remote graph runs server-side. Every write executes on the server — staged per touched table, then published atomically as a **single manifest commit** guarded by a compare-and-swap on expected table versions — and is gated by a connection-level idle timeout (CloudFront defaults to ~30s). There is no separate "run" object to poll — write status is implied by the HTTP response (and verifiable via `commit list`). The local CLI is a thin client; it never sees the commit happen, only the HTTP response. That asymmetry is the root of every gotcha below.

| Local repo | Remote repo |
|---|---|
| CLI writes S3 directly | Server executes the write, publishes one atomic manifest commit |
| No connection timeout | ~30s idle timeout (CloudFront) |
| No admission control | Per-actor `429` + `Retry-After` on writes |
| `load` writes S3-backed storage directly | `load` is server-orchestrated — same command, one atomic commit |
| CLI exit code is authoritative | CLI exit code can lie — verify via `commit list` |

## Verify after every write

The CLI's exit code is **not authoritative on remote graphs**. The proxy can drop a response after the server has already committed. Always verify by comparing `main`'s head:

```bash
HEAD_BEFORE=$(omnigraph commit list --config X --branch main --json | jq -r '.commits[0].graph_commit_id')

# … run your load / mutate …

HEAD_AFTER=$(omnigraph commit list --config X --branch main --json | jq -r '.commits[0].graph_commit_id')

if [[ "$HEAD_BEFORE" != "$HEAD_AFTER" ]]; then
  echo "landed"
else
  echo "did NOT land — safe to retry"
fi
```

For a `load --from` that forks a review branch, also compare the new branch head's `graph_commit_id` against `main`'s. **Identical means the load didn't land — empty fork left behind.**

For pointed verification of a single record:

```bash
omnigraph export --config X --type <NodeType> | grep <slug>
omnigraph export --config X --type <EdgeType> | grep <slug>
```

## 504 Gateway Timeout: response lost, write status unknown

A 504 from the proxy means the server didn't respond within the idle timeout. Two server-side outcomes are possible — **the 504 alone cannot distinguish them**:

1. **Write completed and published** — landed, `main`'s head advanced. Common for small mutations finishing just past the 30s edge.
2. **Write still in progress** — will publish or fail soon. Re-check after a minute.

Always verify via `commit list` before retrying. Blind retry on append-only types creates duplicates.

## Fork-branch 504 fingerprint

`load --from <base>` creates the branch **before** loading data. A timed-out fork-load where the data didn't land leaves an empty branch at `<base>`'s head. Stale numbered branches (`feature-v2`, `-v3`, `-v4` …) all sitting at the same `graph_commit_id` as `main` are the fingerprint of prior 504-blocked attempts.

Find them by comparing each branch's head against `main`'s in `omnigraph branch list --config X --json`, then delete the empty ones.

## Targeting a remote graph: `--server` and `login`

`load`, `query`, and `mutate` all run against a remote `omnigraph-server` endpoint — there is no local-only restriction as of 0.7.0. Address an operator-defined server by name instead of pasting URLs and juggling tokens:

```bash
echo "$TOKEN" | omnigraph login intel-dev          # stores it in ~/.omnigraph/credentials (0600)
omnigraph load --server intel-dev --graph spike \
  --data delta.jsonl --from main --mode merge --branch staging
```

`--server <name>` resolves the URL from `~/.omnigraph/config.yaml` and the token via `OMNIGRAPH_TOKEN_<NAME>` or the credentials file. A token is only ever sent to the server it is keyed to. `--graph <id>` selects the graph on a multi-graph server.

## Version drift / `sync_branch()`

```
version drift on node:<Type>: snapshot pinned vN but dataset is at vM — call sync_branch() and retry
```

- `sync_branch()` is **not a CLI command** — it's a server-internal directive that leaked into the error text. Don't go looking for it.
- Cause: another actor committed to `main` between your CLI's snapshot pin and your `mutate` attempt.
- Usually self-resolves on retry — the next call re-pins.
- Calling `omnigraph snapshot` does **not** reliably re-pin for subsequent `mutate`s in the same session.
- If persistent, fall back to `load --from main` onto a fresh branch — a forked branch doesn't suffer from concurrent-commit drift on `main`.
- The cleaner, modern form of this conflict is a structured `manifest_conflict` **409** — see below.

## `manifest_conflict` 409 — stale snapshot, retry

When another actor commits to the same branch between your query's snapshot pin and your write, the server returns a structured **`manifest_conflict` 409** carrying `table_key` / `expected` / `actual`, rather than silently overwriting. Since v0.4.2 this is the form most concurrent update/delete/merge races take.

- **Retry it.** A 409 means your write was computed against a stale view and was rejected *before* committing — there is no partial state and no duplicate risk. Re-issue the same call; it re-pins to the new head.
- Concurrent `mutate` × branch-merge on the same target branch resolves to either success or a clean 409 depending on who wins the server's per-table queue — both outcomes are safe.

## 429 Too Many Requests — back off, then retry

The server applies **per-actor admission control** to every mutating endpoint (`mutate` / `load` / `schema apply` / branch create·delete·merge). An actor that exceeds its in-flight-request or estimated-byte budget gets a structured **HTTP 429** (`code: too_many_requests`) with a `Retry-After` header — instead of blocking unrelated actors behind a global lock.

- This is **not** a failed write — the write never started. Honor `Retry-After` and retry; it is always safe (no partial write, no duplicate risk).
- It's per-actor, so one noisy automation can't starve others. If you hit it constantly, batch less aggressively or space your calls out.
- Read-only endpoints are not admission-gated.

## Duplicate risk on blind retry

After a 504, never retry without verifying first. Different node kinds have different retry semantics:

| Kind | Retry safety |
|---|---|
| Pointer nodes (`Org`, `Person`, `Opportunity`, `Channel`, `Actor`, `ActionItem`, `Artifact`, `Meeting`, `Technology`, `Campaign`, `UseCase`) | ✓ Idempotent — `@key` upserts dedupe |
| Append-only nodes (`Signal`, `Claim`, `Decision`, `Event`, `Interaction`, `MarketingElement`, `Policy`, `Outcome`) | ✗ Duplicates on retry — verify before retrying |
| Edges | ⚠ No `@key`. Verify via `export --type <EdgeName>` + grep. Some simple edges dedupe server-side; don't rely on it. |

## Reading large schemas safely

Remote schemas can be large (tens of KB). Tools that cap stdout (~50KB is common) will truncate or duplicate the output silently — leading to memory-based answers from agents that look correct but reference nonexistent fields.

Always redirect to a file before reading:

```bash
omnigraph schema show --config X > /tmp/schema.pg
wc -l /tmp/schema.pg
```

Then read the file with offset/limit, not via piped stdout.

## Prevention checklist

- Keep mutations small. Single-node inserts finish well under the timeout.
- Prefer `mutate` over `load` for ≤ a handful of records.
- Always run `commit list` after a 504 before deciding to retry.
- For destructive or large-batch work, use `load --from main` onto a feature branch and verify the branch head before merging.
- Read large schemas via file redirect, not piped stdout.
- A `429` (throttle) or a `manifest_conflict` `409` (stale snapshot) is always safe to retry — the write never committed. Honor `Retry-After` on a 429.
