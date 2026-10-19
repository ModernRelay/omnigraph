# Local servers

`omnigraph serve` serves one direct [target](targets.md) on loopback, to
clients running as the same account on the same machine: an editor, a notebook
or another tool that speaks the [HTTP API](../operations/server.md). It is a
local server for you, not a deployment; to serve others, run
[`omnigraph-server`](../operations/server.md) from a cluster.

```bash
omnigraph serve --target local --json
omnigraph serve --target ops --as act-me --json
```

```text
omnigraph serve [--target NAME] [--write] [--reuse] [--port PORT] [--as ACTOR] [--json]
```

The target is `--target NAME`, else `defaults.target`. It must be a direct
target whose `uri` is a local path or `file://` root: a standalone graph
(`scope: graph`) or a cluster root (`scope: root`). A server target refuses with
`target_kind_unsupported`, since that server already serves it. An `s3://` or
`az://` target is not yet a complete binding and refuses with `target_invalid`.

## Access and actor

A local server is read-only: it opens its graphs read-only, and its engine and
its HTTP layer both refuse every change, branch control and commit control, the
HTTP layer with 403 `forbidden` before reading the request body. `--write` is
not available yet and refuses with `local_server_read_write_unavailable`, after
the actor check: a write whose outcome is unknown would still stop the server,
taking the graph away from its clients. Write through the CLI's direct commands
meanwhile.

The actor is `--as ACTOR`, else `operator.actor`. A standalone graph acts as
that actor, or as `default` when none is set; a cluster root requires one, and
a missing actor refuses with `actor_required`. A cluster root's local server
applies the cluster's own policies, stored queries, embedding-provider bindings
and external-Blob rules to its actor, as a deployment of that cluster would,
and takes no cluster admission, so `cluster apply` can run beside it. No
`/cluster/*` route is served, so schema and deployment changes stay with
`cluster apply`.

## Starting and readiness

The local server listens on `127.0.0.1`, on `--port` or a port the system
picks, with a bearer token generated for this run. Logs and a one-line banner
go to stderr. With `--json`, stdout carries one ready line once the server
handles signals and accepts connections:

```json
{"protocol":1,"id":"01K…","reused":false,"url":"http://127.0.0.1:52817","scope":"graph","graph_id":"default","access":"read_only","actor":"default","binding":"sha256:…","token":"…","record":"…/servers/01K….json"}
```

`token` is present only when stdout is a pipe or a file, never a terminal.
`graph_id` names the route a client addresses (`/graphs/default/…` for a
standalone graph, the target's `default_graph` for a cluster root, which may be
absent). `binding` is the digest of the target's binding. `omnigraph version
--json` reports this line's version as `protocols.serve`.

A start that fails before its ready line leaves no record and exits 1; a
refusal names its `code`, on stdout under `--json`.

## Records and reuse

Every running local server has a record: a JSON file, readable only by its
account, naming its id, URL, token, scope, root, access, actor, process id and
the CLI that started it. A same-account client attaches by reading it.
Records live in a directory only the account may use:

| Platform | Directory |
|---|---|
| macOS | `~/Library/Application Support/OmniGraph/servers` |
| Linux | `~/.local/state/omnigraph/servers` |

Other platforms cannot yet restrict a record to its account, so `serve`
refuses there with `local_server_records_unsafe`.

`OMNIGRAPH_LOCAL_SERVERS_DIR` names another directory. A directory another
account owns, or one with group or other access, refuses with
`local_server_records_unsafe`. So does a machine whose host identifier cannot be
read (`gethostuuid` on macOS, `/etc/machine-id` or `/var/lib/dbus/machine-id` on
Linux): a record names its machine, so that a home directory shared with
another machine never sends a client to that machine's port or removes its
record. `omnigraph version --json` reports the record format as
`protocols.local_server_record`.

Each record is held by its process for as long as it runs. A record whose
process is gone, killed included, is not live: readers ignore it and remove it.
A record without its lock file is not live either, and readers leave it.

There is at most one live local server per destination, which is the target's
scope and its root on this machine. A second start for the same destination
refuses with `local_server_exists`. With `--reuse` it prints the live server's
ready line, with `reused: true` and this target's `graph_id`, and
exits 0, unless that server cannot serve what this start asks for:

| Code | When |
|---|---|
| `local_server_access_mismatch` | `--write` asks for a read-write server and the live one is read-only |
| `local_server_actor_mismatch` | The live server of a cluster root acts as another actor |
| `local_server_cli_incompatible` | The live server speaks another HTTP contract than this CLI |

Starts of one destination are serialized: a start waits up to 60 seconds for
another start of it to publish its record, then refuses with
`local_server_starting`.

`omnigraph target list --json` and `target show NAME --json` name each direct
target's live local servers under `local_servers`, with their `id`, `url`,
`access` and `actor`, never their token; `local_servers` is absent when the
records directory cannot be read safely.

## Loopback only

A local server answers only requests whose `Host` is exactly
`127.0.0.1:PORT`, `localhost:PORT` or `[::1]:PORT` for its port, and refuses any
request that carries an `Origin` header, with 403 `forbidden`. A browser page,
including one whose name was rebound to `127.0.0.1`, therefore cannot reach it.
No response carries a CORS header, and every response carries
`Cross-Origin-Resource-Policy: same-origin`.

## Stopping

`SIGTERM` or Ctrl-C stops the local server: it stops admitting requests, lets
admitted work finish within the shutdown grace, removes its record and exits 0.
