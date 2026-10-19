# Named targets

A target names one place a command runs against, and how to reach it: a graph
or a cluster root in storage, or a server with the credential its requests
carry. Commands select one with `--target NAME`, or use `defaults.target` when
they name no address.

Targets live in the operator configuration, `~/.omnigraph/config.yaml` (or the
directory `OMNIGRAPH_HOME` names), beside the existing `servers:`, `profiles:`
and `clusters:` entries, which keep working.

```yaml
target_config_version: 1
defaults:
  target: staging
targets:
  local:
    kind: direct
    scope: graph
    uri: file:///Users/me/company.omni
  ops:
    kind: direct
    scope: root
    uri: /srv/omnigraph/cluster
    default_graph: knowledge
  staging:
    kind: server
    endpoint: https://staging.example
    credential: staging
    default_graph: knowledge
  public:
    kind: server
    endpoint: https://public.example
    auth: anonymous
```

`target_config_version: 1` is required once the file has a `targets:` entry or
`defaults.target`. Each entry is one complete binding:

| Kind | Fields |
|---|---|
| `direct` with `scope: graph` | `uri`: an absolute path or a `file://` URI of one standalone graph |
| `direct` with `scope: root` | `uri`: an absolute path or a `file://` URI of a cluster's storage root; optional `default_graph` |
| `server` | `endpoint`: an `http://`, `https://` or `unix:///path.sock` address with no user, password, query or fragment; `credential: OWNER` or `auth: anonymous`; optional `default_graph` |

An unknown field, an unsupported `target_config_version` or a malformed name
refuses the whole file. An incomplete entry refuses only when a command selects
it (`target_invalid`), so it never blocks another target.

A direct target cannot name cloud storage (`s3://`, `az://`) yet. Its endpoint
and account come from the environment when the store opens, and this format
cannot pin them in the binding, so such an entry refuses with `target_invalid`
rather than name a destination the environment still chooses. Address cloud
storage with `--store` or `--cluster`.

## Selecting a target

`--target NAME` is a complete address: it refuses `--server`, `--store`,
`--cluster`, `--profile` and a positional URI beside it, and its `uri` or
`endpoint` is used as written, never looked up under `clusters:` or `servers:`.
`--graph` still selects a graph within a server or cluster-root target,
overriding its `default_graph`.
When a command names no address and no profile, `defaults.target` applies.
`defaults.target` refuses beside `defaults.server`, `defaults.store` or
`defaults.default_graph`, and, for a command that names no address, beside a set
`OMNIGRAPH_PROFILE`.

A direct graph target serves graph commands and direct maintenance; a
cluster-root target serves direct maintenance as `--cluster` does, and `queries`
and `policy`; a server target serves graph and served commands. `schema apply`,
`schema upgrade-system-columns` and `upgrade` take only a direct graph target.
A command that cannot use the selected target's kind refuses it with
`target_kind_unsupported`, and refuses a profile of that kind too. `queries` and
`policy` select their cluster by the same rules as every other command, and
keep their explicit `--graph` filter: they ignore a target's `default_graph`. `init`
and the `cluster` commands do not take `--target` yet.

## Credentials

A server target's `credential` names the owner its token is stored under:
`OMNIGRAPH_TOKEN_<OWNER>` (for owner `staging`, `OMNIGRAPH_TOKEN_STAGING`), else
the `[staging]` section of the `0600` credentials file. The owner is bound to the
target's endpoint, and the CLI sends the token only to URLs under that endpoint.
One owner serves one endpoint, and one endpoint has one owner: the file refuses
two owners for an endpoint, an owner at two endpoints, an anonymous target beside
a keyed one at the same endpoint, and a `servers:` name used as an owner at
another address. A target may use a `servers:` entry's name as its owner at that
entry's `url`.

```bash
echo "$TOKEN" | omnigraph login --target staging   # store under the target's owner
omnigraph logout --target staging
```

With `--json` they report the target and its owner,
`{"target":"staging","owner":"staging","credentials_path":"…","declared":true}`
(`logout` omits `declared`).

`omnigraph credential get --target NAME --json` (else the default target)
hands the token to another program, as `gh auth token` does:

```json
{"protocol":1,"target":"staging","owner":"staging","endpoint":"https://staging.example","scheme":"bearer","token":"…","source":"credentials_file"}
```

Without `--json` it prints the token alone. It refuses when stdout is a terminal
(`credential_terminal`), for a direct target (`target_kind_unsupported`), for an
anonymous server (`credential_anonymous`) and when no token is stored
(`credential_missing`). `source` is `environment` or `credentials_file`.

## What you may do at a target

```bash
omnigraph access --target staging --branch main --json
```

```json
{"protocol":1,"target":"staging","source":"server","actor":"act-me","graph_id":"knowledge",
 "graph_wide":["read","export"],"branch":"main","target_branch":null,"actions":["read","export","change"]}
```

`access` names the actor and the actions the graph's policy grants it:
`graph_wide`, then with `--branch` the actions on that branch (`read`, `export`
and `change` there, and the branch-targeted ones with it as the target), and
with `--target-branch` too `branch_create` and `branch_merge` from one into the
other. A server target asks the server's
[access route](../operations/policy.md#learn-your-access) (`source: server`),
with the target's token. For direct storage the CLI decides itself
(`source: direct`) for the actor `--as` or `operator.actor` names:

- A cluster graph, addressed by its cluster or by its store
  (`<root>/graphs/<id>.omni`), answers by the bundle the cluster applies to it.
  With a bundle, nothing is allowed without an actor. With none, every action is
  allowed, with or without an actor. A graph the cluster does not apply refuses
  with `not_found`, and one it quarantined with `graph_unavailable`, as a server
  refuses them.
- A standalone graph carries no policy, so once its store opens every action is
  allowed, as direct writes do; its `graph_id` is `default`. A store that holds
  no graph refuses with `not_found`.

`access` also accepts `--server`, `--store`, `--cluster` and `--profile`.

## Listing and editing targets

```bash
omnigraph target list --json
omnigraph target show staging --json
echo '{"kind":"server","endpoint":"https://staging.example","credential":"staging"}' \
  | omnigraph target add staging --json
omnigraph target remove staging --json
```

`target list` prints every target without a secret and without a network
request: each target's address, `default_graph`, `auth` (`token` or
`anonymous`), `credential` owner and `binding`, a digest of where the target
points and how it authenticates, which changes when either does; a direct
target's storage is digested in its normalized spelling, so `/data/g` and
`file:///data/g` digest alike. A direct target carries `local_servers`, empty
today. An incomplete entry is listed with its `error`, and its address without
user, password, query or fragment. Aliases are not listed, since they still name
`servers:` entries. `target show` prints one target in the same shape.

`target add NAME` reads one entry as JSON on stdin, with the fields the file
holds. It refuses a name that exists (`target_exists`) and an entry that is not a
complete, consistent binding (`target_invalid`). `target remove NAME` refuses an
unknown name (`target_unknown`) and the default target (`target_is_default`), and
keeps the target's stored credential; remove that with `omnigraph logout OWNER`.

Both change only that entry's lines and keep the rest of the file's text,
comments included. They add `target_config_version: 1` when the file lacks it,
never change `defaults.target` and never store a secret. Each holds an exclusive
lock on `config.yaml.lock` while it edits, checks that the edited file loads with
exactly the expected targets and every other key unchanged, and replaces the file
by rename. Off Unix there is no such lock, so both refuse with
`config_not_editable`; edit the file by hand on that platform. A file it cannot
edit that way, such as one whose `targets` is a flow mapping or indented with
tabs, refuses with `config_not_editable`; edit it by hand.

Each refusal carries its `code`, and the `target` and `credential` commands
report an `outcome` of `not_started`, except an edit that fails after it
replaced the file, which reports `unknown`; see
[failed commands](../operations/troubleshooting.md#failed-data-write-commands).
`omnigraph version --json` reports the output versions as `protocols.target`,
`protocols.credential` and `protocols.access`.
