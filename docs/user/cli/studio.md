# Open Studio

`omnigraph studio` opens [OmniGraph Studio](https://github.com/ModernRelay/omnigraph-studio)
on a configured server target or managed cluster target, prints the link it
opened, and exits:

```bash
omnigraph studio --target staging
omnigraph studio --target staging --graph knowledge --json
omnigraph studio --target production
```

The command reads the operator configuration and nothing else: it sends no
request to a server or a control plane, reads no credential and starts no
process that serves a graph. For a server target, Studio connects to the
server itself and obtains the target's credential through the CLI when the
target has one. For a managed cluster target, Studio reaches the cluster with
its own sign-in to the control plane; the CLI's login is not used.

## Targets

| Target | Result |
|---|---|
| Server with a token or helper `auth` provider | Opens Studio on that server |
| Server with `auth: {provider: anonymous}` | Opens Studio on that server |
| Managed cluster | Opens Studio on that cluster, identified by its control plane, organization, cluster ID and incarnation |
| Direct graph or root, or an inline path | Refuses with `target_kind_unsupported` |

Select the target with `--target NAME` or `defaults.target`. `--graph ID` asks
Studio to open that graph; without it Studio opens the target's
`default_graph`, or lets you choose one when the server lists its graphs.
The two link forms carry that graph differently; see [the link](#the-link).
`--profile` and `--as` are refused: Studio acts as the actor its credential
resolves to, and a profile alone names no cluster.

## The link

A server target is named, and a managed cluster target is identified. A link
of either form carries no secret, never names a path or an executable, and
opening one runs no write.

### A server target

```text
omnigraph-studio://open?target=NAME&binding=sha256:HEX&home=sha256:HEX[&graph=ID]
```

| Parameter | Meaning |
|---|---|
| `target` | The registered target name |
| `binding` | A digest of what the target resolves to: its kind, endpoint, `auth` provider with its source (the store, an environment variable's name, a file's path or a helper's command), and `transport`. A token's value never enters it, so a rotated token keeps the binding; `default_graph` does not enter it either |
| `home` | A digest of the operator file the command read, at its canonical location: `--operator-config FILE`, else `config.yaml` under `OMNIGRAPH_HOME` or `~/.omnigraph` |
| `graph` | The graph `--graph` named, only when given. The target's `default_graph` is never in this link: Studio reads it when it resolves the target |

Equivalent spellings of one endpoint or path give the same digest.

Studio resolves the target itself, through the CLI in its own operator home,
and compares. `omnigraph target inspect NAME --json` reports the same `home`
digest and, for a server target, the same `binding` digest. Studio refuses a
link made in another operator home, for example under a one-off
`OMNIGRAPH_HOME`, and a link whose binding no longer matches because the
target changed since the link was made.

With `--json` the command prints the link and the facts it was built from:

```json
{"protocol":1,"target":"staging","binding":"sha256:…","home":"sha256:…","link":"omnigraph-studio://open?target=staging&binding=sha256:…&home=sha256:…"}
```

### A managed cluster target

```text
omnigraph-studio://open?api=ORIGIN&organization=ORG_ID&cluster=CLUSTER_ID&incarnation=INCARNATION[&graph=ID]
```

| Parameter | Meaning |
|---|---|
| `api` | The control plane the target's profile names: its `api`, without a trailing slash, percent-encoded |
| `organization` | The profile's `organization_id` |
| `cluster` | The target's `cluster_id` |
| `incarnation` | The target's `cluster_incarnation`, so a cluster deleted and created again under the same name is not opened in its place |
| `graph` | The graph `--graph` named; else the target's `default_graph`, when it has one; else absent |

The link has no `target`, `binding` or `home`: it says which cluster, not
which name this operator file gives it, so it opens in a Studio that reads
another operator file, or none. That is also why `graph` differs from the
server form: Studio resolves no CLI target for this link and so has no
`default_graph` to read, and the link carries the target's own when `--graph`
names none. Studio opens the cluster only through a
control plane it already knows, with its own sign-in there, and only when
that control plane still lists a cluster with this ID and incarnation in the
organization. The command checks nothing against the control plane: a
cluster that has since been deleted, or an organization you have left, is
Studio's to report.

```json
{"protocol":1,"target":"production","api":"https://platform.omnigraph.dev","organization":"org_…","cluster":"…","incarnation":"…","link":"omnigraph-studio://open?api=https:%2F%2Fplatform.omnigraph.dev&organization=org_…&cluster=…&incarnation=…"}
```

`target` in this report is the name the command resolved; the link does not
carry it.

### The protocol number

`protocol` is `protocols.studio` in [`omnigraph version --json`](../install.md#verify).
It versions both link forms, this report, and the digests `target inspect`
reports. The managed cluster form was added within protocol 1: every link,
report and digest for a server target is unchanged, and a Studio that
predates the form refuses a link without a `target` instead of misreading it.
Without `--json` the link is the only line on stdout.

## Platforms

| Platform | How the link opens |
|---|---|
| macOS | `open -b com.modernrelay.omnigraph-studio LINK`: only the application with Studio's bundle identifier |
| Linux and other Unix desktops | The program of the desktop entry `xdg-mime query default x-scheme-handler/omnigraph-studio` names, run directly with the link in place of `%u` |
| Windows | The `omnigraph-studio` URL protocol, only when the registry holds one with an open command |

On Linux the CLI finds the named desktop entry in `$XDG_DATA_HOME/applications`
(default `~/.local/share/applications`), then in each `$XDG_DATA_DIRS/applications`
(default `/usr/local/share:/usr/share`); a `-` in its name may stand for a
subdirectory. It runs the entry's `Exec` command itself, without a shell,
replacing `%u` or `%U` with the link and dropping the other field codes. An
entry that is hidden, runs in a terminal, takes no URL through `%u` or `%U`, or
uses a form the CLI cannot run exactly as written is refused.

The CLI never hands the link to a generic opener such as `xdg-open`, which
passes a link it cannot place to a web browser, for example when no display is
set. When nothing opens the link, because Studio is not installed or no
handler is registered for its scheme, the command still prints the link, says
where to download Studio, and refuses with `studio_unavailable`. With `--json`
that refusal carries `link` and `install`. Studio registers its scheme when
installed on macOS; on Linux and Windows the command opens a link once Studio
registers a handler there.
