# Open Studio

`omnigraph studio` opens [OmniGraph Studio](https://github.com/ModernRelay/omnigraph-studio)
on a configured server target, prints the link it opened, and exits:

```bash
omnigraph studio --target staging
omnigraph studio --target staging --graph knowledge --json
```

The command reads the operator configuration and nothing else: it sends no
request to the server, reads no credential and starts no process that serves
a graph. Studio connects to the server itself and obtains the target's
credential through the CLI when the target has one.

## Targets

| Target | Result |
|---|---|
| Server with a token or helper `auth` provider | Opens Studio on that server |
| Server with `auth: {provider: anonymous}` | Opens Studio on that server |
| Direct graph or root, or an inline path | Refuses with `target_kind_unsupported` |
| Managed | Refuses with `target_kind_unsupported` |

Select the target with `--target NAME` or `defaults.target`. `--graph ID` asks
Studio to open that graph; without it Studio opens the target's
`default_graph`, or lets you choose one when the server lists its graphs.
`--profile` and `--as` are refused: Studio acts as the actor its credential
resolves to.

## The link

```text
omnigraph-studio://open?target=NAME&binding=sha256:HEX&home=sha256:HEX[&graph=ID]
```

| Parameter | Meaning |
|---|---|
| `target` | The registered target name |
| `binding` | A digest of what the target resolves to: its kind, endpoint, `auth` provider with its source (the store, an environment variable's name, a file's path or a helper's command), and `transport`. A token's value never enters it, so a rotated token keeps the binding; `default_graph` does not enter it either |
| `home` | A digest of the operator file the command read, at its canonical location: `--operator-config FILE`, else `config.yaml` under `OMNIGRAPH_HOME` or `~/.omnigraph` |
| `graph` | The graph `--graph` named, only when given |

Equivalent spellings of one endpoint or path give the same digest. A link
carries no secret, never names a path or an executable, and opening one runs
no write.

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

`protocol` is `protocols.studio` in [`omnigraph version --json`](../install.md#verify).
It versions the link, this report, and the digests `target inspect` reports.
Without `--json` the link is the only line on stdout.

## Platforms

| Platform | How the link opens |
|---|---|
| macOS | `open -b com.modernrelay.omnigraph-studio LINK`: only the application with Studio's bundle identifier |
| Linux and other Unix desktops | `xdg-open LINK`, only when `xdg-mime query default x-scheme-handler/omnigraph-studio` names a handler |
| Windows | The `omnigraph-studio` URL protocol, only when the registry holds one with an open command |

The CLI never hands an unregistered scheme to a generic opener, which would
pass the link to a web browser. When nothing opens the link, because Studio is
not installed or no handler is registered for its scheme, the command still
prints the link, says where to download Studio, and refuses with
`studio_unavailable`. With `--json` that refusal carries `link` and `install`.
Studio registers its scheme when installed on macOS; on Linux and Windows the
command opens a link once Studio registers a handler there.
