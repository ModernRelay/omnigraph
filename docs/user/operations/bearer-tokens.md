# Static bearer tokens

The server authenticates a request's `Authorization: Bearer` token against its
static tokens, its [signed data credentials](server.md#signed-data-credentials)
and its [OIDC resource identity](server.md#oidc-resource-identities-and-mcp).
This page covers the static tokens: where the server reads them, what it
refuses, and how the token file refreshes without a restart.

## Sources

| Environment variable | Value |
|---|---|
| `OMNIGRAPH_SERVER_BEARER_TOKEN` | One bearer token; actor is `default` |
| `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON` | Actor-to-token object, e.g. `{"act-alice":"secret-a"}` |
| `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` | Path to that JSON object, at most 256 KiB. On Unix a file other accounts can write is refused, and one they can read logs a warning; use `chmod 600`, or `defaultMode: 0400` on a Kubernetes secret volume |
| `OMNIGRAPH_SERVER_BEARER_TOKENS_REFRESH_SECONDS` | Seconds between reads of the token file, `1` to `3600`; unset or `0` reads it only at startup ([refresh](#refresh-static-tokens)) |
| `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` | Secrets Manager mapping; requires an AWS-enabled build: the container images, the release archives from v0.14.0 and every edge build, or a source build with `cargo build -p omnigraph-server --features aws` |

`OMNIGRAPH_SERVER_BEARER_TOKEN` combines with the file or the JSON object. When
both of those are set, the file is read and the JSON object is ignored. When
`OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` is set, Secrets Manager is the only
static source the server reads.

Every source refuses a blank actor, a duplicate token value and an actor name
starting with `principal:`, which is reserved for signed and OIDC identities
(`principal:<id>`), so a static token cannot match their policy rules.

Make every static token a random, high-entropy value, for example the output of
`openssl rand -base64 32`. The server keeps only each token's SHA-256 digest.
That protects a random token, but not a word or passphrase someone could guess.

A static source is configured when one of its variables is set, whatever it
holds, and a configured source makes the server require bearer authentication.
A token file holding `{}` therefore starts a server that refuses every static
token, also when `--unauthenticated` is passed. To run an open development
server, unset the static source variables. The mode is fixed at startup, so a
refresh changes which tokens are admitted, never whether one is required. See
[server startup modes](policy.md#server-startup-modes).

## Refresh static tokens

The server reads its static tokens at startup. To apply changes to the token
file without a restart, set `OMNIGRAPH_SERVER_BEARER_TOKENS_REFRESH_SECONDS`:

```bash
OMNIGRAPH_SERVER_BEARER_TOKENS_FILE=/etc/omnigraph/tokens/tokens.json \
OMNIGRAPH_SERVER_BEARER_TOKENS_REFRESH_SECONDS=5 \
omnigraph-server --cluster s3://customer/root
```

At startup the server then logs `static credential refresh enabled` with the
polled source and the interval. A binary without refresh ignores the variable,
so look for that line before you rely on refresh.

| Source | When the server reads it |
|---|---|
| `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` | At startup, and again every interval when refresh is on |
| `OMNIGRAPH_SERVER_BEARER_TOKEN`, `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON` | At startup only: process environment cannot change. Beside the file, the `default` actor stays fixed and the file's entries refresh around it |
| `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` | At startup only |

The variable refuses startup unless `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` is set
and `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` is not; the refusal names the
source the server selected.

Each read opens the path again, so a symlink swap is seen. The server applies a
change within one interval of the new content becoming visible at that path. On
Kubernetes the kubelet's own delay comes first; see
[Kubernetes](#kubernetes).

- A request admitted after the change is authenticated against the new tokens:
  an added token is admitted, and a removed one gets 401 with
  `error="invalid_token"`. Requests admitted before the change finish.
- A token whose value changed counts as a removal of the old value and an
  addition of the new one.
- The server logs each change once at `info`: the generation, the first eight
  hexadecimal digits of the token set's digest, the actor count, and the actors
  added and removed. It never logs a token.

A file that cannot be read or does not validate never changes the tokens in
force. This covers a missing or unreadable file, a file that is not a regular
file, a file other accounts can write, a file over 256 KiB, invalid JSON, and
any entry the startup checks refuse. The last valid tokens stay in force with no
expiry, so **while the file is invalid, revocation stops**. To revoke an
actor urgently while the file is invalid, deny it in the graph's policy with
`cluster apply`, which applies without a restart. The last valid tokens never
time out: a timeout would turn a broken Secret into a lockout of every static
token. The server logs one
warning for each refused content, or for each kind of read failure, until the
next successful read. A rewrite that changes no token (whitespace, key order)
changes nothing.

A valid file with no entries is applied: every static token is refused, and the
server warns that the static principal set is empty. This does not turn
authentication off. A configured source keeps bearer authentication required,
whatever it holds.

To confirm a change, call `GET /graphs/{graph_id}/access` with the added token
(200) or the removed one (401).

The server has no reload signal. Do not send `SIGHUP`: the server does not
handle it, so the process exits and leaves its cluster lock behind, and the next
start needs `cluster force-unlock`.

## Kubernetes

To apply token changes from a Kubernetes Secret without a restart, enable
refresh and:

- mount the Secret as a directory and point `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE`
  at the key inside it. Never mount it with `subPath`: a `subPath` mount never
  receives updates, so the server cannot see a change;
- set the volume's `defaultMode: 0400`. The server checks the file's mode at
  every read, after following the mount's symlink, and keeps the last valid
  tokens when another account can write the file;
- expect the kubelet's own delay (its sync period plus cache propagation)
  before the server's interval starts to count;
- write a complete file and swap it in atomically. A Secret volume already does
  this through its `..data` symlink. A partly written file reads as invalid
  content and is refused, which keeps the last valid tokens.

```yaml
volumes:
  - name: omnigraph-tokens
    secret:
      secretName: omnigraph-tokens
      defaultMode: 0400
containers:
  - name: omnigraph
    env:
      - name: OMNIGRAPH_SERVER_BEARER_TOKENS_FILE
        value: /etc/omnigraph/tokens/tokens.json
      - name: OMNIGRAPH_SERVER_BEARER_TOKENS_REFRESH_SECONDS
        value: "5"
    volumeMounts:
      - name: omnigraph-tokens
        mountPath: /etc/omnigraph/tokens
        readOnly: true
```
