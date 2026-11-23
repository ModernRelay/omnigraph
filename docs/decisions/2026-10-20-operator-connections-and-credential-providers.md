# Operator profiles and credential providers

**Decides:** the operator file holds `profiles` for managed service and organization access and `targets` for resource access, and each managed profile and each server target owns exactly one authentication provider and one identity.
**Contract:** keeps E6
**Code:** `crates/omnigraph-cli/src/operator_schema.rs`, `crates/omnigraph-cli/src/operator_routing.rs`, `crates/omnigraph-cli/src/credential_sources.rs`, `crates/omnigraph-cli/src/operator_transport.rs`, `crates/omnigraph-cli/src/commands/targets`

## Context

One person may hold several organizations, several identities in one
organization, a CI workload and standalone servers. Account-level work (sign
in, list clusters, manage members) has no cluster; resource work has one. A
single active account, a credential found by fallback, or a destination taken
from the most recent login can send a token to the wrong place or act as the
wrong principal. The file needs one owner per credential and a strict grammar
that nothing ambient can change.

## Decision

- The file is `~/.omnigraph/config.yaml`, or `config.yaml` under
  `OMNIGRAPH_HOME`, with `version: 1`. `--operator-config FILE` replaces it for
  one run. Files are never merged, included or found by searching parent
  directories.
- The whole document is validated offline: unknown fields and versions,
  duplicate keys, YAML tags, anchors, aliases and merge keys refuse. Credentials,
  helpers and CA files are checked only for the selected operation.
- A profile has `api`, `organization_id` and one `auth`. A managed target pins a
  profile, `cluster_id` and `cluster_incarnation`; without `endpoint` it is
  control-only. A server target owns `endpoint` and one `auth`. A direct target
  has `scope: graph|root` and an absolute local `file://` URI.
- Account operations select `--profile`, then `defaults.profile`; resource
  operations select `--target`, then `defaults.target`. The selectors are
  mutually exclusive and neither default infers the other. A managed target
  always uses its pinned profile.
- `auth` is one tagged provider: `omnigraph` (device login, managed only),
  `token` with exactly one source (`store`, `env` or `file`), `exec` (argv
  helper, bounded time and output, versioned JSON protocol),
  `oauth_client_credentials` (managed only) or `anonymous` (server only). A
  missing source fails; there is no fallback. A different identity is a
  different profile or target.
- No token, refresh token or private key appears in YAML. Persistent login
  fails when secure storage is unavailable; there is no plaintext fallback.
- Managed access exchanges the profile's control credential for a
  cluster-bound data credential; a control or refresh token never reaches a
  graph endpoint.
- `credentials set|status|remove|export` take one explicit owner and ignore
  defaults. `status` makes no network call and runs no helper. `export` writes
  only to a pipe.
- Endpoints require HTTPS, except an anonymous loopback server. Credentials are
  scoped to the endpoint's base path. Proxy and CA settings are declared per
  endpoint, captured once and not inherited; ambient proxy variables are read
  only under `proxy: {mode: environment}`.
- `target add --profile --cluster` saves the service-attested identity and
  endpoint; `target refresh` changes the endpoint only for the same
  incarnation. Requests never follow redirects or adopt a changed endpoint.
- `config rename-profile` and `rename-target` keep credential custody;
  renaming a YAML key by hand does not move it.

## Rejected

- **Account entries as targets:** keeps one selector but puts tenant
  configuration in resource listings and repeats it in every cluster target.
- **One global active account:** fails for several organizations, CI and
  concurrent runs; a destination taken from the latest credential is unsafe.
- **A generic graph of services, accounts, identities and transports:** no
  reuse case justifies it; one profile reference per target is enough.
- **`kind: byoc`:** deployment placement is not a client access protocol.
- **Discover the endpoint on every request:** breaks offline use and
  destination stability; pin at registration and refresh explicitly.
