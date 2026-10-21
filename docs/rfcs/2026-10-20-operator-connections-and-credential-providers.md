---
rfc: "2026-10-20-operator-connections-and-credential-providers"
title: "Operator profiles and credential providers"
track: maintainer
status: draft
implementation: in-progress
authors:
  - ragnorc
created: 2026-10-20
updated: 2026-10-21
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Operator profiles and credential providers

No backward-compatibility requirement. This is the clean-slate operator contract.

MUST, MUST NOT, SHOULD and MAY express requirements of the proposed implementation, not capabilities of the installed CLI. All CLI examples below are proposed interfaces. This document replaces the previous proposal; it does not modify the live operator configuration.

## Recommendation

Use two main configuration sections: `profiles` for managed service and organization access, and `targets` for concrete resource access. Each managed profile owns exactly one auth provider and one identity. Only standalone servers retain named auth maps. Resolve all references into one immutable invocation binding before sending credentials or performing an operation.

A managed profile is first-class operator configuration, not a cluster target. BYOC is deployment placement, not a new client access kind. Direct storage and standalone servers remain independent of managed profiles.

The common experience MUST be login, register a resource, then operate on it. Users SHOULD NOT need to hand-author authentication internals. Advanced providers add capability without making a simple managed login verbose. Authentication configuration selects credential acquisition; it never grants authorization.

## Design precedents

This revision replaces `connections` with `profiles`, managed target `connection`
with `profile`, `defaults.connection` with `defaults.profile`, and `--connection`
with `--profile`. Old fields and selectors are rejected, with no importer or
compatibility aliases. Each former managed auth identity becomes a separate
profile containing one provider object; managed `default_auth` is removed.
`config rename-profile FROM TO` replaces `rename-connection` and preserves
credential custody while updating target references and the profile default.
`config rename-auth server TARGET FROM TO` remains standalone-only.

These are design inputs, not claims that another product implements this specification.

| Precedent | Observed behavior | Decision for OmniGraph |
| --- | --- | --- |
| [Modal configuration](https://modal.com/docs/sdk/py/latest/config) | Named profiles, explicit override precedence, mutually exclusive complete authentication methods | Small generated configuration and strict provider variants; do not copy inline token storage |
| [Modal environments](https://modal.com/docs/guide/environments) | Resource environments are distinct from authentication profiles | Do not overload auth profiles with graph or deployment selection |
| [Supabase CLI](https://supabase.com/docs/reference/cli/supabase-login) | Login and project linking are separate operations | Profile login does not implicitly select or create a cluster |
| [Fly access tokens](https://docs.fly.io/security/tokens) | Automation tokens can be scoped to an app or organization | Workload credentials require actual server-side qualification and least privilege |
| [Wrangler authentication](https://developers.cloudflare.com/workers/wrangler/commands/general/) | Browser login and named authentication profiles | Multiple identities are supported independently from resource configuration |
| [AWS SSO](https://docs.aws.amazon.com/sdkref/latest/guide/feature-sso-credentials.html) | Sessions are distinct from account/role profiles | Service, tenant, principal and destination are separate concepts |
| [Kubernetes credential plugins](https://kubernetes.io/docs/reference/access-authn-authz/authentication/#client-go-credential-plugins) | A versioned helper response provides typed credentials and expiry | Provider adapters return typed credentials through a bounded contract |

OmniGraph deliberately does not copy ambient project-directory routing or an unrestricted credential fallback chain. Its multiple access paths and maintenance operations need explainable, frozen destination selection. Hosted-only CLI simplicity is useful ergonomically, but is not sufficient for standalone or BYOC operation.

## Evidence and limits

The CLI source inspected is commit `b692610c4aa31066cb6a9fd34830f56977288943`, matching the installed binary. The control-plane source is `ModernRelay/og-control-plane` at `2f61511`, fetched for this review. The deployed control-plane commit was not established; current repository code is not proof of live deployment qualification.

Four isolated probes of the installed CLI passed: a human-device account entry without an organization is accepted; a missing API is rejected; a managed cluster missing its incarnation is rejected; a cluster without a data endpoint is accepted for control operations. The probes used temporary operator homes and no network requests. Existing source tests were inspected, not executed. Future BYOC, private transport and workload scenarios below are design requirements, not completed implementation tests.

| Finding | Evidence |
| --- | --- |
| Existing account targets are deliberate, not a parser accident | CLI `targets.rs:550` validates them; RFC 0059 defines a target as the complete destination of any backend operation |
| Organization/account is the managed tenant, not the human identity | Control `auth.rs:287` resolves the verified provider subject and organization through enrolled memberships |
| First login need not know the organization ID | CLI `targets.rs:583` permits its omission for human-device account login; `managed.rs:344` requires it for resource operations |
| No baked-in production origin is used | CLI `targets.rs:557` requires `api`; missing-API probe rejected |
| Credentials are already distinct from target nicknames | CLI `managed/auth/renewal.rs:109` keys session custody by origin, provider and verified identity; data credentials also validate cluster and incarnation |
| Current sessions are selected per origin/organization | `managed/auth/renewal.rs:201`; logging in another user is not an independent named user profile today |
| Authentication does not grant graph permissions | Control `data_tokens.rs:434` checks cluster admission; signed identity carries identity, not graph action grants; server policy remains authoritative |
| Managed automation is not established by the CLI's automation-env option | Control `auth.rs:287` takes the WorkOS human path; IDN-03 says workload authentication remains disabled in managed mode until qualified |
| Endpoints are independent from resource identity but currently pinned | CLI `managed/data.rs:80` validates issuer, organization, cluster, incarnation and endpoint; it rejects a changed endpoint |
| Data access can use a valid cached credential | CLI `managed/data.rs:396`; expiry, identity changes and invalid custody can still prevent access |
| An account target cannot run graph operations | CLI `managed.rs:364` requires cluster context; data commands require an endpoint and graph where applicable |
| BYOC transport is not implemented merely because it is specified | Control RUN-11 leaves transport open at G8, with outbound customer connectivity and customer-held storage credentials |
| OIDC support is constrained, not arbitrary provider support | Server `oidc_identity.rs:154` and `:310` validate a specific bounded trust document and human credential profile |

Source anchors:
- [CLI target validation](https://github.com/ModernRelay/omnigraph/blob/b692610c4aa31066cb6a9fd34830f56977288943/crates/omnigraph-cli/src/targets.rs#L550)
- [Existing targeting decision](https://github.com/ModernRelay/omnigraph/blob/b692610c4aa31066cb6a9fd34830f56977288943/docs/rfcs/0059-config-cli-coherency.md)
- [CLI credential custody](https://github.com/ModernRelay/omnigraph/blob/b692610c4aa31066cb6a9fd34830f56977288943/crates/omnigraph-cli/src/managed/auth/renewal.rs#L109)
- [CLI data identity validation](https://github.com/ModernRelay/omnigraph/blob/b692610c4aa31066cb6a9fd34830f56977288943/crates/omnigraph-cli/src/managed/data.rs#L80)
- [Control authentication](https://github.com/ModernRelay/og-control-plane/blob/2f61511/intent-api/src/auth.rs#L287)
- [Control identity issuance](https://github.com/ModernRelay/og-control-plane/blob/2f61511/intent-api/src/data_tokens.rs#L434)
- [BYOC boundary](https://github.com/ModernRelay/og-control-plane/blob/2f61511/specs/07-RUN-Runs.md#L589)
- [Managed identity qualification](https://github.com/ModernRelay/og-control-plane/blob/2f61511/specs/06-IDN-Identity.md#L266)

## Proposed configuration

The names and fields below define a new contract. `version: 1` identifies this proposed format, not the released `target_config_version: 1` format.

```yaml
version: 1

profiles:
  modernrelay:
    api: https://platform.omnigraph.dev
    organization_id: org_example
    auth:
      provider: omnigraph

targets:
  mr-managed:
    kind: managed
    profile: modernrelay
    cluster_id: cluster_example
    cluster_incarnation: incarnation_example
    endpoint: https://cluster.example.com

  personal:
    kind: server
    endpoint: https://graph.example.com
    default_auth: personal-token
    auth:
      personal-token:
        provider: token
        source:
          store: {}

  scratch:
    kind: direct
    scope: graph
    uri: file:///srv/graphs/scratch.omni

defaults:
  output: table
```

`profiles` identifies a service, tenant and identity. Each entry owns one `auth` provider object. It does not select a cluster, graph, actor permission or source directory.

`targets` identifies a resource access path. Managed targets reference a profile and pin cluster ID/incarnation. Server targets own their endpoint and authentication configuration. Direct targets own their storage URI, graph/root scope and any required provider destination settings. A local target need not invent an organization or remote credential.

Keep `scope` only for direct graph versus root distinctions where it changes addressing. Do not require `scope: cluster` under managed targets: the managed variant already denotes a cluster. No managed account target remains.

No separate `services`, `organizations`, `regions`, `cells`, `accounts`, or `environments` registry is necessary. Repeating an API URL across two organizations is a reasonable cost for avoiding another layer of references. If actual usage later justifies a reusable transport object, add that narrow feature rather than generic inheritance.

## Configuration grammar

The operator document is one YAML mapping with the following public fields. Unless stated optional, variant fields are required. Profile, target, alias and auth-profile names MUST match `[A-Za-z][A-Za-z0-9_.-]*` and are case-sensitive. Names are local labels, not service identities; `default`, `admin` and `production` have no implicit behavior. These lexical rules do not apply to service-issued IDs or stored-query identifiers.

| Object | Fields |
| --- | --- |
| Document | `version: 1`; optional `profiles`, `targets`, `aliases`, `defaults` |
| Profile | `api`, `organization_id`, one `auth` provider object; optional `transport` |
| Managed target | `kind: managed`, `profile`, `cluster_id`, `cluster_incarnation`; optional `endpoint`, `transport`, `default_graph` |
| Server target | `kind: server`, `endpoint`, `auth`; optional `default_auth`, `transport`, `default_graph` |
| Direct graph target | `kind: direct`, `scope: graph`, `uri` |
| Direct root target | `kind: direct`, `scope: root`, `uri`; optional `default_graph` |
| Defaults | Optional `profile`, `target`, `output`; no standalone auth default |
| Read alias | `target`, `query`; conditional `graph`; optional `params` mapping |

`kind` has exactly three values: `managed`, `server`, `direct`. They describe access mechanisms. A catalogue is a read-only resource view, not a fourth access mechanism. The previously proposed catalogue target wrapper is withdrawn; version 1 rejects `kind: catalogue` and the magic `--target og_catalog` selector.

The explicit local `catalogue` command exposes the existing engine `CatalogueScope` independently of operator configuration. It requires exactly one inline `-e` or `--query` source and optionally accepts `--json`. It reads only the binary's registries and explicit source file; it MUST NOT read operator configuration (including an explicit config path), acquire credentials or open graph storage. Target, profile, auth, actor, graph, branch and snapshot selectors are rejected. Presentation uses in-memory defaults. Existing catalogue query restrictions apply, including refusal of mutations and graph-state operations. Served catalogue queries remain ordinary reads through a named target with `--graph og_catalog`; they do not use this local command.

`version` MUST be the integer 1. Unknown versions, duplicate YAML keys, unknown fields, invalid enum values, YAML tags, anchors, aliases and merge keys MUST be rejected. YAML is a serialization format, not a programming language: no interpolation, includes, inheritance or executable expressions. References are literal local names. Empty strings do not mean absent; null MUST NOT be used as an implicit reset/default.

The parser MUST validate required fields, types, variants, local references and cross-field constraints across the entire document without acquiring credentials. Any invalid entry invalidates the document; incomplete drafts do not belong in the active file. Missing credentials, unavailable helper executables/CA files, remote authorization, reachability and runtime capabilities are checked only for the selected operation, not unrelated entries. `config validate` performs the same complete offline schema/reference validation without reading secrets, executing helpers or contacting services. A saved profile MUST include its organization; first-login discovery is transient state, not an incompletely persisted profile.

An explicit operator file replaces, rather than merges with, the default operator file. Relative filesystem paths resolve against that file's directory. Authentication helpers are permitted only from operator-trusted configuration, never from an automatically discovered repository file. Selecting an explicit file authorizes its declared helpers only when executing an operation that needs them; inspection and validation never execute helpers.

Graph selection is `--graph` followed by the selected target's `default_graph`; otherwise graph-scoped operations refuse. A direct graph URI already identifies its graph and rejects a separate graph selector. Cluster-wide operations need no graph. No command selects the first discovered graph.

`default_graph`, alias `graph` and `--graph` contain the exact graph identifier accepted by the graph API, not a display-name lookup. If a backend uses name-like identifiers, the CLI MUST preserve those semantics rather than claim a stronger immutable identity. Per-graph recreation protection requires a backend generation/identity contract and is not provided by pinning the cluster incarnation alone. Direct version-1 URIs are absolute local `file://` URIs without query, fragment or remote authority; cloud URI variants remain deferred until their schemas are specified.

Aliases bind a stored read-query identifier (`query`, not a file path or inline program) to an explicit target. `graph` is required for multi-graph targets and forbidden for direct-graph targets, whose URI supplies it. Aliases never inherit `default_graph`. An alias MUST NOT change auth configuration or routing; caller target/graph overrides are rejected. `--auth` selects a named entry only on a bound standalone server; managed aliases retain their target's pinned profile. Normal credential acquisition can invoke that owner's helper, but the alias cannot define a helper. `params` is a mapping of query parameter names to JSON-compatible values, including null where the query type allows it. Caller parameters shallowly replace same-name defaults; values are passed as typed parameters, never interpolated. Aliases MUST NOT invoke mutations and use the ordinary resolver and authorization checks.

`defaults.output` has exactly `table | json | jsonl | csv | kv` and applies only to tabular/read-result commands supporting `--format`. Selection is explicit `--format`, then `defaults.output`, then `table`. A selected format unsupported by that read command fails before execution; it never silently substitutes another format. Action commands, durable receipts and errors use their own documented human/structured output contracts and ignore this read-result default. Raw credential export always ignores it. Machine stdout MUST NOT contain destination notices or diagnostics.

There are no general destination environment-variable overrides in version 1. Environment variables named by a selected credential source supply only that credential. Any future configuration override mechanism requires a published precedence contract; it MUST NOT silently combine fields into an unintended destination.

## Authentication

### Exact proposed syntax

A managed profile declares `auth` as one tagged provider object. Standalone servers retain a named `auth` map and optional `default_auth`. `provider` is the discriminator. Unknown fields and irrelevant provider fields are errors; there is no untyped options bag. Transport configuration is separate. Do not add `protocol: oauth | bearer`: OAuth is an acquisition/authorization framework and can issue bearer credentials, so those are not mutually exclusive protocols.

For a managed profile:

```yaml
auth:
  provider: omnigraph
```

For a standalone server, named provider alternatives remain available:

```yaml
default_auth: stored-token
auth:

  stored-token:
    provider: token
    source:
      store: {}

  injected-token:
    provider: token
    source:
      env: OG_SERVICE_TOKEN

  mounted-token:
    provider: token
    source:
      file: /var/run/omnigraph/token

  external-helper:
    provider: exec
    command: ["/usr/local/bin/example-credential-helper", "get"]
    timeout: 30s

  public:
    provider: anonymous
```

This is a catalogue of alternatives, not a valid set for every owner. Managed profiles reject anonymous authentication. `omnigraph` is initially available only on managed profiles. `token` and `exec` do not imply that the managed service accepts the resulting credential or recognizes a workload; the verified service contract must permit the credential class. Standalone servers may use only providers supported by that deployment. Direct storage uses native provider credentials separately; these HTTP providers do not replace AWS/Azure credential providers.

`omnigraph` has only `provider` in the initial schema. It owns interactive login, refresh and logout for a qualified platform adapter. The initial adapter uses the supported device flow; the public config does not encode its incidental token format. Public provider configuration comes from authenticated service discovery and an explicitly trusted adapter contract; application client IDs, issuer and login URLs are validated/pinned rather than guessed or accepted from an arbitrary redirect. The profile's verified principal and secret session are private local state, not YAML. `administrator` is merely a nickname and grants no administrator rights. Headless resource commands MUST NOT start an interactive login; they fail with an actionable login-required error.

`token` requires exactly one `source` variant: `store: {}`, `env: NAME`, or `file: PATH`. `store` is OmniGraph's secure credential store, indexed by the selected owner/profile binding, with no second credential nickname. The implementation may use the OS keychain, but that implementation name is not part of the grammar. `env` requires a name matching `[A-Za-z_][A-Za-z0-9_]*`. `file` requires a nonempty filesystem path, resolved against the operator file when relative. No shell expansion, literal token value or fallback source is accepted. A missing or empty selected source fails.

All three sources return one opaque HTTP bearer credential, not a refresh token or JSON object. Token input is bounded to 64 KiB and must conform to the HTTP bearer-token character grammar; header delimiters, embedded whitespace and control characters are rejected. File and stdin ingestion may remove one terminal LF or CRLF; they MUST NOT silently trim arbitrary whitespace. Environment values must already contain the token alone. Installation binds the credential to the selected route/tenant constraints, but does not prove token audience, principal or server acceptance.

A file source is an explicit externally managed secret, not a plaintext fallback created by OmniGraph. The CLI never writes, chmods or deletes it. Open the configured path per acquisition, read one bounded regular-file snapshot and close it; symlink-based atomic rotation is supported for projected volumes. Do not retain an open descriptor between invocations. The selected path and destination remain frozen; rotation never changes routing. File/env/store sources are read once for a normal invocation and do not promise automatic renewal. A fresh invocation can observe rotation. Reacquisition during long-running recovery requires the command's explicit safe recovery contract, not a blind retry after failure.

`exec` requires a nonempty argv array in `command`; `timeout` is optional with a 30-second default and a bounded maximum. Execute without a shell, after explicit owner/profile selection, never while merely listing targets. The helper protocol must bind the request to the selected endpoint, tenant and audience and return a bounded JSON credential with optional expiry. Server verification establishes the principal; helper-provided identity labels are not authority. Failure or expiry never triggers a different profile. This is a proposed extension, not an adapter established by the reviewed managed implementation.

`anonymous` has only `provider`; it sends no credential. It is allowed only on standalone server targets and must be explicitly selected/configured. It cannot be an error fallback.

Managed operations use the selected profile's single provider and reject `--auth`. Managed targets always use their pinned profile; neither `defaults.profile` nor a global `--profile` can override it. Managed profiles reject `default_auth` and nested named auth maps.

Only standalone servers have named `auth` entries. `--auth NAME` overrides the server's `default_auth`; without either, selection refuses even with one entry.

A profile describes credential acquisition; it does not assert a verified principal. On successful login, bind the profile to the server-verified identity in private local state. Two profiles in one organization may hold two independent human identities. Replacing a profile's principal requires an explicit account-replacement operation, and never retargets an already running command.

Each human profile owns an independent provider session. Include verified session identity in secret custody, not just the human principal. Reject a provider response that would silently share or overwrite another profile's refresh session. Logout removes the selected profile binding and attempts supported remote revocation; other independent sessions remain intact. A supported profile-rename command moves the local binding without changing its audience. An unrecognized manually renamed profile requires login rather than guessing a prior session.

Human and workload identities must remain distinguishable by verified claims, not profile names or the credential's source. Add a qualified workload adapter when both service and client support it; do not treat an arbitrary environment bearer or a `provider: workload` label as workload identity. A credential helper must have an explicit protocol, audience and expiry, bounded output/runtime, argv execution without a shell, and trusted operator configuration. It must not run merely because a repository supplied configuration or a remote discovery document named a command.

Tokens stay in keychain/secret storage or a declared credential source. Public profile-to-principal bindings, endpoint registrations and operation receipts are separate from secret material. Cache custody includes canonical service/provider identity, organization, principal and audience; data credentials additionally bind cluster and incarnation. Renaming a nickname does not change credential audience or erase operation history.

Profile session independence is a proposed change: the current origin/organization session index does not provide it. Logout must also clear data credentials owned by that profile locally; it cannot promise instant revocation of an already issued independently valid data token at the server. That requires a separate server-side revocation contract.

A qualified adapter's credential refresh may renew the same verified identity. It may not substitute another tenant, principal, resource or authentication method. Human login must never silently fall back to a CI credential, nor CI to an interactive login. For opaque standalone tokens without an authenticated introspection contract, the CLI can freeze the source and destination but cannot prove principal continuity. It MUST report identity as unknown rather than trust decoded claims or helper labels. Identity-sensitive renewal/recovery must refuse unless the server contract supplies the required evidence; ordinary requests remain subject to server authorization.

### Managed credential exchange

A managed profile's auth provider obtains a control-plane credential. The managed adapter uses the qualified control-plane exchange to acquire a data credential bound to the selected organization, cluster, incarnation, route and audience. It MUST NOT forward the control credential or login refresh token to the graph endpoint. The target does not configure a second independent data-auth profile: data credentials belong to the selected profile and resource binding. Direct cluster-token access without this exchange is a standalone `server` access path, not an implicit managed-mode fallback.

### Provider interface and helper protocol

The resolver passes an immutable credential request to the selected provider. It contains the credential purpose (`control` or `data`), canonical destination, expected audience where defined, tenant where applicable, resource identity where applicable and interactive/noninteractive mode. These values come from the selected binding, not helper output.

The internal result separates credential material, credential type, optional expiry and verified identity evidence. A provider cannot add arbitrary HTTP headers or change the request URL. An unverified helper claim is not verified identity evidence. Opaque tokens are allowed; the CLI MUST NOT require every token to be a locally decodable JWT.

The version-1 exec adapter uses one JSON request on stdin and one JSON response on stdout. The request includes `version: 1`, `purpose`, `endpoint`, `interactive: false` and the applicable identity fields. The response is:

```json
{
  "version": 1,
  "credential": {"type": "bearer", "token": "<secret>"},
  "expires_at": "2030-01-01T00:00:00Z"
}
```

`expires_at` is optional; when absent the result is not cached across invocations. Unknown response fields, multiple JSON values, empty credentials, expired results and unsupported credential types fail. Version 1 supports bearer results only; certificate support requires a new explicitly qualified variant, not arbitrary header injection. Helpers MUST return nonzero on failure and MUST NOT write credentials to stderr.

`command` is an argv array with an absolute executable path; no shell evaluation or current-directory/PATH lookup occurs. `timeout` is an integer number of seconds followed by `s`, from `1s` through `120s`, default `30s`. Stdout and stderr are each bounded to 64 KiB; exceeding either limit fails and terminates the child. Timeout terminates the child process group. Stderr is not included in routine logs or error JSON because a faulty helper may leak secrets. The helper inherits the invoking process environment, receives no additional OmniGraph cached secrets, and is an explicitly trusted local program, not a sandboxed plugin.

Refresh MAY rerun that same helper. Cached credentials are keyed by owner/profile binding, purpose, source configuration and resource/audience constraints, plus verified identity when available, not executable path alone. Unknown standalone identity is recorded explicitly; it is not a shared cache identity across profiles. Explicitly expired credentials MUST NOT be sent. A 403 MUST NOT trigger a provider switch. A 401 may invalidate credentials but MUST NOT automatically replay an effectful request whose outcome is uncertain.

### Secret custody and logout

Public YAML MUST NOT contain tokens, refresh tokens or private keys. The secret store contains sessions and explicitly installed static tokens; runtime caches contain derived credentials; the operation journal contains non-secret receipts and recovery identity. These stores have separate lifecycles.

If the OS secret store is unavailable, persistent interactive login MUST fail rather than silently create a plaintext token file. Headless operation can use an explicit environment, file or exec source. Persistent writes require atomic replacement and restrictive filesystem permissions for any non-keychain metadata. Secret values MUST be redacted from inspect output, logs and structured errors.

Profile binding identifiers are local metadata independent of nicknames. Supported renames preserve binding identifiers; manual renames do not guess or adopt sessions. Logout clears local credentials first and attempts supported remote session revocation; an unavailable or unsupported revocation MUST be reported truthfully. It MUST NOT claim immediate revocation of independently valid data tokens.

## Commands and selection

### Credential management

`login` and `logout` manage `provider: omnigraph` sessions. `credentials` manages locally installed static credentials and explicit access-credential export; it is not another routing/configuration registry. `config` and `target` declare providers and destinations. Static-token installation MUST NOT be overloaded onto `login`.

```sh
omnigraph credentials set --target production --auth operator --stdin
omnigraph credentials status --target production --auth operator
omnigraph credentials remove --target production --auth operator
omnigraph credentials set --profile work --stdin
omnigraph credentials export --target production --auth operator
```

These commands require exactly one explicit owner selector. Managed credentials use `--profile NAME` without `--auth`; standalone credentials use `--target NAME --auth PROFILE`. Managed-target export uses its pinned profile without `--auth`. Defaults do not apply. For set/status/remove, the selector names an owner: a profile or standalone server target. Managed targets MUST use their profile selector for those operations to avoid confusing control credentials with derived data tokens. Direct targets have no HTTP credential owner and reject this command family.

| Provider/source | Set/remove | Status | Export |
| --- | --- | --- | --- |
| `omnigraph` | Reject; use login/logout | Local session metadata | Qualified access credential only |
| `token` / `store` | Install/remove local entry | Local presence and binding metadata | Selected bearer credential |
| `token` / `env` or `file` | Reject; source is externally owned | Source availability, never value | Explicitly resolved bearer credential |
| `exec` | Reject; helper owns its credentials | Declared helper configuration, no execution | Explicitly invoke the helper |
| `anonymous` | Reject | Reports anonymous | Reject |

`set --stdin` reads bounded token input without echo, validates it, and writes atomically to the owner/profile's secure-store entry. It requires a configured `token`/`store` profile and does not create or edit YAML. An existing entry requires `--replace`; replacement invalidates credentials derived from that local entry. Credential arguments and environment-to-store implicit copying are not accepted. Installation does not contact the server and MUST report server validity as unverified. `remove` deletes only the local entry and its derived cache, leaves the profile configured and is idempotent. It never claims remote revocation or deletes operation evidence.

`status` is non-networking and never refreshes sessions, starts login or runs helpers. It reports selected owner/profile, configured source, availability, and locally known expiry/verified identity with provenance and timestamp. Unknown metadata stays unknown. Environment availability can be checked without printing values; file availability checks do not read contents. Availability is not proof of acceptance. Secure-store access must be noninteractive; if unavailable, report unavailable/unknown rather than trigger a login prompt. Output and errors MUST NOT expose tokens or raw credential payloads.

`export` explicitly releases an access credential to another local process. It requires stdout to be a pipe and refuses terminal or regular-file output; there is no force-to-terminal option. On success stdout contains only the bearer token followed by a newline, regardless of output defaults. Diagnostics go to stderr with secrets redacted. All validation/issuance completes before any secret bytes are written. `--profile` exports only a qualified control-plane access credential; `--target` exports a standalone server credential or, for a managed target, a qualified cluster-bound data credential. Managed export never substitutes a control token. The operation may refresh the selected provider or run its helper, but may not initiate interactive login. Unsupported export, anonymous access or missing/expired credentials fail explicitly. Refresh tokens are never exportable.

Export is not a sandbox: the receiving process can copy or disclose the secret. The pipe restriction prevents accidental terminal/file output, not malicious consumers. Export does not broaden the credential's existing server-enforced privileges. A future structured credential export needs a separately specified format; version 1 exports bearer access credentials only.

### Onboarding and resource selection

```sh
omnigraph login --register modernrelay
omnigraph login --api https://control.example.com --register private

omnigraph login --profile modernrelay
omnigraph organization members list --profile modernrelay
omnigraph cluster list --profile modernrelay
omnigraph cluster create demo --profile modernrelay

omnigraph target add mr-managed --profile modernrelay --cluster CLUSTER_ID
omnigraph graphs list --target mr-managed
omnigraph query ... --target mr-managed --graph spike
omnigraph cluster plan --target mr-managed --rev REVISION
omnigraph cluster apply --target mr-managed --plan PREVIEW_ID

omnigraph target inspect mr-managed --resolved
omnigraph target refresh mr-managed
omnigraph config validate
```

For a new profile, `login --register NAME` creates `auth: {provider: omnigraph}`, performs verified login and organization selection, then atomically saves the profile. Login rejects `--auth`; `login --profile NAME` selects an existing profile. An existing registration is never overwritten by this command. Bare `login` is the interactive onboarding wizard: it uses the hosted API default, proposes the profile name `omnigraph` and asks for an organization where needed, and makes the same explicit registration. In noninteractive mode, incomplete onboarding fails. None of these operations changes `defaults.target` or `defaults.profile` without an explicit configuration command.

Account-level operations select a profile. Resource operations select a target. These selectors are mutually exclusive. Login/logout accept a profile or resolve a managed target to its profile, and operate only on an `omnigraph` profile. Standalone server profiles use `credentials` or external source management instead; they do not have a platform-login session in version 1.

Optional `defaults.profile` and `defaults.target` apply only to their respective command classes. Neither is inferred from the other. No mutable current organization, current cluster or last-used identity exists outside the declarative configuration. `--config` continues to mean deployment source; use a distinct `--operator-config` for an explicit operator file. No automatic upward directory search or merging of personal and project operator files.

First login accepts `--api URL` without a registered profile. It performs supported device discovery and returns the verified principal/organization. An explicit registration option saves that result as a named profile; it does not select a cluster or alter defaults. Several organizations require an explicit choice. Automation refuses ambiguous or incomplete selection rather than prompting.

The agreed hosted default is `https://platform.omnigraph.dev`. Zero-argument login and new-profile creation use it when no API is supplied. Saved/generated profiles contain that explicit canonical API value; an incomplete hand-written profile does not borrow an ambient destination. Private services override it with `--api`. Resource commands still require a selected profile or target, and never infer an organization or cluster from the hosted default. This is proposed product behavior; the default is absent from the installed CLI today.

`target add --profile --cluster` reads authorized service discovery, verifies the response, and saves the selected cluster's exact identity and endpoint. It changes local registration only. Creating a remote cluster is a separate command. Names are display/lookup conveniences; once selected, operations bind immutable IDs and incarnation.

`target inspect --resolved` shows the expanded service, tenant, resource, graph selection, auth profile/verified principal if available, transport and their selection sources. It redacts secret material and does not refresh credentials by default.

## Identity, endpoints and transport

The managed resource identity is service plus organization plus cluster ID plus incarnation. An endpoint is a pinned route to that identity. Authentication identity is another component of the invocation, not part of the cluster's identity.

A control-only cluster registration may omit the data endpoint. Data operations then fail with a specific configuration error. They do not guess a hostname or find another target.

Populate endpoint pins from authenticated discovery at registration. An explicit refresh may update a changed route after verifying the same resource identity and showing the change. It must refuse a changed incarnation; replacement/rebinding is a separate explicit operation. Normal requests never follow redirects or silently adopt a newly issued credential's different endpoint.

Control and data transports are independent. Optional transport settings belong beside the corresponding API or endpoint: CA trust, proxy choice and supported network profile settings. Default to system trust; custom CA changes, when supported, have explicit append/replace semantics. Do not use `insecure` as a private-CA workaround. Proxy or tunnel support must preserve TLS authority, server-name validation and credential audience. Native Unix sockets retain socket identity rather than treating their synthetic HTTP origin as the destination.

The initial optional `transport` schema is deliberately bounded:

```yaml
transport:
  tls:
    ca:
      mode: append
      file: /absolute/path/company-ca.pem
  proxy:
    mode: url
    url: http://proxy.example.com:8080
```

`tls.ca` is optional; if present both `mode: append | replace` and `file` are required. Without it use system CA roots. The file is a PEM CA bundle, not a private key, and relative paths resolve against the operator file. Certificate verification and hostname verification are always enabled. `proxy` is optional and defaults to `mode: direct`. Its variants are `mode: direct` with no other fields, `mode: url` with a required URL, or `mode: environment` with no other fields. Explicit proxy URLs accept only `http` or `https`, a host and optional port, with no userinfo, query, fragment or non-root path. SOCKS and authenticated proxies are deferred. For HTTPS destinations, HTTP proxies use CONNECT and never receive origin Authorization headers outside the TLS tunnel. HTTPS proxy TLS uses system roots in this version; destination CA overrides do not override proxy trust. No profile transport is inherited by a managed target's data transport or by an identity-provider adapter.

Only `mode: environment` consults proxy variables. Capture the environment once per invocation. For HTTPS destinations use `https_proxy` or `HTTPS_PROXY`; for the loopback HTTP exception use lowercase `http_proxy` only (never uppercase `HTTP_PROXY`). `no_proxy`/`NO_PROXY` may bypass the chosen proxy. If both supported spellings of a variable exist with different values, fail instead of choosing implicitly. Empty proxy values mean direct; `ALL_PROXY` is not supported. Proxy URLs obey the same URL restrictions as explicit mode. Bypass lists are comma-separated `*`, exact hosts/IPs, or leading-dot DNS suffixes, optionally qualified by a port; suffix matching respects DNS label boundaries. IPv6 literals use brackets. Whitespace around entries is ignored; CIDR and other unsupported patterns fail. Resolve bypass matching from the destination's host and effective port, not reverse DNS. Inspection reports the resolved route and source without exposing environment contents. No other mode consults these variables.

Only HTTPS remote API/data endpoints are accepted initially. Plain HTTP is allowed only for an explicitly configured loopback standalone server with anonymous auth. Unix sockets, mTLS, custom identity-provider transport and tunnels are extension requirements, not fields that version 1 silently accepts. Qualification must specify their exact destination/trust semantics before enabling them. A private CA or proxy does not require a separate target kind.

Version 1 explicitly rejects custom destination CA trust combined with an HTTPS proxy, because the selected HTTP implementation cannot keep their trust stores independent. Explicit combinations fail whole-document validation; an environment-selected combination fails before network access. HTTP CONNECT proxies with custom destination CAs remain supported. HTTPS proxies without custom destination CA settings use system trust. This is an intentional version-1 capability boundary, not a fallback to shared custom trust.

Identity-provider traffic has its own verified origin and trust requirements; an API's transport settings are not permission to send identity credentials elsewhere. Unsupported private identity-provider or TLS modes refuse explicitly.

`api` and `endpoint` are base URLs, including a path prefix where supported, rather than assuming every service owns a hostname root. Reject userinfo, queries, fragments, traversal, encoded path separators and ambiguous encoding. Normalize scheme/host case and default ports, and treat the base path as a directory prefix with exactly one joining slash; never discard or case-fold the prefix. Credentials remain scoped to the accepted service route, not a sibling route on the same host. Preserve issuer/audience separately from the HTTP base URL when the authenticated protocol defines them separately. Current managed origin-only validation is a restriction to change deliberately, with prefix-boundary and credential-disclosure tests; do not just concatenate URLs or broaden credential matching to a whole host.

A hosted service may manage a private BYOC endpoint; the API and graph host need not share a DNS suffix. The service must attest the endpoint's association with the exact cluster. Existing public-route implementation assumptions must be revisited for BYOC; changing the local YAML alone cannot establish that association.

No endpoint pool, read-replica selector, automatic failover or write rerouting until server semantics qualify it. A reachable alternative URL is not proof that it represents the same safe writer.

## BYOC, storage and deployment

BYOC retains `kind: managed` when a managed service controls lifecycle. A customer-operated server without that management uses `kind: server`. Direct access to customer storage uses a separate `kind: direct` target. Never fall back between these access paths after an authorization, network or service failure.

Cloud account, placement, region, sizing, cells, network provisioning and execution-agent registration belong in deployment/service configuration. Do not add them to every client target. A direct-storage target does need region, endpoint and other provider destination fields when required to identify its storage unambiguously.

Customer AWS/Azure storage credentials remain with the customer-side execution/storage access path. Managed graph queries do not need those credentials. Storage credentials use the provider's native lifecycle: refreshable providers can refresh, supplied static credentials keep their actual lifetime. Destination selection stays frozen while credential refresh occurs.

`storage` is not an accepted field in this version. A provider-specific schema for S3-compatible endpoints, region/addressing choices, Azure and credential selection MUST be specified and tested before adding cloud-direct variants. Core version 1 specifies local `file://` direct targets only. Cloud direct access MUST NOT be advertised as qualified solely because this design describes its boundary. Native provider credential chains are permissible only under that adapter's explicit documented selection contract; HTTP auth profiles never substitute for storage authentication.

A direct maintenance target is explicitly privileged and still must satisfy existing writer-exclusion and recovery requirements. Configuring it does not make concurrent direct maintenance safe or grant a policy bypass to ordinary managed commands.

Air-gapped operation must work with local storage or a locally authenticated server. Hosted AuthKit is not an air-gapped authentication strategy. Current OIDC verification support is not evidence that arbitrary customer identity providers are already supported.

## One invocation, one resolved binding

1. Parse a single operator config snapshot with strict version and duplicate-key validation.
2. Select a profile or target by command class and explicit/default selector.
3. Resolve only its declared references. Validate selected variant fields, graph selection, endpoint, transport and auth profile. Local source-only commands need no destination.
4. Capture immutable destination and authentication configuration before any credential helper or network request. Relative paths resolve against the explicit config file, not the later working directory.
5. Acquire or refresh credentials and verify principal, tenant, issuer and audience where the qualified protocol supplies evidence. Managed exchange must verify its full binding. Opaque standalone credentials without introspection retain an unknown principal locally; freeze their source/destination without inventing verification. Complete the invocation binding once; identity-sensitive recovery requires the corresponding evidence.
6. Execute against that binding. Record operation identity and provenance where the operation is durable or may have uncertain outcomes.

Unknown references, missing required fields and unsupported fields refuse across the document. There is no dictionary merge across targets, profile chaining, credential fallback or runtime configuration reload. Unrelated runtime unavailability does not block a structurally valid selected target.

Durable retries bind canonical destination, verified principal where available, credential-owner binding, exact input and original operation identity. When principal verification is unavailable, the journal records that limitation and identity-sensitive recovery refuses rather than infer continuity from a token nickname. A renamed target, changed default, new login or altered profile must not cause replay against another destination. Credential caches never become the source of routing, and deleting a target never deletes unresolved operation evidence.

Use one resource-client dispatch after resolution. A managed credential adapter should produce an authenticated graph client usable by every supported served graph command, rather than maintain a second per-command allowlist. Current managed wrappers only cover a subset of graph operations; that is an implementation limitation to remove. Capability and protocol checks must still reject genuinely unsupported operations, and storage-only maintenance must not silently acquire direct storage access.

## Required verification before adoption

These are acceptance scenarios for the proposed implementation, not tests already passed.

| Scenario | Required result |
| --- | --- |
| Fresh user, no organization ID | Verified hosted login returns organization; registration stores it explicitly |
| Several organizations on one API | Each profile stays tenant-bound; no last-login switching |
| Two humans in one organization | Independent managed profiles/sessions; refresh/logout affects the selected profile; session collisions refuse |
| Human and workload access | Same resource can be selected; verified principal class and authorization remain distinct; unsupported workloads refuse |
| Config changed while a command runs | Existing invocation retains its captured binding |
| BYOC private data endpoint, public control API | Independent routing/trust; no customer storage credential leaves its execution domain |
| Control API unavailable | Valid independently usable data credentials work until expiry; fresh issuance and control operations report unavailable |
| Expired credential or identity drift | Refresh only the selected identity, otherwise refuse; no mutation replay |
| Renamed/recreated cluster | Rename does not change identity; changed incarnation refuses |
| Endpoint changes during issuance | Refuse and require explicit verified refresh, never leak credentials to an unaccepted route |
| Proxy, private CA, tunnel or Unix socket | Preserve endpoint identity and credential scope; unsupported cases fail before credential disclosure |
| Several service routes on one host | Preserve base-path boundaries; never send one route's credential to a sibling route |
| Self-hosted or air-gapped server | No managed-service or hosted-login dependency |
| Direct storage on S3-compatible/Azure | Capture provider destination once; preserve native credential refresh without destination drift |
| Source config, alias or cwd changes | No accidental backend selection or credential-helper execution |
| Failed apply/lost response followed by target edit | Observe/reconcile the original operation and identity; never submit to the edited destination |
| Alias/default changes | Selection is explainable and deterministic; graph commands do not discover an arbitrary graph |
| Same graph command over server and managed access | Common resource-client semantics; managed credential acquisition does not arbitrarily remove commands |
| Wrong YAML version, duplicate key, unknown field, invalid variant | Reject locally without network or helper execution |
| `token.source` has multiple variants | Reject; never select one by precedence |
| Selected token variable is empty | Actionable credential-source error; no alternate profile |
| Exec helper emits excessive output, malformed JSON or times out | Terminate/fail safely without logging raw credential output |
| Exec helper requests a new endpoint or returns an unsupported type | Reject; helper output cannot change the captured binding |
| Secret store unavailable during login | Fail persistence explicitly; no plaintext fallback |
| Unattended command lacks a session | Login-required error; never open a browser or prompt |
| One host serves sibling API prefixes | Credential cache and request construction preserve exact route boundaries |
| Logout during control outage | Local session/cache removed; remote revocation reported unconfirmed |
| Unselected config entry lacks a required field | Reject entire document before credential acquisition |
| Unselected valid profile has an unavailable secret/helper | Selected operation is not blocked by unrelated runtime availability |
| Alias targets a direct graph | Graph field absent; a redundant graph selector is rejected |
| Alias targets a multi-graph resource | Explicit alias graph required; target default cannot change the alias destination |
| Stored token installation/replacement | Explicit owner/profile and stdin; replacement requires `--replace`; no YAML changes |
| External token source removal requested | Refuse; do not modify environment, mounted files or helper state |
| Token file rotates atomically | Next invocation reads the new bounded snapshot; destination does not change |
| Credentials status on exec profile | No helper execution, network login or secret output |
| Credential export to terminal or regular file | Refuse before acquiring/exporting a secret |
| Managed credential export | Release only exchanged cluster data credential; never control/refresh token |
| Opaque standalone token without introspection | Principal remains unknown locally; ordinary server authorization still applies |
| Proxy environment mode with conflicting variables | Fail deterministically; no implicit spelling precedence |
| Proxy direct/URL mode with ambient variables | Ignore ambient proxy settings; preserve declared routing |
| Control credential offered to a graph endpoint | Refuse; graph access must use the managed data exchange |
| `kind: catalogue` or undefined `storage` field | Reject locally; future extension prose is not accepted grammar |

Implementation tests MUST cover parser/schema validation, deterministic resolution, provider contract tests, fake-server credential-disclosure tests, session-concurrency tests and durable-operation recovery. YAML syntax validation of examples alone is not implementation acceptance.

## Alternatives rejected

Keeping account entries in `targets` preserves one selector but forces control-plane tenant configuration into resource listings and repeats it in every cluster. It remains defensible; the benefit of separating it must justify the explicit reference and second command-class selector.

A single global active account is insufficient for multiple organizations, services, CI and concurrent operations. Deriving destination from the most recent credential is unsafe.

A fully generic graph of services, accounts, identities, environments, transports and resources is excessive before those reuse cases exist. Restrict composition to target-to-profile references with one identity each and server-local auth maps. Transport settings can initially remain inline.

`kind: byoc` confuses deployment ownership with the client's access protocol. Automatically discovered endpoints on every request complicate offline access and destination stability. Endpoint pinning with explicit refresh is the smaller, more auditable initial contract.

Generic authentication plugin support does not create a server-side workload trust contract. Likewise, a YAML replica list cannot create safe failover, and an AWS account field cannot implement BYOC execution.

## Implementation boundary

Source inspection at `7fd9cc9037c634e2f3284f1ede55a85a6f7b28e8` confirms that the stored-query registry is served-only. A structurally valid alias addressing direct storage therefore still requires a capability refusal: this RFC does not introduce a direct stored-query registry. Conditional alias graph validation defines addressing, not new backend functionality.

Authenticated cluster discovery can supply an endpoint pin: the reviewed control-plane `clusters::view_with_access` includes `data_endpoint` only after `data_tokens::access` validates the account and public-route association with cluster ID/incarnation. Registration and refresh must consume that attestation and preserve control-only semantics when absent, rather than synthesize a hostname. This evidence does not qualify future BYOC routing or establish the deployed control-plane revision.

The format, resolver, scoped auth profiles, onboarding/registration, credential-custody changes and command selection require an intentional CLI rewrite. Current target validation/credential isolation tests provide useful assertions, not compatibility requirements. Control-plane APIs may largely retain their current organization and cluster identities, but independent named sessions, qualified workload access and BYOC private-route association require their own concrete implementation and validation where applicable.

Adopt the configuration model independently of shipping every future adapter. Expose only qualified authentication/transport methods. Retain explicit unsupported results for everything else. There is no migration layer or legacy syntax requirement in this proposal.

### Delivery gates

1. Publish the machine-readable strict configuration schema and resolver conformance tests. Implement generated onboarding, single-identity managed profiles, server-local auth maps, server/managed targeting, credential management and local direct storage. Qualify read-alias bindings; catalogue view semantics remain a separate design gate, not another target kind.
2. Qualify platform login/session isolation, token sources, secret custody and the common served-resource client against real CLI and control-plane contracts. Generated config and diagnostics must use the same parser/resolver as execution.
3. Qualify the exec adapter and private CA/proxy transports with adversarial tests. If an adapter is unavailable, reject it with an unsupported-capability error before acquiring credentials; do not silently ignore its fields.
4. Specify and qualify cloud-direct provider schemas, workload federation, private/BYOC endpoint attestation and additional transports independently. None is implied by completion of the basic config rewrite.

Release readiness means the supported subset is internally complete and accurately documented, not that every future extension exists. Any advertised field must have a precise schema, resolver behavior, credential-custody behavior and passing acceptance tests.
