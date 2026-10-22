# Security Policy

OmniGraph is maintained by EQTR, Inc. Do not report security issues through
public GitHub issues, pull requests or the Slack community.

## Reporting a vulnerability

Report privately through GitHub:
[open a security advisory report](https://github.com/ModernRelay/omnigraph/security/advisories/new)
(**Report a vulnerability** on the repository's Security tab). Only the
reporter and the maintainers can see it.

Include:

- the affected release, edge build or commit;
- the impact, and the setup it needs (embedded, CLI or served; storage
  backend; policy configuration);
- reproduction steps;
- any proposed mitigation.

Leave real credentials, bearer tokens and customer data out of the report, and
redact them from logs you attach.

## What to expect

- Acknowledgement within 3 business days.
- An initial assessment, saying whether the report is accepted and how severe
  it is, within 10 business days.
- Public disclosure through a GitHub security advisory once a fixed release is
  available, or 90 days after the report, whichever comes first. A different
  date can be agreed with the reporter when a fix needs longer.

The advisory credits the reporter unless they ask not to be named.

## Supported versions

OmniGraph is pre-1.0. Security fixes land on `main`, ship in the next edge
build, and are included in the next stable release. Only the latest stable
release and the newest edge build receive fixes. Reports against older
releases are welcome, but the fix ships in a new release, not a backport.

## Scope

In scope: the code in this repository and the release artifacts built from it,
namely the `omnigraph` and `omnigraph-server` binaries, the
`modernrelay/omnigraph-server` container images on Docker Hub and GHCR, and
the install script.

Out of scope: the configuration of the services a deployment relies on, such
as storage bucket policies and encryption, the identity provider, the
embedding provider and the TLS-terminating proxy. The
[deployment guide](docs/user/deployment.md) describes what OmniGraph expects
from each.
