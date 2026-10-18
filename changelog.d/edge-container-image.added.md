- Every edge build is now also a container image with the build's tag, such
  as `modernrelay/omnigraph-server:edge-20261016-9c2b367` on Docker Hub and on
  GHCR. It packages that build's released Linux x86_64 binaries on Debian
  trixie, carries a build provenance attestation, and an `edge` image tag
  follows the rolling `edge` release. `latest` still names the newest stable
  release. See [release channels][edge-container-image].

[edge-container-image]: ../docs/user/install.md#release-channels
