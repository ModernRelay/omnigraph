- Release archives and the container image now name the commit and channel
  they were built from. `omnigraph version` prints a `commit` line, `--json`
  adds `channel` and `commit`, and an edge build reports a pre-release version
  such as `0.14.0-edge+9c2b367`, which sorts after the latest stable release
  and before the release it precedes. The server reports the same version on
  `GET /healthz` and in its MCP server info, and the commit as
  `source_version`. Every
  archive and the image's GHCR digest also carry a build provenance
  attestation that `gh attestation verify` checks. See
  [Verify][release-build-identity].

[release-build-identity]: ../docs/user/install.md#verify
