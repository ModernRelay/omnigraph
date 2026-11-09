- The installers fall back from stable to edge only when no stable release publishes the archive, print the stable tag they install, and verify build provenance with `gh attestation verify` when the GitHub CLI is signed in. See [install][installer-provenance].

[installer-provenance]: ../docs/user/install.md#release-channels
