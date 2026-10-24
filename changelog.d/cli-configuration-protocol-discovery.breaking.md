- `omnigraph version --json` now reports target and credential protocol 2 for the current operator configuration and credential commands. Clients selecting a CLI must require these exact versions: protocol 1 appeared in multiple incompatible edge builds and cannot identify one format. The YAML document remains `version: 1`; see [compatibility discovery][cli-protocol-discovery] and the [configuration upgrade guide][cli-protocol-upgrade].

[cli-protocol-discovery]: ../docs/user/install.md#verify
[cli-protocol-upgrade]: ../docs/user/cli/targets.md#upgrade-from-the-old-format
