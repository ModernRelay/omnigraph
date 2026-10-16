- `omnigraph skill export DIR` writes the `omnigraph` agent skill built into the binary, so an agent reads the skill written for the CLI it runs. It refuses to replace existing files without `--force`, records what it wrote so a later `--force` export removes only unchanged files the skill no longer has, and with `--json` reports the files written, `skill_version`, `cli_version` and `http_contract`; `omnigraph skill show [PATH]` prints one skill file. See [the CLI guide][skill-export-guide].

[skill-export-guide]: ../docs/user/cli/index.md#give-an-agent-the-matching-skill
