- `omnigraph version --json` prints the package version, the HTTP contract the
  CLI sends (`http_contract`, the exact `Omnigraph-Http-Api` value a server
  requires) and the storage-format stamp it writes with the inclusive range it
  serves (`internal_schema.writes`, `internal_schema.serves`), without
  contacting a server, so a program can choose among installed CLIs. The text
  output gains a third line, `http-contract 0.13`. See [Verify][cli-version-json].

[cli-version-json]: ../docs/user/install.md#verify
