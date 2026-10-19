- The CLI now selects one complete backend with `--target NAME|ADDRESS` or `defaults.target`. Legacy routing flags, profiles, folder selection and `use` are removed; `--config` supplies source files only, and resolved targets are always printed to stderr. Run `omnigraph target import` for legacy operator configuration and lifecycle custody, then review the complete target and endpoint-bound credentials before use. See the [CLI reference][target-cli].

[target-cli]: ../docs/user/cli/reference.md
