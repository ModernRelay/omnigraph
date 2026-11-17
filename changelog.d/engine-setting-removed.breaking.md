- The session setting `engine` and its variable `OMNIGRAPH_ENGINE` are
  removed: every read runs from its plan, and `v2` was the setting's one
  value. A `set engine = v2;` line, `--set engine=v2` or an `engine` field in
  a request's `settings` is now refused as an unknown setting; delete it. The
  variable is no longer read. See the [settings guide][engine-setting-removed-settings].

[engine-setting-removed-settings]: ../docs/user/queries/settings.md
