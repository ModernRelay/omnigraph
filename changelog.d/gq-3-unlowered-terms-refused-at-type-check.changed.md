- A query that type-checks but uses a form this release does not run, such as `limit $param`, is refused at type check with `T86`, naming the stage that adds it, before any data is read. See [diagnostics][gq-3-t86-guide].

[gq-3-t86-guide]: ../docs/user/queries/diagnostics.md#terms-this-release-does-not-lower
