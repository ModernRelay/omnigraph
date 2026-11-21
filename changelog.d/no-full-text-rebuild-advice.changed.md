- `index build` and `optimize` no longer recommend `index rebuild --kind full-text`, which now warns that older binaries refuse full-text reads until their own rebuild. See [rollback][full-text-rollback-guide].

[full-text-rollback-guide]: ../docs/user/operations/upgrade.md#query-language-gq-3
