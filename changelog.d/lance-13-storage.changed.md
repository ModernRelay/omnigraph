- OmniGraph now uses Lance 13.0.0 while retaining graph storage format 16 and stable V2_2 data files. Format-16 graphs need no storage conversion; existing full-text analyzer checks and bounded Blob reads and compaction remain in effect. See the [Lance compatibility contract][lance-13-contract].
- Run only `omnigraph cleanup` against graph storage. Lance 13's own cleanup can delete files used only by detached versions once those files are older than seven days, leaving manifests present but affected snapshots—including the current graph—unreadable. This also applies to the `__manifest` catalog. See [cleanup][lance-13-cleanup].

[lance-13-contract]: ../docs/dev/versioning.md#lance-contract
[lance-13-cleanup]: ../docs/user/operations/maintenance.md#cleanup
