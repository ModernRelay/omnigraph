- OmniGraph now uses Lance 13.0.0 while retaining graph storage format 16 and stable V2_2 data files. Format-16 graphs need no storage conversion; existing full-text analyzer checks and bounded Blob reads and compaction remain in effect. See the [Lance compatibility contract][lance-13-contract].
- Run only `omnigraph cleanup` against a graph root. Lance 13's own cleanup deletes every data file older than seven days that no attached manifest references, and OmniGraph's table versions are detached commits it does not trace, so a stock Lance cleanup or vacuum tool pointed at a graph's tables or its `__manifest` catalog now destroys the rows of every graph commit older than a week while leaving their manifests in place. See [cleanup][lance-13-cleanup].

[lance-13-contract]: ../docs/dev/versioning.md#lance-contract
[lance-13-cleanup]: ../docs/user/operations/maintenance.md#cleanup
