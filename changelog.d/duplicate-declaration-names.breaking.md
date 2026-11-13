- A source that declares one query name twice is now refused with `T83`, naming both declarations; before, the first ran and a call resolved to the last. Stored queries are affected too: rename or delete the later declaration before you upgrade. See the [query guide][duplicate-declaration-names-guide].

[duplicate-declaration-names-guide]: ../docs/user/queries/index.md#declarations-and-parameters
