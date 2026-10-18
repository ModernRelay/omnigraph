- OmniGraph now uses Lance 13.0.0 while retaining graph storage format 16 and stable V2_2 data files. Format-16 graphs need no storage conversion; existing full-text analyzer checks and bounded Blob reads and compaction remain in effect. See the [Lance compatibility contract][lance-13-contract].

[lance-13-contract]: ../docs/dev/versioning.md#lance-contract
