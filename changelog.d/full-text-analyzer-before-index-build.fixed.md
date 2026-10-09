- Full-text functions now match with the index's analyzer (lowercasing and stemming) as soon as rows are written: after a type's first rows, an overwrite load, a newly declared full-text `@index`, or a schema change that adds, renames or drops a property. Before, until `omnigraph optimize` built the index they matched case-sensitively and without stemming, so `search($d.body, "deep")` missed "Deep Learning". See the [search guide][analyzer-before-build-search].

[analyzer-before-build-search]: ../docs/user/search/index.md
