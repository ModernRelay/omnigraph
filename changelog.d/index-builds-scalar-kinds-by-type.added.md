- `@index` builds what a type serves: BITMAP (enum, `Bool`), LABEL_LIST (list), NGRAM and BTREE (String); a String `@key` builds a BTREE, and only `@index(fulltext)` full text. See [what `@index` builds][scalar-index-kinds-guide].

[scalar-index-kinds-guide]: ../docs/user/schema/index.md#what-index-builds
