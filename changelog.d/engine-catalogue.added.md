- The engine catalogue: five built-in read-only node types (`@Operator`, `@Pass`, `@Setting`, `@Engine`, `@LogicalKind`) answer GQ reads from the running binary's registries, beside a graph's own types or under the store-free scope `og_catalog` (`--store og_catalog`, `--graph og_catalog`, policy target `og_catalog`). Writes and text search over them are refused; `schema show` lists them. See [Engine catalogue][engine-catalogue].

[engine-catalogue]: ../docs/user/queries/catalogue.md
