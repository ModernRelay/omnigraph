- A declared read named after a new function or aggregate (`lower`, `length`, `left`, `round`, `size`, `cast`, `stddev`, `percentile`, …) is refused with `T74`. Rename such stored queries and their callers before you upgrade; `omnigraph queries validate` lists them. See [upgrading queries][gq-3-function-names-guide].

[gq-3-function-names-guide]: ../docs/user/queries/upgrading-to-gq3.md#other-changes-a-reader-sees
