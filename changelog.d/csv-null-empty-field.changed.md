- CSV query output writes a null as an empty field instead of `null`, so `load csv` reads it back as null and a stored string `null` stays distinct. See the [CLI reference][csv-null-reference].

[csv-null-reference]: ../docs/user/cli/reference.md
