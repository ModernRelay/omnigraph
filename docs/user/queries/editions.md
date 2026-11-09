# Editions

A file without an edition line, or one that opens with `gq 2;`, is GQ edition
2, the language the [query guide](index.md) describes. A file that opens with
`gq 3;` selects the edition-3 preview, designed in the draft
[GQ 3 RFC](https://github.com/ModernRelay/omnigraph/pull/1302). Its
declarations are parsed and type-checked, so a mistake is refused with its code
and fix, but nothing in the preview executes: a declaration that checks is
refused at planning with `P006`, before any snapshot is read. Any other edition
line is refused with `gq 3;` as the fix.
