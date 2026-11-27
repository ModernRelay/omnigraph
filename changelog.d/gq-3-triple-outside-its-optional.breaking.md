- A triple outside an `optional` that reads a name the `optional` introduced is `T90`: it dropped every row where the name was null, so the outer join silently became an inner one. Move the triple inside the `optional` (the printed fix) to keep those rows, or drop the `optional` where only its matches were meant. See [optional elements][gq-3-optional-guide].

[gq-3-optional-guide]: ../docs/user/queries/traversal.md#optional-elements
