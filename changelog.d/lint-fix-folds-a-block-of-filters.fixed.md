- `lint --fix` writes a block holding only filters as the scalar it folds to, `if(coalesce(φ, false), 1, 0)` for `count { φ }`, where it looped between two fixes. See the [map][fold-block-map].

[fold-block-map]: ../docs/user/queries/upgrading-to-gq3.md#the-respelling-map
