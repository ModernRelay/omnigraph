- `case`, `||`, `<>`, single quotes and uppercase keywords now compile, so they no longer return `Q012` or `Q014`; `lint` reports them as `L212` to `L216` infos. Scripts and agents that match those refusals should read the lint findings instead. See [lint][gq-3-admitted-spellings-lint].

[gq-3-admitted-spellings-lint]: ../docs/user/queries/lint.md#canonical-forms
