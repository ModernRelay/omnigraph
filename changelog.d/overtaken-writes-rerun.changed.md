- An update, delete or overwrite load overtaken by another commit reruns once on the new head, selecting its rows again, instead of returning `read_set_conflict`; `--if-commit` refuses. See [mutations][overtaken-writes-mutations].

[overtaken-writes-mutations]: ../docs/user/mutations/index.md
