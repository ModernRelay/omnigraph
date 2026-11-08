- `branch merge` now selects its base as the newest commit both branches descend from. It previously selected the common ancestor closest to both heads by hop count, so a merge commit's second parent could lead it to an older commit and a delete or update made on the branch after it forked could be dropped or reported as a conflict. When two common ancestors are each not an ancestor of the other, the one nearest the two heads by commit count wins, then the higher generation, then the smaller commit id. See [merge][merge-conflicts].

[merge-conflicts]: ../docs/user/branching/merge.md#conflicts
