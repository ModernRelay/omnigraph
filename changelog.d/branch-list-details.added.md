- `GET /graphs/{graph_id}/branches?detailed=true` and `omnigraph branch list
  --detailed` report each branch's head commit, the branch and commit it
  started from (the `from` and `from_commit_id` its create receipt named),
  and when it was created and last moved, beside the names. A client no
  longer reads each branch's whole commit list for these: the listing reads
  each branch's manifest once, or twice once the branch has written a
  commit, whatever its history. A branch deleted and created again reports
  its new creation. See [Branches][branch-list-details].

[branch-list-details]: ../docs/user/branching/index.md#branch-workflow
