- **Page through a branch's commit history.** `GET /commits` takes `limit`
  (1 to 1,000) and an opaque `page_token`, and `omnigraph commit list` takes
  `--limit` and `--page-token`; the MCP `commits_list` tool takes both too. A
  page returns `head_commit_id`, the head its listing resolved, and a
  `next_page_token` while older commits remain; later pages answer for that
  head even after the branch moves. A page reads only the history it returns,
  so its cost no longer grows with the branch's history. Under a policy, a
  caller whose `read` does not cover every branch continues only from a commit
  of the branch's history; one it holds or a readable branch published costs
  nothing more, and any other is looked for down the whole history. Without
  either
  parameter the whole history is listed as before. See
  [commits][commit-list-pages-guide].

[commit-list-pages-guide]: ../docs/user/branching/index.md#commits
