- **Fewer, larger change pages from the CLI.** When `commit changes` or
  `changes poll` follows every page itself, it now asks for 8,192 changes
  per page instead of the 1,000 the HTTP routes default to. Every page
  resumes its comparison where the last one stopped, so a large result
  costs fewer requests and less time; the output is unchanged. `--limit`
  still sets the size, and a single `--page-token` fetch still defaults to
  1,000. See [changes and change feeds][cli-change-pages].

[cli-change-pages]: ../docs/user/branching/changes.md#inspect-one-commit
