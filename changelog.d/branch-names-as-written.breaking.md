- Branch names are used exactly as written. A name with leading or trailing
  whitespace or a control character is now refused as a bad request wherever
  a branch is named (HTTP routes, CLI commands and embedded engine calls), as
  branch statements already refused it; such a name used to be trimmed, so
  `main ` named `main`. Clients that send padded names must send the exact
  name. A policy whose `protected_branches` lists such a name now fails
  validation. See [branch names][branch-names-as-written].

[branch-names-as-written]: ../docs/user/branching/index.md#branch-names
