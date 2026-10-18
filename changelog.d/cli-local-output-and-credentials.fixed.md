- Local account, profile, version, skill-export and embedding reports now return ordinary output errors instead of panicking on a broken pipe; completed credential and file changes remain in place without replay. Invocations share one operator configuration, and ordinary graph clients retain their selected token across registry discovery and the command even if the credentials file changes. See [CLI output and errors][cli-local-output-errors].

[cli-local-output-errors]: ../docs/user/cli/index.md#output-and-errors
