- The CLI no longer reads `OMNIGRAPH_BEARER_TOKEN`. It used to send that
  token to any remote server a command addressed, including a literal
  `--server https://…` URL the operator does not control. A token now reaches
  only the server it is keyed to: a request to a URL under a server declared
  in `~/.omnigraph/config.yaml` carries that server's token, and any other
  request carries none. When such a request gets HTTP 401, the CLI names the
  server to declare or the `omnigraph login` command to run; `--json` output
  carries this as `missing_credential` beside the server's unchanged error.
  The server's own `OMNIGRAPH_SERVER_BEARER_TOKEN*` settings are unchanged.

  If you export `OMNIGRAPH_BEARER_TOKEN` for the CLI, declare the server under
  `servers:` and bind its token with `omnigraph login <name>`, or set
  `OMNIGRAPH_TOKEN_<NAME>` (the name uppercased, `-` written as `_`). In CI,
  write a `config.yaml` with the server's `url` into `$OMNIGRAPH_HOME` and
  pass the secret as `OMNIGRAPH_TOKEN_<NAME>`. Address the server by name
  (`--server <name>`) or by a URL under its `url`. Managed credentials
  (`omnigraph login --api`, `OMNIGRAPH_CONTROL_TOKEN`) are unchanged. See
  [server credentials][cli-unkeyed-bearer-token-removed].

[cli-unkeyed-bearer-token-removed]: ../docs/user/cli/index.md#use-a-server
