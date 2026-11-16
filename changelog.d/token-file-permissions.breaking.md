- On Unix the server refuses a `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` that other accounts can write, and warns when they can read it. Restrict the file with `chmod 600`. See [token sources][token-file-mode].

[token-file-mode]: ../docs/user/operations/server.md#authentication
