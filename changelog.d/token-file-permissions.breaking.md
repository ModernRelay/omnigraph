- Another local account that could write `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` could add a token for any actor. On Unix the server now refuses to start when that file is writable by other accounts or is not a regular file, and warns when they can read it. Run `chmod 600` on it. See [token sources][token-file-mode].

[token-file-mode]: ../docs/user/operations/server.md#authentication
