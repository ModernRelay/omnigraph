- `--server unix:///absolute/path.sock`, or a server `url:` of that form in
  `~/.omnigraph/config.yaml`, reaches a server through a Unix-domain socket, for
  example a local gateway that a sandboxed agent may reach when it has no TCP
  network access. The CLI sends the same HTTP/1.1 requests over the socket,
  with the same `/healthz` discovery, contract header, bearer token, and
  no-redirect and no-retry rules as over TCP. Unix platforms only. See
  [Use a server through a Unix socket][server-unix-socket-cli].

[server-unix-socket-cli]: ../docs/user/cli/index.md#use-a-server-through-a-unix-socket
