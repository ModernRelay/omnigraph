- The CLI matches a server's token on where a request is sent, not on the
  address as written. A request carries the token only when its scheme, host
  and port (or its Unix socket) match the server's `url` and its path lies
  under that `url` after `.` and `..` segments resolve. Before, an address
  such as `https://host/tenant-a/../tenant-b`, or a socket path ending in
  `../other.sock`, could carry a declared server's token to a different
  path prefix or socket. A `--graph` value with a `.` or `..` path segment
  is now refused. See [server credentials][cli-credential-transport-target].

[cli-credential-transport-target]: ../docs/user/cli/index.md#use-a-server
