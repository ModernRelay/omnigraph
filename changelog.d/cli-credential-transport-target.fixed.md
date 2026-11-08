- The CLI matches a server's token on where a request is sent, not on the
  address as written. A request carries the token only when its scheme, host
  and port (or its Unix socket) match the server's `url` and its path lies
  under that `url` after `.` and `..` segments resolve. Before, an address
  such as `https://host/tenant-a/../tenant-b`, or a socket path ending in
  `../other.sock`, could carry a declared server's token to a different
  path prefix or socket. A `--graph` value for a served graph must now be
  one path segment of ASCII letters, digits, `-` or `_`; any other value is
  refused before a request is sent. Two `servers:` names that spell the
  same `OMNIGRAPH_TOKEN_<NAME>` variable, such as `a-b` and `a_b`, are now
  refused when the operator config loads, since one token would bind both.
  See [server credentials][cli-credential-transport-target].

[cli-credential-transport-target]: ../docs/user/cli/index.md#use-a-server
