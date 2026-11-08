- Export and change-baseline streams retain transport capacity through the last
  owner of each yielded buffer. Consumers holding all available chunks receive
  a bounded stream failure without a usable baseline cursor; releasing chunks
  lets streaming continue. See [server resource limits][export-retained-limits].

[export-retained-limits]: ../docs/user/deployment.md#admission-limits
