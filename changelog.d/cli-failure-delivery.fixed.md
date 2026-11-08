- CLI completion preserves delivered refusal exit codes and does not append a second JSON response after a final flush failure; served deployment errors retain the original refusal when recovery hints cannot be written. Reported timeouts no longer wait for blocked background lookups at exit, and help/version keep their usual status when output closes. Managed commands attempt recovery-only output on healthy stdout if an accepted operation cannot be reported on stderr; see [CLI output and errors][cli-failure-output].

[cli-failure-output]: ../docs/user/cli/index.md#output-and-errors
