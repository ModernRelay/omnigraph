- Managed CLI login no longer counts OS-keychain consent against its 35-second
  network deadline. Cached-session collision checks still finish before
  organization selection; credentials are saved only after verification succeeds.
  See [managed login][managed-login-keychain-consent].

[managed-login-keychain-consent]: ../docs/user/cli/reference.md#managed-cluster-commands
