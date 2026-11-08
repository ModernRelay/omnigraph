- Authenticated CLI operations can read operator configuration from a read-only directory while keeping credential metadata in the writable operator home. Credential expiry remains attached through discovery and is checked before dispatch, preventing expired standalone or managed credentials from submitting requests. Existing managed credential ownership and recovery evidence are preserved; conflicting legacy state refuses. See [operator configuration][cli-credential-state-expiry].

[cli-credential-state-expiry]: ../docs/user/cli/targets.md
