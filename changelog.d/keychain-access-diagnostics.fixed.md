- Secure-store failures now include redacted operation and failure categories,
with numeric macOS OSStatus when available, rather than hiding every cause behind
an unavailable-store message. macOS writes no longer attempt creation after an
access-denied lookup, and deletion failures are reported instead of ignored.
These changes do not change Keychain permissions
or repair trust across ad-hoc-signed CLI upgrades. See
[macOS Keychain access failures][keychain-access].

[keychain-access]: ../docs/user/cli/managed-data.md#macos-keychain-access-failures
