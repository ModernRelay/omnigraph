- Managed login and folder context now use version 2 and select an organization
explicitly or from a bound folder. Separate organizations keep separate device
sessions; ambiguous selection refuses, running commands keep the same verified
identity, and logout clears only the selected organization's control credentials.
Existing managed users must sign in and bind their folder again. Direct access,
named-server credentials, graph HTTP contracts and storage formats are unchanged.
