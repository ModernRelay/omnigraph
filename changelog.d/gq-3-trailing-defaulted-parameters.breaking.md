- A query parameter with a default is followed only by parameters with defaults, since a call's arguments bind by position: `($min: I32 = 18, $city: String)` is `T88` at the declaration, and its fix moves the defaulted ones last. See [declarations][gq-3-trailing-defaults-guide].

[gq-3-trailing-defaults-guide]: ../docs/user/queries/index.md#declarations-and-parameters
