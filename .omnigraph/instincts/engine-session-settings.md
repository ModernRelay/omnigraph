---
id: engine-session-settings
trigger: "when reading, adding, or modifying configuration settings or environment variables in engine crates"
confidence: 0.95
domain: engine
source: architectural-invariant
action: "Read settings from the query/mutation Session, never via std::env::var or settings::from_env in engine crates"
---

# Engine Session Settings Derivation

## Action
Inside the engine crates (`crates/omnigraph`, `crates/omnigraph-core`, and `crates/omnigraph-catalog`), take all configuration and behavior settings exclusively from `Session` (`self.session_settings()`). Never call `std::env::var`, `std::env::var_os`, or `settings::from_env`.

## Explanation & Invariant
Omnigraph maintains strict session isolation. Environment variables are resolved once at the session doors (server startup and CLI entrypoints). The engine crate must read no session-setting environment variables directly. Tests and examples must also pass settings through `db.session(...)` rather than using `EnvGuard::set` or `std::env::set_var`.

This invariant is mechanically guarded in CI by `scripts/check-agents-md.sh`. Any direct `env::var("OMNIGRAPH_...")` call under `crates/omnigraph*/src` will immediately fail the build gate.

## Examples
### Correct
```rust
let work_limit = session.settings().traversal_work_limit;
```

### Incorrect
```rust
// Forbidden: Reaches the process environment directly from engine code
let work_limit = std::env::var("OMNIGRAPH_TRAVERSAL_WORK_LIMIT")
    .ok()
    .and_then(|v| v.parse().ok());
```
