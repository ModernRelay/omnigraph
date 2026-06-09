//! Fault-injection hooks for the cluster apply protocol, mirroring the
//! engine's `omnigraph::failpoints` pattern. With the `failpoints` feature
//! off, every call site compiles to `Ok(())`.

use crate::Diagnostic;

pub(crate) fn maybe_fail(_name: &str) -> Result<(), Diagnostic> {
    #[cfg(feature = "failpoints")]
    {
        let name = _name;
        fail::fail_point!(name, |_| {
            return Err(Diagnostic::error(
                "injected_failpoint",
                name,
                format!("injected failpoint triggered: {name}"),
            ));
        });
    }
    Ok(())
}

#[cfg(feature = "failpoints")]
pub struct ScopedFailPoint {
    name: String,
}

#[cfg(feature = "failpoints")]
impl ScopedFailPoint {
    pub fn new(name: &str, action: &str) -> Self {
        fail::cfg(name, action).expect("configure failpoint");
        Self {
            name: name.to_string(),
        }
    }

    /// Register a callback failpoint with the same Drop-based cleanup as
    /// `new`. Without the guard, a panic while the point is active would
    /// leak the callback into the process-global registry and fire it under
    /// later tests in the same binary.
    pub fn with_callback<F>(name: &str, callback: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        fail::cfg_callback(name, callback).expect("configure callback failpoint");
        Self {
            name: name.to_string(),
        }
    }
}

#[cfg(feature = "failpoints")]
impl Drop for ScopedFailPoint {
    fn drop(&mut self) {
        fail::remove(&self.name);
    }
}
