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
}

#[cfg(feature = "failpoints")]
impl Drop for ScopedFailPoint {
    fn drop(&mut self) {
        fail::remove(&self.name);
    }
}
