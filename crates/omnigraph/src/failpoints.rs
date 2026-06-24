use crate::error::Result;

pub(crate) fn maybe_fail(_name: &str) -> Result<()> {
    #[cfg(feature = "failpoints")]
    {
        let name = _name;
        fail::fail_point!(name, |_| {
            return Err(crate::error::OmniError::manifest(format!(
                "injected failpoint triggered: {}",
                name
            )));
        });
    }
    Ok(())
}

/// Failpoint that injects a *Lance* error rather than an `OmniError`. Used to
/// stand in for a `Dataset::open` failing with a transient/corrupt (non-not-found)
/// error, so a test can drive the caller's lance-error classification — the
/// behavior FIX A (`read_legacy_commit_cache`) relies on: a not-found is benign
/// (empty), anything else propagates. A no-op without the `failpoints` feature
/// (the injected variant is therefore unreachable in release builds).
#[allow(unused_variables)]
pub(crate) fn maybe_fail_lance_open(name: &str) -> std::result::Result<(), lance::Error> {
    #[cfg(feature = "failpoints")]
    {
        fail::fail_point!(name, |_| {
            Err(lance::Error::io(format!(
                "injected failpoint triggered: {name}"
            )))
        });
    }
    Ok(())
}

/// Failpoint that injects a Lance `IncompatibleTransaction` — the variant a
/// concurrent `UpdateConfig` stamp race produces. Lets a test drive the v3→v4
/// stamp loop's exhaustion path (`commit_v4_stamp_idempotently`) deterministically;
/// it is otherwise near-unreachable, since a real concurrent winner stamps the SAME
/// value, so the loop's re-read returns `Ok` on the first retry. A no-op without the
/// `failpoints` feature.
#[allow(unused_variables)]
pub(crate) fn maybe_fail_lance_incompatible(name: &str) -> std::result::Result<(), lance::Error> {
    #[cfg(feature = "failpoints")]
    {
        fail::fail_point!(name, |_| {
            Err(lance::Error::incompatible_transaction_source(
                format!("injected failpoint triggered: {name}").into(),
            ))
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
