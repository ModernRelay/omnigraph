use crate::error::Result;
#[cfg(feature = "failpoints")]
use crate::error::OmniError;

pub(crate) fn maybe_fail(_name: &str) -> Result<()> {
    #[cfg(feature = "failpoints")]
    {
        let name = _name;
        fail::fail_point!(name, |_| {
            return Err(OmniError::Manifest(format!(
                "injected failpoint triggered: {}",
                name
            )));
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
