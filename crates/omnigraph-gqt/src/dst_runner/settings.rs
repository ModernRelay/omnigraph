//! Recorded process settings configure each worker and are verified before setup.

use std::process::Command;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EffectiveSettings {
    rayon_num_threads: usize,
    lance_cpu_threads: usize,
    lance_deterministic_backoff: bool,
    dst_entropy_seed: Option<u64>,
    lance_memory_pool: LanceMemoryPool,
    pub(super) tokio: TokioRuntime,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LanceMemoryPool {
    DependencyDefault,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum TokioRuntime {
    MultiThread {
        worker_threads: usize,
        thread_stack_bytes: usize,
    },
    SeededCurrentThread,
}

impl EffectiveSettings {
    pub(super) fn for_seed(seed: Option<u64>) -> Self {
        Self {
            rayon_num_threads: 1,
            lance_cpu_threads: 1,
            lance_deterministic_backoff: true,
            dst_entropy_seed: seed,
            lance_memory_pool: LanceMemoryPool::DependencyDefault,
            tokio: if seed.is_some() {
                TokioRuntime::SeededCurrentThread
            } else {
                TokioRuntime::MultiThread {
                    worker_threads: 2,
                    thread_stack_bytes: 16 * 1024 * 1024,
                }
            },
        }
    }

    pub(super) fn verify_expected(&self, seed: Option<u64>) -> Result<(), String> {
        if self != &Self::for_seed(seed) {
            return Err(
                "environment_changed: effective worker settings differ from this build".into(),
            );
        }
        Ok(())
    }

    fn environment(&self) -> [(&'static str, Option<String>); 5] {
        [
            (
                "RAYON_NUM_THREADS",
                Some(self.rayon_num_threads.to_string()),
            ),
            (
                "LANCE_CPU_THREADS",
                Some(self.lance_cpu_threads.to_string()),
            ),
            (
                "LANCE_DETERMINISTIC_BACKOFF",
                Some(u8::from(self.lance_deterministic_backoff).to_string()),
            ),
            (
                "DST_ENTROPY_SEED",
                self.dst_entropy_seed.map(|seed| seed.to_string()),
            ),
            ("LANCE_MEM_POOL_SIZE", None),
        ]
    }

    pub(super) fn configure(&self, command: &mut Command) {
        for (key, value) in self.environment() {
            if let Some(value) = value {
                command.env(key, value);
            } else {
                command.env_remove(key);
            }
        }
    }

    pub(super) fn verify_process(&self) -> Result<(), String> {
        self.verify_environment(|key| std::env::var_os(key))
    }

    fn verify_environment(
        &self,
        read: impl Fn(&str) -> Option<std::ffi::OsString>,
    ) -> Result<(), String> {
        for (key, expected) in self.environment() {
            let expected = expected.map(std::ffi::OsString::from);
            if read(key) != expected {
                return Err(format!(
                    "environment_changed: process {key} differs from recorded effective settings"
                ));
            }
        }
        for key in ["FAILPOINTS", "OMNIGRAPH_TRAVERSAL_MODE"] {
            if read(key).is_some() {
                return Err(format!("environment_changed: worker inherited {key}"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_drive_the_spawn_and_refuse_process_drift() {
        for seed in [None, Some(0), Some(u64::MAX)] {
            let settings = EffectiveSettings::for_seed(seed);
            let mut command = Command::new("unused-worker");
            settings.configure(&mut command);
            let environment = command
                .get_envs()
                .map(|(key, value)| (key.to_os_string(), value.map(ToOwned::to_owned)))
                .collect::<std::collections::BTreeMap<_, _>>();
            let read = |key: &str| {
                environment
                    .get(std::ffi::OsStr::new(key))
                    .cloned()
                    .flatten()
            };
            assert!(settings.verify_environment(read).is_ok());
            for key in [
                "RAYON_NUM_THREADS",
                "LANCE_CPU_THREADS",
                "LANCE_DETERMINISTIC_BACKOFF",
                "DST_ENTROPY_SEED",
                "LANCE_MEM_POOL_SIZE",
                "FAILPOINTS",
                "OMNIGRAPH_TRAVERSAL_MODE",
            ] {
                assert!(
                    settings
                        .verify_environment(|name| {
                            if name == key {
                                Some("changed".into())
                            } else {
                                read(name)
                            }
                        })
                        .is_err(),
                    "accepted process drift for {key}"
                );
            }
        }
    }

    #[test]
    fn recorded_settings_cannot_change_the_execution() {
        let expected = EffectiveSettings::for_seed(Some(42));
        let mut changed = expected.clone();
        changed.rayon_num_threads = 2;
        assert!(changed.verify_expected(Some(42)).is_err());
        assert!(expected.verify_expected(Some(0)).is_err());
        assert!(expected.verify_expected(None).is_err());
        let mut value = serde_json::to_value(&expected).unwrap();
        value["unknown"] = true.into();
        assert!(serde_json::from_value::<EffectiveSettings>(value).is_err());
    }

    #[cfg(tokio_unstable)]
    #[test]
    fn seeded_pool_settings_match_the_dst_owner() {
        let settings = EffectiveSettings::for_seed(Some(0));
        for (key, value) in omnigraph_dst::env_knobs::QUIESCE_ENV {
            assert!(settings.environment().contains(&(key, Some(value.into()))));
        }
    }
}
