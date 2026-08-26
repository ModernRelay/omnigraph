//! Deterministic process-effective machine evidence for durable benchmark records.
//!
//! Benchmark cases deliberately do not name a machine. The harness observes
//! the machine at invocation time and persists these facts beside the point
//! identity so result readers can refuse silent cross-machine comparisons.
//! Capture is fail-closed: a required fact that cannot be proved never becomes
//! the string `"unknown"` in an otherwise publishable record.
//! The hostname-derived label is only a non-secret, non-stable correlation
//! hint. It is neither anonymization nor a machine-identity guarantee.

use std::error::Error;
use std::fmt::{Display, Formatter};

#[cfg(any(target_os = "linux", test))]
use std::collections::BTreeMap;
#[cfg(any(target_os = "linux", test))]
use std::collections::BTreeSet;
#[cfg(any(target_os = "linux", test))]
use std::fs::{self, File};
#[cfg(any(target_os = "linux", test))]
use std::io::Read;
#[cfg(any(target_os = "linux", test))]
use std::path::{Component, Path, PathBuf};

#[cfg(target_os = "macos")]
use std::ffi::CString;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Version of the persisted machine-identity contract.
pub const MACHINE_IDENTITY_FORMAT_VERSION: u32 = 1;
/// Version of the fixed resource-limit set covered by the persisted digest.
pub const RESOURCE_LIMIT_SCOPE_VERSION: u32 = 1;

const MAX_FACT_BYTES: usize = 1_024;
const HOSTNAME_DIGEST_DOMAIN: &[u8] = b"omnigraph-bench-hostname-v1\0";
const RESOURCE_LIMIT_DIGEST_DOMAIN: &[u8] = b"omnigraph-bench-resource-limits-v1\0";
#[cfg(any(target_os = "linux", test))]
const CPU_AFFINITY_DIGEST_DOMAIN: &[u8] = b"omnigraph-bench-cpu-affinity-v1\0";
#[cfg(target_os = "linux")]
const CGROUP_HIERARCHY_DIGEST_DOMAIN: &[u8] = b"omnigraph-bench-cgroup-v2-v1\0";
const HOSTNAME_LABEL_PREFIX: &str = "hostname-sha256:";

#[cfg(target_os = "linux")]
const MAX_OS_RELEASE_BYTES: usize = 64 * 1_024;
#[cfg(target_os = "linux")]
const MAX_CPUINFO_BYTES: usize = 8 * 1_024 * 1_024;
#[cfg(target_os = "linux")]
const MAX_MEMINFO_BYTES: usize = 1024 * 1_024;
#[cfg(target_os = "linux")]
const MAX_TOPOLOGY_VALUE_BYTES: usize = 128;
#[cfg(target_os = "linux")]
const MAX_PROC_STATUS_BYTES: usize = 1024 * 1024;
#[cfg(target_os = "linux")]
const MAX_CGROUP_DOCUMENT_BYTES: usize = 64 * 1024;
#[cfg(target_os = "linux")]
const MAX_MOUNTINFO_BYTES: usize = 4 * 1024 * 1024;
#[cfg(any(target_os = "linux", test))]
const MAX_CGROUP_VALUE_BYTES: usize = 64 * 1024;
#[cfg(any(target_os = "linux", test))]
const MAX_CGROUP_CONTROL_FILES: usize = 256;
#[cfg(any(target_os = "linux", test))]
const MAX_CGROUP_CONTROL_NAME_BYTES: usize = 255;
#[cfg(any(target_os = "linux", test))]
const MAX_CGROUP_CONTROL_TOTAL_BYTES: usize = 4 * 1024 * 1024;
#[cfg(any(target_os = "linux", test))]
const MAX_CGROUP_V2_MOUNTS: usize = 256;
#[cfg(target_os = "linux")]
const MAX_CGROUP_HIERARCHY_DEPTH: usize = 256;
#[cfg(any(target_os = "linux", test))]
const MAX_LOGICAL_CPUS: usize = 65_536;

/// Complete machine identity carried by every durable benchmark record.
///
/// This DTO contains no maps and serde emits struct fields in declaration
/// order. Together with canonical string validation, that gives callers a
/// deterministic JSON projection. The raw hostname is omitted, but its digest
/// is not a privacy boundary and remains guessable when hostnames are known.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineIdentityV1 {
    pub format_version: u32,
    pub os_name: String,
    pub os_version: String,
    pub kernel_version: String,
    pub architecture: String,
    pub cpu_model: String,
    pub logical_cores: u32,
    pub physical_cores: u32,
    /// Effective memory available to the process. On Linux this is the lower
    /// of host memory and the inherited cgroup-v2 memory limit.
    pub total_memory_bytes: u64,
    pub resource_control: ResourceControlV1,
    /// Process-effective nice value, scheduler policy, and priority.
    pub scheduling: SchedulingIdentityV1,
    /// Digest of one fixed, ordered set of process-effective soft/hard limits.
    pub resource_limits: ResourceLimitIdentityV1,
    /// Non-secret, non-stable correlation hint derived from the hostname.
    ///
    /// This is not anonymization or machine identity: a known hostname can be
    /// tested against the digest, and hostname changes split the correlation.
    pub machine_label: String,
}

/// Scheduler policy classes which are completely represented by policy and
/// priority on the supported hosts. Linux deadline and unknown policies are
/// refused because they carry additional parameters this format does not hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SchedulerPolicyV1 {
    Other,
    Fifo,
    RoundRobin,
    LinuxBatch,
    LinuxIdle,
}

/// Process-effective scheduling state inherited unchanged by a benchmark
/// worker. Capture refuses reset-on-fork rather than describing the parent as
/// though it were the child.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulingIdentityV1 {
    pub nice_level: i32,
    pub policy: SchedulerPolicyV1,
    pub priority: i32,
    pub reset_on_fork: bool,
}

/// Digest of the common process resource-limit scope.
///
/// Scope v1 hashes, in declaration order, both the soft and hard values for
/// address space, core size, CPU time, data size, file size, locked memory,
/// open files, process count, and stack size. Infinity is explicitly tagged
/// rather than encoded as a platform integer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLimitIdentityV1 {
    pub scope_version: u32,
    pub values_sha256: String,
}

/// Process-effective resource controls which can materially change timings on
/// the same host. Linux cgroup-v1 is deliberately refused during capture
/// rather than silently projected as unconstrained.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ResourceControlV1 {
    MacosNative,
    LinuxUncontrolled {
        cpu_affinity_sha256: String,
    },
    LinuxCgroupV2 {
        cpu_affinity_sha256: String,
        hierarchy_sha256: String,
        effective_cpu_quota_micros: Option<u64>,
        effective_cpu_period_micros: Option<u64>,
        effective_memory_limit_bytes: Option<u64>,
    },
}

impl MachineIdentityV1 {
    /// Revalidate an identity loaded from an untrusted durable record.
    pub fn validate(&self) -> Result<(), MachineIdentityError> {
        validate_machine_identity(self)
    }
}

/// Typed failure from capture or validation of machine identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineIdentityError {
    pub code: &'static str,
    pub field: &'static str,
    pub message: String,
}

impl MachineIdentityError {
    fn new(code: &'static str, field: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            field,
            message: message.into(),
        }
    }

    fn probe(field: &'static str, message: impl Into<String>) -> Self {
        Self::new("machine_probe_failed", field, message)
    }

    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::new("invalid_machine_identity", field, message)
    }
}

impl Display for MachineIdentityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} for {}: {}",
            self.code, self.field, self.message
        )
    }
}

impl Error for MachineIdentityError {}

/// Capture and validate the current machine without reading credentials or
/// persisting a raw hostname.
pub fn capture_machine_identity() -> Result<MachineIdentityV1, MachineIdentityError> {
    let identity = capture_platform_identity()?;
    validate_machine_identity(&identity)?;
    Ok(identity)
}

/// Validate that a deserialized identity is complete and canonical.
pub fn validate_machine_identity(identity: &MachineIdentityV1) -> Result<(), MachineIdentityError> {
    if identity.format_version != MACHINE_IDENTITY_FORMAT_VERSION {
        return Err(MachineIdentityError::invalid(
            "format_version",
            format!(
                "expected {MACHINE_IDENTITY_FORMAT_VERSION}, observed {}",
                identity.format_version
            ),
        ));
    }

    for (field, value) in [
        ("os_name", identity.os_name.as_str()),
        ("os_version", identity.os_version.as_str()),
        ("kernel_version", identity.kernel_version.as_str()),
        ("architecture", identity.architecture.as_str()),
        ("cpu_model", identity.cpu_model.as_str()),
    ] {
        let canonical = canonical_fact(field, value)?;
        if canonical != value {
            return Err(MachineIdentityError::invalid(
                field,
                "value is not in canonical single-line form",
            ));
        }
    }

    if identity.logical_cores == 0 {
        return Err(MachineIdentityError::invalid(
            "logical_cores",
            "logical core count must be nonzero",
        ));
    }
    if identity.physical_cores == 0 {
        return Err(MachineIdentityError::invalid(
            "physical_cores",
            "physical core count must be nonzero",
        ));
    }
    if identity.physical_cores > identity.logical_cores {
        return Err(MachineIdentityError::invalid(
            "physical_cores",
            "physical core count cannot exceed logical core count",
        ));
    }
    if identity.total_memory_bytes == 0 {
        return Err(MachineIdentityError::invalid(
            "total_memory_bytes",
            "total memory must be nonzero",
        ));
    }
    validate_resource_control(
        &identity.os_name,
        identity.total_memory_bytes,
        &identity.resource_control,
    )?;
    validate_scheduling(&identity.os_name, &identity.scheduling)?;
    validate_resource_limits(&identity.resource_limits)?;
    validate_machine_label(&identity.machine_label)
}

fn validate_scheduling(
    os_name: &str,
    scheduling: &SchedulingIdentityV1,
) -> Result<(), MachineIdentityError> {
    let nice_range = match os_name {
        "linux" => -20..=19,
        "macos" => -20..=20,
        _ => {
            return Err(MachineIdentityError::invalid(
                "scheduling",
                "scheduling identity requires a supported operating system",
            ));
        }
    };
    if !nice_range.contains(&scheduling.nice_level) {
        return Err(MachineIdentityError::invalid(
            "scheduling.nice_level",
            format!(
                "nice level {} is outside the supported host range",
                scheduling.nice_level
            ),
        ));
    }

    match (os_name, scheduling.policy) {
        ("macos", SchedulerPolicyV1::Other)
        | ("macos", SchedulerPolicyV1::Fifo)
        | ("macos", SchedulerPolicyV1::RoundRobin)
        | ("linux", SchedulerPolicyV1::Other)
        | ("linux", SchedulerPolicyV1::Fifo)
        | ("linux", SchedulerPolicyV1::RoundRobin)
        | ("linux", SchedulerPolicyV1::LinuxBatch)
        | ("linux", SchedulerPolicyV1::LinuxIdle) => {}
        _ => {
            return Err(MachineIdentityError::invalid(
                "scheduling.policy",
                "scheduler policy does not match the recorded operating system",
            ));
        }
    }
    if scheduling.reset_on_fork {
        return Err(MachineIdentityError::invalid(
            "scheduling.reset_on_fork",
            "reset-on-fork would change worker scheduling, so benchmark capture requires it to be disabled",
        ));
    }
    match (os_name, scheduling.policy) {
        (
            "linux",
            SchedulerPolicyV1::Other | SchedulerPolicyV1::LinuxBatch | SchedulerPolicyV1::LinuxIdle,
        ) if scheduling.priority != 0 => Err(MachineIdentityError::invalid(
            "scheduling.priority",
            "non-real-time scheduler policies require priority zero",
        )),
        _ if scheduling.priority < 0 => Err(MachineIdentityError::invalid(
            "scheduling.priority",
            "scheduler priority cannot be negative",
        )),
        _ => Ok(()),
    }
}

fn validate_resource_limits(
    resource_limits: &ResourceLimitIdentityV1,
) -> Result<(), MachineIdentityError> {
    if resource_limits.scope_version != RESOURCE_LIMIT_SCOPE_VERSION {
        return Err(MachineIdentityError::invalid(
            "resource_limits.scope_version",
            format!(
                "expected {RESOURCE_LIMIT_SCOPE_VERSION}, observed {}",
                resource_limits.scope_version
            ),
        ));
    }
    validate_hex_sha256(
        "resource_limits.values_sha256",
        &resource_limits.values_sha256,
    )
}

fn validate_resource_control(
    os_name: &str,
    effective_memory_bytes: u64,
    control: &ResourceControlV1,
) -> Result<(), MachineIdentityError> {
    match (os_name, control) {
        ("macos", ResourceControlV1::MacosNative) => Ok(()),
        (
            "linux",
            ResourceControlV1::LinuxUncontrolled {
                cpu_affinity_sha256,
            },
        ) => validate_hex_sha256("resource_control.cpu_affinity_sha256", cpu_affinity_sha256),
        (
            "linux",
            ResourceControlV1::LinuxCgroupV2 {
                cpu_affinity_sha256,
                hierarchy_sha256,
                effective_cpu_quota_micros,
                effective_cpu_period_micros,
                effective_memory_limit_bytes,
            },
        ) => {
            validate_hex_sha256("resource_control.cpu_affinity_sha256", cpu_affinity_sha256)?;
            validate_hex_sha256("resource_control.hierarchy_sha256", hierarchy_sha256)?;
            match (effective_cpu_quota_micros, effective_cpu_period_micros) {
                (Some(quota), Some(period)) if *quota != 0 && *period != 0 => {}
                (None, None) => {}
                _ => {
                    return Err(MachineIdentityError::invalid(
                        "resource_control.cpu_quota",
                        "effective CPU quota and period must be both positive or both absent",
                    ));
                }
            }
            if effective_memory_limit_bytes.is_some_and(|limit| limit == 0) {
                return Err(MachineIdentityError::invalid(
                    "resource_control.effective_memory_limit_bytes",
                    "effective memory limit must be positive when present",
                ));
            }
            if effective_memory_limit_bytes.is_some_and(|limit| effective_memory_bytes > limit) {
                return Err(MachineIdentityError::invalid(
                    "total_memory_bytes",
                    "effective memory cannot exceed the inherited cgroup-v2 limit",
                ));
            }
            Ok(())
        }
        _ => Err(MachineIdentityError::invalid(
            "resource_control",
            "resource-control kind does not match the recorded operating system",
        )),
    }
}

fn validate_hex_sha256(field: &'static str, value: &str) -> Result<(), MachineIdentityError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(MachineIdentityError::invalid(
            field,
            "digest must be exactly 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn capture_platform_identity() -> Result<MachineIdentityV1, MachineIdentityError> {
    let hostname = macos_sysctl_string("kern.hostname", "machine_label")?;
    Ok(MachineIdentityV1 {
        format_version: MACHINE_IDENTITY_FORMAT_VERSION,
        os_name: "macos".to_string(),
        os_version: macos_sysctl_string("kern.osproductversion", "os_version")?,
        kernel_version: macos_sysctl_string("kern.osrelease", "kernel_version")?,
        architecture: std::env::consts::ARCH.to_string(),
        cpu_model: macos_sysctl_string("machdep.cpu.brand_string", "cpu_model")?,
        logical_cores: macos_sysctl_u32("hw.logicalcpu", "logical_cores")?,
        physical_cores: macos_sysctl_u32("hw.physicalcpu", "physical_cores")?,
        total_memory_bytes: macos_sysctl_u64("hw.memsize", "total_memory_bytes")?,
        resource_control: ResourceControlV1::MacosNative,
        scheduling: capture_process_scheduling()?,
        resource_limits: capture_resource_limits()?,
        machine_label: machine_label_from_hostname(&hostname)?,
    })
}

#[cfg(target_os = "linux")]
fn capture_platform_identity() -> Result<MachineIdentityV1, MachineIdentityError> {
    let os_release = read_bounded_utf8(
        Path::new("/etc/os-release"),
        MAX_OS_RELEASE_BYTES,
        "os_version",
    )?;
    let distribution = os_release_value(&os_release, "NAME").ok_or_else(|| {
        MachineIdentityError::probe("os_name", "NAME is absent from os-release")
    })??;
    let distribution_version = os_release_value(&os_release, "VERSION_ID")
        .or_else(|| os_release_value(&os_release, "VERSION"))
        .ok_or_else(|| {
            MachineIdentityError::probe(
                "os_version",
                "VERSION_ID and VERSION are absent from os-release",
            )
        })??;
    let os_version = canonical_fact(
        "os_version",
        &format!("{distribution} {distribution_version}"),
    )?;
    let cpuinfo = read_bounded_utf8(Path::new("/proc/cpuinfo"), MAX_CPUINFO_BYTES, "cpu_model")?;
    let status = read_bounded_utf8(
        Path::new("/proc/self/status"),
        MAX_PROC_STATUS_BYTES,
        "resource_control",
    )?;
    let processors = linux_allowed_cpu_list(&status)?;
    let (logical_cores, physical_cores) = linux_cpu_topology(&processors)?;
    let meminfo = read_bounded_utf8(
        Path::new("/proc/meminfo"),
        MAX_MEMINFO_BYTES,
        "total_memory_bytes",
    )?;
    let hostname = read_bounded_utf8(
        Path::new("/proc/sys/kernel/hostname"),
        MAX_FACT_BYTES,
        "machine_label",
    )?;

    let host_memory_bytes = linux_total_memory_bytes(&meminfo)?;
    let (resource_control, effective_memory_bytes) =
        linux_resource_control(&processors, host_memory_bytes)?;

    Ok(MachineIdentityV1 {
        format_version: MACHINE_IDENTITY_FORMAT_VERSION,
        os_name: "linux".to_string(),
        os_version,
        kernel_version: canonical_fact(
            "kernel_version",
            &read_bounded_utf8(
                Path::new("/proc/sys/kernel/osrelease"),
                MAX_FACT_BYTES,
                "kernel_version",
            )?,
        )?,
        architecture: std::env::consts::ARCH.to_string(),
        cpu_model: linux_cpu_model(&cpuinfo)?,
        logical_cores,
        physical_cores,
        total_memory_bytes: effective_memory_bytes,
        resource_control,
        scheduling: capture_process_scheduling()?,
        resource_limits: capture_resource_limits()?,
        machine_label: machine_label_from_hostname(&hostname)?,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn capture_nice_level() -> Result<i32, MachineIdentityError> {
    // `getpriority` may legitimately return -1, so errno must be cleared and
    // inspected rather than treating the return value alone as failure.
    // SAFETY: the platform-specific errno accessor returns thread-local errno.
    unsafe { *errno_location() = 0 };
    // SAFETY: PRIO_PROCESS with who=0 queries the calling process and does not
    // write through caller-provided pointers.
    let nice_level = unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) };
    // SAFETY: the pointer remains valid for the calling thread.
    let errno = unsafe { *errno_location() };
    if nice_level == -1 && errno != 0 {
        return Err(MachineIdentityError::probe(
            "scheduling.nice_level",
            format!(
                "getpriority failed: {}",
                std::io::Error::from_raw_os_error(errno)
            ),
        ));
    }
    Ok(nice_level)
}

#[cfg(target_os = "linux")]
unsafe fn errno_location() -> *mut libc::c_int {
    // SAFETY: forwarded directly to libc's thread-local errno accessor.
    unsafe { libc::__errno_location() }
}

#[cfg(target_os = "macos")]
unsafe fn errno_location() -> *mut libc::c_int {
    // SAFETY: forwarded directly to libc's thread-local errno accessor.
    unsafe { libc::__error() }
}

#[cfg(target_os = "linux")]
fn capture_process_scheduling() -> Result<SchedulingIdentityV1, MachineIdentityError> {
    // SAFETY: pid=0 queries the calling thread and writes no caller memory.
    let raw_policy = unsafe { libc::sched_getscheduler(0) };
    if raw_policy == -1 {
        return Err(MachineIdentityError::probe(
            "scheduling.policy",
            format!(
                "sched_getscheduler failed: {}",
                std::io::Error::last_os_error()
            ),
        ));
    }
    let mut parameter = std::mem::MaybeUninit::<libc::sched_param>::zeroed();
    // SAFETY: `parameter` points to writable sched_param storage and pid=0
    // queries the calling thread.
    let status = unsafe { libc::sched_getparam(0, parameter.as_mut_ptr()) };
    if status != 0 {
        return Err(MachineIdentityError::probe(
            "scheduling.priority",
            format!("sched_getparam failed: {}", std::io::Error::last_os_error()),
        ));
    }
    // SAFETY: a successful sched_getparam initialized the whole value.
    let priority = unsafe { parameter.assume_init() }.sched_priority;
    decode_linux_scheduling(raw_policy, priority, capture_nice_level()?)
}

#[cfg(target_os = "linux")]
fn decode_linux_scheduling(
    raw_policy: libc::c_int,
    priority: libc::c_int,
    nice_level: libc::c_int,
) -> Result<SchedulingIdentityV1, MachineIdentityError> {
    if raw_policy < 0 {
        return Err(MachineIdentityError::probe(
            "scheduling.policy",
            "scheduler policy cannot be negative",
        ));
    }
    let reset_on_fork = raw_policy & libc::SCHED_RESET_ON_FORK != 0;
    let policy = raw_policy & !libc::SCHED_RESET_ON_FORK;
    let policy = if policy == libc::SCHED_OTHER {
        SchedulerPolicyV1::Other
    } else if policy == libc::SCHED_FIFO {
        SchedulerPolicyV1::Fifo
    } else if policy == libc::SCHED_RR {
        SchedulerPolicyV1::RoundRobin
    } else if policy == libc::SCHED_BATCH {
        SchedulerPolicyV1::LinuxBatch
    } else if policy == libc::SCHED_IDLE {
        SchedulerPolicyV1::LinuxIdle
    } else if policy == libc::SCHED_DEADLINE {
        return Err(MachineIdentityError::probe(
            "scheduling.policy",
            "SCHED_DEADLINE is unsupported because policy and priority do not capture its runtime, deadline, and period",
        ));
    } else {
        return Err(MachineIdentityError::probe(
            "scheduling.policy",
            format!("unsupported Linux scheduler policy value {policy}"),
        ));
    };
    let scheduling = SchedulingIdentityV1 {
        nice_level,
        policy,
        priority,
        reset_on_fork,
    };
    validate_scheduling("linux", &scheduling)?;
    Ok(scheduling)
}

#[cfg(target_os = "macos")]
fn capture_process_scheduling() -> Result<SchedulingIdentityV1, MachineIdentityError> {
    let mut raw_policy = 0;
    let mut parameter = std::mem::MaybeUninit::<libc::sched_param>::zeroed();
    // SAFETY: pthread_self returns the calling thread, and both output
    // pointers reference writable storage for the duration of the call.
    let status = unsafe {
        libc::pthread_getschedparam(
            libc::pthread_self(),
            &mut raw_policy,
            parameter.as_mut_ptr(),
        )
    };
    if status != 0 {
        return Err(MachineIdentityError::probe(
            "scheduling",
            format!(
                "pthread_getschedparam failed: {}",
                std::io::Error::from_raw_os_error(status)
            ),
        ));
    }
    // SAFETY: a successful pthread_getschedparam initialized the whole value.
    let priority = unsafe { parameter.assume_init() }.sched_priority;
    decode_macos_scheduling(raw_policy, priority, capture_nice_level()?)
}

#[cfg(target_os = "macos")]
fn decode_macos_scheduling(
    raw_policy: libc::c_int,
    priority: libc::c_int,
    nice_level: libc::c_int,
) -> Result<SchedulingIdentityV1, MachineIdentityError> {
    let policy = if raw_policy == libc::SCHED_OTHER {
        SchedulerPolicyV1::Other
    } else if raw_policy == libc::SCHED_FIFO {
        SchedulerPolicyV1::Fifo
    } else if raw_policy == libc::SCHED_RR {
        SchedulerPolicyV1::RoundRobin
    } else {
        return Err(MachineIdentityError::probe(
            "scheduling.policy",
            format!("unsupported macOS scheduler policy value {raw_policy}"),
        ));
    };
    let scheduling = SchedulingIdentityV1 {
        nice_level,
        policy,
        priority,
        reset_on_fork: false,
    };
    validate_scheduling("macos", &scheduling)?;
    Ok(scheduling)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanonicalLimitValue {
    Finite(u64),
    Infinity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CanonicalResourceLimit {
    soft: CanonicalLimitValue,
    hard: CanonicalLimitValue,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn capture_resource_limits() -> Result<ResourceLimitIdentityV1, MachineIdentityError> {
    macro_rules! read_limit {
        ($name:literal, $resource:expr) => {{
            let mut limit = std::mem::MaybeUninit::<libc::rlimit>::zeroed();
            // SAFETY: `limit` points to writable rlimit storage and the
            // resource constants below are defined by both supported hosts.
            let status = unsafe { libc::getrlimit($resource, limit.as_mut_ptr()) };
            if status != 0 {
                return Err(MachineIdentityError::probe(
                    "resource_limits",
                    format!(
                        "getrlimit({}) failed: {}",
                        $name,
                        std::io::Error::last_os_error()
                    ),
                ));
            }
            // SAFETY: a successful getrlimit initialized the whole value.
            let limit = unsafe { limit.assume_init() };
            ($name.as_bytes(), canonical_resource_limit(limit)?)
        }};
    }

    let values = [
        read_limit!("address-space", libc::RLIMIT_AS),
        read_limit!("core-size", libc::RLIMIT_CORE),
        read_limit!("cpu-time", libc::RLIMIT_CPU),
        read_limit!("data-size", libc::RLIMIT_DATA),
        read_limit!("file-size", libc::RLIMIT_FSIZE),
        read_limit!("locked-memory", libc::RLIMIT_MEMLOCK),
        read_limit!("open-files", libc::RLIMIT_NOFILE),
        read_limit!("process-count", libc::RLIMIT_NPROC),
        read_limit!("stack-size", libc::RLIMIT_STACK),
    ];
    Ok(ResourceLimitIdentityV1 {
        scope_version: RESOURCE_LIMIT_SCOPE_VERSION,
        values_sha256: resource_limit_values_sha256(&values),
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn canonical_resource_limit(
    limit: libc::rlimit,
) -> Result<CanonicalResourceLimit, MachineIdentityError> {
    Ok(CanonicalResourceLimit {
        soft: canonical_limit_value(limit.rlim_cur)?,
        hard: canonical_limit_value(limit.rlim_max)?,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn canonical_limit_value(value: libc::rlim_t) -> Result<CanonicalLimitValue, MachineIdentityError> {
    if value == libc::RLIM_INFINITY {
        return Ok(CanonicalLimitValue::Infinity);
    }
    let widened = u128::from(value);
    let value = u64::try_from(widened).map_err(|_| {
        MachineIdentityError::probe(
            "resource_limits",
            "finite rlimit value does not fit the canonical u64 representation",
        )
    })?;
    Ok(CanonicalLimitValue::Finite(value))
}

fn resource_limit_values_sha256(values: &[(&[u8], CanonicalResourceLimit)]) -> String {
    let mut digest = Sha256::new();
    digest.update(RESOURCE_LIMIT_DIGEST_DOMAIN);
    digest.update(RESOURCE_LIMIT_SCOPE_VERSION.to_be_bytes());
    digest.update(
        u64::try_from(values.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for (name, value) in values {
        digest.update(u64::try_from(name.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(name);
        hash_limit_value(&mut digest, value.soft);
        hash_limit_value(&mut digest, value.hard);
    }
    format!("{:x}", digest.finalize())
}

fn hash_limit_value(digest: &mut Sha256, value: CanonicalLimitValue) {
    match value {
        CanonicalLimitValue::Finite(value) => {
            digest.update([0]);
            digest.update(value.to_be_bytes());
        }
        CanonicalLimitValue::Infinity => digest.update([1]),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn capture_platform_identity() -> Result<MachineIdentityV1, MachineIdentityError> {
    Err(MachineIdentityError::probe(
        "platform",
        format!(
            "machine identity capture is unsupported on {}",
            std::env::consts::OS
        ),
    ))
}

fn canonical_fact(field: &'static str, raw: &str) -> Result<String, MachineIdentityError> {
    if raw.len() > MAX_FACT_BYTES {
        return Err(MachineIdentityError::invalid(
            field,
            format!("value exceeds the {MAX_FACT_BYTES}-byte bound"),
        ));
    }
    let canonical = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if canonical.is_empty() {
        return Err(MachineIdentityError::invalid(field, "value is empty"));
    }
    if ["unknown", "n/a", "none", "unspecified"]
        .iter()
        .any(|marker| canonical.eq_ignore_ascii_case(marker))
    {
        return Err(MachineIdentityError::invalid(
            field,
            "an unknown marker is not evidence",
        ));
    }
    Ok(canonical)
}

fn machine_label_from_hostname(hostname: &str) -> Result<String, MachineIdentityError> {
    let canonical = canonical_fact("machine_label", hostname)?;
    let canonical = canonical.trim_end_matches('.').to_ascii_lowercase();
    if canonical.is_empty() {
        return Err(MachineIdentityError::invalid(
            "machine_label",
            "hostname is empty after normalization",
        ));
    }
    let mut hasher = Sha256::new();
    hasher.update(HOSTNAME_DIGEST_DOMAIN);
    hasher.update(canonical.as_bytes());
    Ok(format!("{HOSTNAME_LABEL_PREFIX}{:x}", hasher.finalize()))
}

fn validate_machine_label(label: &str) -> Result<(), MachineIdentityError> {
    let digest = label.strip_prefix(HOSTNAME_LABEL_PREFIX).ok_or_else(|| {
        MachineIdentityError::invalid(
            "machine_label",
            format!("label must use the `{HOSTNAME_LABEL_PREFIX}` policy"),
        )
    })?;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(MachineIdentityError::invalid(
            "machine_label",
            "hostname digest must be exactly 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn macos_sysctl_raw(
    name: &'static str,
    field: &'static str,
) -> Result<Vec<u8>, MachineIdentityError> {
    let name = CString::new(name)
        .map_err(|_| MachineIdentityError::probe(field, "sysctl name contains a NUL byte"))?;
    let mut length = 0_usize;
    // SAFETY: `name` is NUL-terminated, the output pointer is null for the
    // sizing call, and `length` points to writable `size_t` storage.
    let status = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            std::ptr::null_mut(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if status != 0 {
        return Err(MachineIdentityError::probe(
            field,
            format!("sysctl sizing failed: {}", std::io::Error::last_os_error()),
        ));
    }
    if length == 0 || length > MAX_FACT_BYTES {
        return Err(MachineIdentityError::probe(
            field,
            format!("sysctl result length {length} is outside 1..={MAX_FACT_BYTES}"),
        ));
    }

    let mut value = vec![0_u8; length];
    // SAFETY: `value` owns `length` writable bytes and `length` is passed by
    // mutable reference so the kernel can report the actual bytes written.
    let status = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            value.as_mut_ptr().cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if status != 0 {
        return Err(MachineIdentityError::probe(
            field,
            format!("sysctl read failed: {}", std::io::Error::last_os_error()),
        ));
    }
    if length > value.len() {
        return Err(MachineIdentityError::probe(
            field,
            "sysctl reported more bytes than its bounded output buffer",
        ));
    }
    value.truncate(length);
    Ok(value)
}

#[cfg(target_os = "macos")]
fn macos_sysctl_string(
    name: &'static str,
    field: &'static str,
) -> Result<String, MachineIdentityError> {
    let mut value = macos_sysctl_raw(name, field)?;
    while value.last() == Some(&0) {
        value.pop();
    }
    let value = String::from_utf8(value)
        .map_err(|_| MachineIdentityError::probe(field, "sysctl result is not valid UTF-8"))?;
    canonical_fact(field, &value)
}

#[cfg(target_os = "macos")]
fn macos_sysctl_u32(name: &'static str, field: &'static str) -> Result<u32, MachineIdentityError> {
    let value = macos_sysctl_raw(name, field)?;
    let bytes: [u8; 4] = value.try_into().map_err(|value: Vec<u8>| {
        MachineIdentityError::probe(
            field,
            format!("expected a 4-byte integer, observed {} bytes", value.len()),
        )
    })?;
    Ok(u32::from_ne_bytes(bytes))
}

#[cfg(target_os = "macos")]
fn macos_sysctl_u64(name: &'static str, field: &'static str) -> Result<u64, MachineIdentityError> {
    let value = macos_sysctl_raw(name, field)?;
    let bytes: [u8; 8] = value.try_into().map_err(|value: Vec<u8>| {
        MachineIdentityError::probe(
            field,
            format!("expected an 8-byte integer, observed {} bytes", value.len()),
        )
    })?;
    Ok(u64::from_ne_bytes(bytes))
}

#[cfg(any(target_os = "linux", test))]
fn os_release_value(
    document: &str,
    wanted: &'static str,
) -> Option<Result<String, MachineIdentityError>> {
    document.lines().find_map(|line| {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let (key, value) = line.split_once('=')?;
        (key.trim() == wanted).then(|| decode_os_release_value(wanted, value))
    })
}

#[cfg(any(target_os = "linux", test))]
fn decode_os_release_value(field: &'static str, raw: &str) -> Result<String, MachineIdentityError> {
    let raw = raw.trim();
    let decoded = if raw.starts_with('"') || raw.starts_with('\'') {
        let quote = raw.as_bytes()[0];
        if raw.len() < 2 || raw.as_bytes().last().copied() != Some(quote) {
            return Err(MachineIdentityError::probe(
                field,
                "os-release contains an unterminated quoted value",
            ));
        }
        let inner = &raw[1..raw.len() - 1];
        let mut decoded = String::with_capacity(inner.len());
        let mut characters = inner.chars();
        while let Some(character) = characters.next() {
            if character == '\\' {
                let escaped = characters.next().ok_or_else(|| {
                    MachineIdentityError::probe(field, "os-release value ends in an escape")
                })?;
                decoded.push(escaped);
            } else {
                decoded.push(character);
            }
        }
        decoded
    } else {
        raw.to_string()
    };
    canonical_fact(field, &decoded)
}

#[cfg(target_os = "linux")]
fn read_bounded_utf8(
    path: &Path,
    maximum: usize,
    field: &'static str,
) -> Result<String, MachineIdentityError> {
    let file = File::open(path).map_err(|error| {
        MachineIdentityError::probe(field, format!("could not open {}: {error}", path.display()))
    })?;
    let limit = u64::try_from(maximum)
        .expect("bounded machine probe sizes fit u64")
        .saturating_add(1);
    let mut bytes = Vec::with_capacity(maximum.min(64 * 1_024).saturating_add(1));
    file.take(limit).read_to_end(&mut bytes).map_err(|error| {
        MachineIdentityError::probe(field, format!("could not read {}: {error}", path.display()))
    })?;
    if bytes.len() > maximum {
        return Err(MachineIdentityError::probe(
            field,
            format!("{} exceeds the {maximum}-byte probe bound", path.display()),
        ));
    }
    String::from_utf8(bytes).map_err(|_| {
        MachineIdentityError::probe(field, format!("{} is not valid UTF-8", path.display()))
    })
}

#[cfg(any(target_os = "linux", test))]
fn read_optional_bounded_utf8(
    path: &Path,
    maximum: usize,
    field: &'static str,
) -> Result<Option<String>, MachineIdentityError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(MachineIdentityError::probe(
                field,
                format!("could not open {}: {error}", path.display()),
            ));
        }
    };
    let limit = u64::try_from(maximum)
        .expect("bounded machine probe sizes fit u64")
        .saturating_add(1);
    let mut bytes = Vec::with_capacity(maximum.min(64 * 1_024).saturating_add(1));
    file.take(limit).read_to_end(&mut bytes).map_err(|error| {
        MachineIdentityError::probe(field, format!("could not read {}: {error}", path.display()))
    })?;
    if bytes.len() > maximum {
        return Err(MachineIdentityError::probe(
            field,
            format!("{} exceeds the {maximum}-byte probe bound", path.display()),
        ));
    }
    String::from_utf8(bytes).map(Some).map_err(|_| {
        MachineIdentityError::probe(field, format!("{} is not valid UTF-8", path.display()))
    })
}

#[cfg(any(target_os = "linux", test))]
fn linux_cpu_model(cpuinfo: &str) -> Result<String, MachineIdentityError> {
    const CANDIDATES: [&str; 4] = ["model name", "hardware", "cpu model", "processor"];
    let mut found: [Option<&str>; CANDIDATES.len()] = [None; CANDIDATES.len()];
    for line in cpuinfo.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        for (index, candidate) in CANDIDATES.iter().enumerate() {
            if name.eq_ignore_ascii_case(candidate)
                && !value.trim().is_empty()
                && !(*candidate == "processor"
                    && value.trim().bytes().all(|byte| byte.is_ascii_digit()))
            {
                found[index].get_or_insert(value);
            }
        }
    }
    let model = found.into_iter().flatten().next().ok_or_else(|| {
        MachineIdentityError::probe("cpu_model", "cpuinfo contains no CPU model field")
    })?;
    canonical_fact("cpu_model", model)
}

#[cfg(target_os = "linux")]
fn linux_cpu_topology(processors: &[u32]) -> Result<(u32, u32), MachineIdentityError> {
    let logical_cores = u32::try_from(processors.len()).map_err(|_| {
        MachineIdentityError::probe("logical_cores", "logical CPU count does not fit u32")
    })?;
    let mut physical = BTreeSet::new();
    for processor in processors {
        let base = format!("/sys/devices/system/cpu/cpu{processor}/topology");
        let package =
            read_linux_topology_id(&format!("{base}/physical_package_id"), "physical_cores")?;
        let core = read_linux_topology_id(&format!("{base}/core_id"), "physical_cores")?;
        physical.insert((package, core));
    }
    let physical_cores = u32::try_from(physical.len()).map_err(|_| {
        MachineIdentityError::probe("physical_cores", "physical CPU count does not fit u32")
    })?;
    Ok((logical_cores, physical_cores))
}

#[cfg(target_os = "linux")]
fn linux_allowed_cpu_list(status: &str) -> Result<Vec<u32>, MachineIdentityError> {
    let value = status
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            (name.trim() == "Cpus_allowed_list").then_some(value)
        })
        .ok_or_else(|| {
            MachineIdentityError::probe(
                "resource_control",
                "/proc/self/status contains no Cpus_allowed_list",
            )
        })?;
    parse_linux_cpu_list(value)
}

#[cfg(target_os = "linux")]
fn linux_resource_control(
    processors: &[u32],
    host_memory_bytes: u64,
) -> Result<(ResourceControlV1, u64), MachineIdentityError> {
    let affinity_sha256 = cpu_affinity_sha256(processors);
    let document = read_bounded_utf8(
        Path::new("/proc/self/cgroup"),
        MAX_CGROUP_DOCUMENT_BYTES,
        "resource_control",
    )?;
    let lines = document
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return Ok((
            ResourceControlV1::LinuxUncontrolled {
                cpu_affinity_sha256: affinity_sha256,
            },
            host_memory_bytes,
        ));
    }
    if lines.len() != 1 || !lines[0].starts_with("0::") {
        return Err(MachineIdentityError::probe(
            "resource_control",
            "cgroup-v1 or hybrid resource control is unsupported for durable benchmark identity",
        ));
    }
    let cgroup_path = lines[0].strip_prefix("0::").unwrap_or_default();
    let mountinfo = read_bounded_utf8(
        Path::new("/proc/self/mountinfo"),
        MAX_MOUNTINFO_BYTES,
        "resource_control",
    )?;
    let directories = cgroup_v2_hierarchy(cgroup_path, &mountinfo)?;
    let mut hierarchy_digest = Sha256::new();
    hierarchy_digest.update(CGROUP_HIERARCHY_DIGEST_DOMAIN);
    let mut effective_cpu: Option<(u64, u64)> = None;
    let mut effective_memory: Option<u64> = None;
    for (level, directory) in directories.into_iter().enumerate() {
        hash_control_field(
            &mut hierarchy_digest,
            b"hierarchy-level",
            &u64::try_from(level)
                .map_err(|_| {
                    MachineIdentityError::probe(
                        "resource_control",
                        "cgroup-v2 hierarchy depth does not fit u64",
                    )
                })?
                .to_be_bytes(),
        );
        let controls = cgroup_control_inventory(&directory)?;
        for (name, value) in &controls {
            hash_control_field(&mut hierarchy_digest, name.as_bytes(), value.as_bytes());
        }
        let cpu = controls.get("cpu.max");
        let memory = controls.get("memory.max");
        if let Some(candidate) = cpu.as_deref().map(parse_cpu_max).transpose()?.flatten()
            && effective_cpu.is_none_or(|current| cpu_quota_is_stricter(candidate, current))
        {
            effective_cpu = Some(candidate);
        }
        if let Some(candidate) = memory
            .as_deref()
            .map(parse_memory_max)
            .transpose()?
            .flatten()
        {
            effective_memory =
                Some(effective_memory.map_or(candidate, |limit| limit.min(candidate)));
        }
    }
    let effective_memory_bytes = effective_memory
        .map(|limit| limit.min(host_memory_bytes))
        .unwrap_or(host_memory_bytes);
    let (effective_cpu_quota_micros, effective_cpu_period_micros) = effective_cpu
        .map(|(quota, period)| (Some(quota), Some(period)))
        .unwrap_or((None, None));
    Ok((
        ResourceControlV1::LinuxCgroupV2 {
            cpu_affinity_sha256: affinity_sha256,
            hierarchy_sha256: format!("{:x}", hierarchy_digest.finalize()),
            effective_cpu_quota_micros,
            effective_cpu_period_micros,
            effective_memory_limit_bytes: effective_memory,
        },
        effective_memory_bytes,
    ))
}

#[cfg(target_os = "linux")]
fn cgroup_v2_hierarchy(
    cgroup_path: &str,
    mountinfo: &str,
) -> Result<Vec<PathBuf>, MachineIdentityError> {
    let membership = normalized_absolute_path(cgroup_path, "cgroup-v2 membership path")?;
    let mounts = parse_cgroup_v2_mounts(mountinfo)?;
    let mut candidates = Vec::new();
    for mount in mounts {
        let Some(leaf) = cgroup_membership_leaf(&membership, &mount) else {
            continue;
        };
        if leaf.is_dir() {
            candidates.push((mount.root.components().count(), mount.mount_point, leaf));
        }
    }
    candidates.sort_by(|left, right| right.0.cmp(&left.0));
    let Some((specificity, root, leaf)) = candidates.first().cloned() else {
        return Err(MachineIdentityError::probe(
            "resource_control",
            "no cgroup-v2 mount exposes the process membership path",
        ));
    };
    if candidates
        .get(1)
        .is_some_and(|candidate| candidate.0 == specificity && candidate.1 != root)
    {
        return Err(MachineIdentityError::probe(
            "resource_control",
            "cgroup-v2 mount resolution is ambiguous",
        ));
    }
    let mut directories = Vec::new();
    let mut cursor = leaf.as_path();
    loop {
        if directories.len() >= MAX_CGROUP_HIERARCHY_DEPTH {
            return Err(MachineIdentityError::probe(
                "resource_control",
                format!("cgroup-v2 hierarchy exceeds the {MAX_CGROUP_HIERARCHY_DEPTH}-level bound"),
            ));
        }
        directories.push(cursor.to_path_buf());
        if cursor == root {
            break;
        }
        cursor = cursor.parent().ok_or_else(|| {
            MachineIdentityError::probe(
                "resource_control",
                "cgroup-v2 hierarchy escaped its mount root",
            )
        })?;
        if !cursor.starts_with(&root) {
            return Err(MachineIdentityError::probe(
                "resource_control",
                "cgroup-v2 hierarchy escaped its mount root",
            ));
        }
    }
    directories.reverse();
    Ok(directories)
}

/// Resolve a namespace-relative cgroup membership through one actual cgroup2
/// mount. Both paths come from the same process's procfs view: mountinfo field
/// 4 is the filesystem root exposed at field 5, while `/proc/self/cgroup` is
/// relative to that process's cgroup namespace. A membership outside the
/// exposed root is therefore not reachable through this mount and must not be
/// rebased onto it heuristically.
#[cfg(any(target_os = "linux", test))]
fn cgroup_membership_leaf(membership: &Path, mount: &CgroupV2Mount) -> Option<PathBuf> {
    membership
        .strip_prefix(&mount.root)
        .ok()
        .map(|relative| mount.mount_point.join(relative))
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct CgroupV2Mount {
    root: PathBuf,
    mount_point: PathBuf,
}

#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_v2_mounts(document: &str) -> Result<Vec<CgroupV2Mount>, MachineIdentityError> {
    let mut mounts = Vec::new();
    for line in document.lines() {
        let Some((prefix, suffix)) = line.split_once(" - ") else {
            return Err(MachineIdentityError::probe(
                "resource_control",
                "mountinfo line has no filesystem separator",
            ));
        };
        let mut filesystem = suffix.split_whitespace();
        if filesystem.next() != Some("cgroup2") {
            continue;
        }
        let fields = prefix.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 6 {
            return Err(MachineIdentityError::probe(
                "resource_control",
                "cgroup-v2 mountinfo line has too few fields",
            ));
        }
        let root =
            normalized_absolute_path(&decode_mountinfo_path(fields[3])?, "cgroup-v2 mount root")?;
        let mount_point =
            normalized_absolute_path(&decode_mountinfo_path(fields[4])?, "cgroup-v2 mount point")?;
        mounts.push(CgroupV2Mount { root, mount_point });
        if mounts.len() > MAX_CGROUP_V2_MOUNTS {
            return Err(MachineIdentityError::probe(
                "resource_control",
                format!("mountinfo contains more than {MAX_CGROUP_V2_MOUNTS} cgroup-v2 mounts"),
            ));
        }
    }
    if mounts.is_empty() {
        return Err(MachineIdentityError::probe(
            "resource_control",
            "mountinfo contains no cgroup-v2 mount",
        ));
    }
    Ok(mounts)
}

#[cfg(any(target_os = "linux", test))]
fn normalized_absolute_path(
    value: &str,
    noun: &'static str,
) -> Result<PathBuf, MachineIdentityError> {
    let path = PathBuf::from(value);
    let mut components = path.components();
    if components.next() != Some(Component::RootDir)
        || components.any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(MachineIdentityError::probe(
            "resource_control",
            format!("{noun} is not a normalized absolute path"),
        ));
    }
    Ok(path)
}

#[cfg(any(target_os = "linux", test))]
fn decode_mountinfo_path(value: &str) -> Result<String, MachineIdentityError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        let escape = bytes.get(index + 1..index + 4).ok_or_else(|| {
            MachineIdentityError::probe("resource_control", "mountinfo path ends in an escape")
        })?;
        let byte = match escape {
            b"040" => b' ',
            b"011" => b'\t',
            b"012" => b'\n',
            b"134" => b'\\',
            _ => {
                return Err(MachineIdentityError::probe(
                    "resource_control",
                    "mountinfo path contains an unsupported escape",
                ));
            }
        };
        decoded.push(byte);
        index += 4;
    }
    String::from_utf8(decoded).map_err(|_| {
        MachineIdentityError::probe("resource_control", "mountinfo path is not valid UTF-8")
    })
}

#[cfg(any(target_os = "linux", test))]
fn cgroup_control_inventory(
    directory: &Path,
) -> Result<BTreeMap<String, String>, MachineIdentityError> {
    let mut controls = BTreeMap::new();
    let mut entries = 0usize;
    let mut retained_bytes = 0usize;
    for entry in fs::read_dir(directory).map_err(|error| {
        MachineIdentityError::probe(
            "resource_control",
            format!("could not list {}: {error}", directory.display()),
        )
    })? {
        let entry = entry.map_err(|error| {
            MachineIdentityError::probe(
                "resource_control",
                format!("could not inspect {}: {error}", directory.display()),
            )
        })?;
        entries = entries.checked_add(1).ok_or_else(|| {
            MachineIdentityError::probe("resource_control", "cgroup control count overflowed")
        })?;
        if entries > MAX_CGROUP_CONTROL_FILES {
            return Err(MachineIdentityError::probe(
                "resource_control",
                format!(
                    "{} contains more than {MAX_CGROUP_CONTROL_FILES} cgroup entries",
                    directory.display()
                ),
            ));
        }
        let file_type = entry.file_type().map_err(|error| {
            MachineIdentityError::probe(
                "resource_control",
                format!("could not inspect {}: {error}", entry.path().display()),
            )
        })?;
        if file_type.is_dir() {
            continue;
        }
        if file_type.is_symlink() || !file_type.is_file() {
            return Err(MachineIdentityError::probe(
                "resource_control",
                format!(
                    "cgroup entry {} is not a regular control file",
                    entry.path().display()
                ),
            ));
        }
        let name = entry.file_name().into_string().map_err(|_| {
            MachineIdentityError::probe(
                "resource_control",
                "cgroup control filename is not valid UTF-8",
            )
        })?;
        if name.len() > MAX_CGROUP_CONTROL_NAME_BYTES {
            return Err(MachineIdentityError::probe(
                "resource_control",
                format!(
                    "cgroup control filename exceeds the {MAX_CGROUP_CONTROL_NAME_BYTES}-byte bound"
                ),
            ));
        }
        if dynamic_or_command_cgroup_file(&name) {
            continue;
        }
        let value =
            read_optional_bounded_utf8(&entry.path(), MAX_CGROUP_VALUE_BYTES, "resource_control")?
                .ok_or_else(|| {
                    MachineIdentityError::probe(
                        "resource_control",
                        format!(
                            "cgroup control {} disappeared during capture",
                            entry.path().display()
                        ),
                    )
                })?;
        let value = canonical_control_value(&value)?;
        retained_bytes = retained_bytes
            .checked_add(name.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or_else(|| {
                MachineIdentityError::probe(
                    "resource_control",
                    "cgroup configuration inventory byte count overflowed",
                )
            })?;
        if retained_bytes > MAX_CGROUP_CONTROL_TOTAL_BYTES {
            return Err(MachineIdentityError::probe(
                "resource_control",
                format!(
                    "cgroup configuration inventory exceeds the {MAX_CGROUP_CONTROL_TOTAL_BYTES}-byte bound"
                ),
            ));
        }
        controls.insert(name, value);
    }
    Ok(controls)
}

#[cfg(any(target_os = "linux", test))]
fn dynamic_or_command_cgroup_file(name: &str) -> bool {
    matches!(
        name,
        "cgroup.controllers" | "cgroup.procs" | "cgroup.threads" | "cgroup.kill" | "memory.reclaim"
    ) || [
        ".current",
        ".peak",
        ".events",
        ".events.local",
        ".stat",
        ".stat.local",
        ".pressure",
        ".numa_stat",
    ]
    .iter()
    .any(|suffix| name.ends_with(suffix))
}

#[cfg(any(target_os = "linux", test))]
fn canonical_control_value(value: &str) -> Result<String, MachineIdentityError> {
    if value.len() > MAX_CGROUP_VALUE_BYTES {
        return Err(MachineIdentityError::probe(
            "resource_control",
            "cgroup control value exceeds its read bound",
        ));
    }
    Ok(value.split_whitespace().collect::<Vec<_>>().join(" "))
}

#[cfg(any(target_os = "linux", test))]
fn parse_cpu_max(value: &str) -> Result<Option<(u64, u64)>, MachineIdentityError> {
    let mut fields = value.split_whitespace();
    let quota = fields
        .next()
        .ok_or_else(|| MachineIdentityError::probe("resource_control", "cpu.max has no quota"))?;
    let period = fields
        .next()
        .ok_or_else(|| MachineIdentityError::probe("resource_control", "cpu.max has no period"))?
        .parse::<u64>()
        .map_err(|error| {
            MachineIdentityError::probe(
                "resource_control",
                format!("cpu.max period is invalid: {error}"),
            )
        })?;
    if period == 0 || fields.next().is_some() {
        return Err(MachineIdentityError::probe(
            "resource_control",
            "cpu.max must contain exactly a positive period and one quota or max",
        ));
    }
    if quota == "max" {
        return Ok(None);
    }
    let quota = quota.parse::<u64>().map_err(|error| {
        MachineIdentityError::probe(
            "resource_control",
            format!("cpu.max quota is invalid: {error}"),
        )
    })?;
    if quota == 0 {
        return Err(MachineIdentityError::probe(
            "resource_control",
            "cpu.max quota must be positive",
        ));
    }
    Ok(Some((quota, period)))
}

#[cfg(any(target_os = "linux", test))]
fn parse_memory_max(value: &str) -> Result<Option<u64>, MachineIdentityError> {
    if value == "max" {
        return Ok(None);
    }
    let limit = value.parse::<u64>().map_err(|error| {
        MachineIdentityError::probe(
            "resource_control",
            format!("memory.max is invalid: {error}"),
        )
    })?;
    if limit == 0 {
        return Err(MachineIdentityError::probe(
            "resource_control",
            "memory.max must be positive",
        ));
    }
    Ok(Some(limit))
}

#[cfg(any(target_os = "linux", test))]
fn cpu_quota_is_stricter(candidate: (u64, u64), current: (u64, u64)) -> bool {
    let candidate_ratio = u128::from(candidate.0) * u128::from(current.1);
    let current_ratio = u128::from(current.0) * u128::from(candidate.1);
    candidate_ratio < current_ratio || (candidate_ratio == current_ratio && candidate.1 < current.1)
}

#[cfg(any(target_os = "linux", test))]
fn cpu_affinity_sha256(processors: &[u32]) -> String {
    let mut digest = Sha256::new();
    digest.update(CPU_AFFINITY_DIGEST_DOMAIN);
    for processor in processors {
        digest.update(processor.to_be_bytes());
    }
    format!("{:x}", digest.finalize())
}

#[cfg(target_os = "linux")]
fn hash_control_field(digest: &mut Sha256, label: &[u8], value: &[u8]) {
    digest.update(u64::try_from(label.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(label);
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_cpu_list(value: &str) -> Result<Vec<u32>, MachineIdentityError> {
    let mut processors = BTreeSet::new();
    for part in value.trim().split(',') {
        if part.is_empty() {
            return Err(MachineIdentityError::probe(
                "logical_cores",
                "online CPU list contains an empty component",
            ));
        }
        let (start, end) = match part.split_once('-') {
            Some((start, end)) if !end.contains('-') => (
                parse_cpu_id(start, "logical_cores")?,
                parse_cpu_id(end, "logical_cores")?,
            ),
            Some(_) => {
                return Err(MachineIdentityError::probe(
                    "logical_cores",
                    "online CPU list contains an invalid range",
                ));
            }
            None => {
                let cpu = parse_cpu_id(part, "logical_cores")?;
                (cpu, cpu)
            }
        };
        if start > end {
            return Err(MachineIdentityError::probe(
                "logical_cores",
                "online CPU range is descending",
            ));
        }
        let span = usize::try_from(u64::from(end) - u64::from(start) + 1).map_err(|_| {
            MachineIdentityError::probe("logical_cores", "online CPU range is too large")
        })?;
        if processors.len().saturating_add(span) > MAX_LOGICAL_CPUS {
            return Err(MachineIdentityError::probe(
                "logical_cores",
                format!("online CPU list exceeds the {MAX_LOGICAL_CPUS}-CPU bound"),
            ));
        }
        for cpu in start..=end {
            if !processors.insert(cpu) {
                return Err(MachineIdentityError::probe(
                    "logical_cores",
                    "online CPU list contains a duplicate CPU",
                ));
            }
        }
    }
    if processors.is_empty() {
        return Err(MachineIdentityError::probe(
            "logical_cores",
            "online CPU list is empty",
        ));
    }
    Ok(processors.into_iter().collect())
}

#[cfg(any(target_os = "linux", test))]
fn parse_cpu_id(value: &str, field: &'static str) -> Result<u32, MachineIdentityError> {
    let value = value.trim();
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(MachineIdentityError::probe(
            field,
            "CPU identifier is not an unsigned decimal integer",
        ));
    }
    value
        .parse::<u32>()
        .map_err(|error| MachineIdentityError::probe(field, format!("invalid CPU id: {error}")))
}

#[cfg(target_os = "linux")]
fn read_linux_topology_id(path: &str, field: &'static str) -> Result<u32, MachineIdentityError> {
    let value = read_bounded_utf8(Path::new(path), MAX_TOPOLOGY_VALUE_BYTES, field)?;
    parse_cpu_id(&value, field)
}

#[cfg(any(target_os = "linux", test))]
fn linux_total_memory_bytes(meminfo: &str) -> Result<u64, MachineIdentityError> {
    let value = meminfo
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("MemTotal")
                .then_some(value)
        })
        .ok_or_else(|| {
            MachineIdentityError::probe("total_memory_bytes", "meminfo contains no MemTotal")
        })?;
    let mut fields = value.split_whitespace();
    let kibibytes = fields
        .next()
        .ok_or_else(|| {
            MachineIdentityError::probe("total_memory_bytes", "MemTotal has no numeric value")
        })?
        .parse::<u64>()
        .map_err(|error| {
            MachineIdentityError::probe(
                "total_memory_bytes",
                format!("MemTotal is invalid: {error}"),
            )
        })?;
    if fields.next() != Some("kB") || fields.next().is_some() {
        return Err(MachineIdentityError::probe(
            "total_memory_bytes",
            "MemTotal must contain exactly a value and kB unit",
        ));
    }
    kibibytes.checked_mul(1024).ok_or_else(|| {
        MachineIdentityError::probe("total_memory_bytes", "MemTotal overflows u64 bytes")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> MachineIdentityV1 {
        MachineIdentityV1 {
            format_version: MACHINE_IDENTITY_FORMAT_VERSION,
            os_name: "macos".to_string(),
            os_version: "26.5".to_string(),
            kernel_version: "25.5.0".to_string(),
            architecture: "aarch64".to_string(),
            cpu_model: "Apple M5 Pro".to_string(),
            logical_cores: 15,
            physical_cores: 15,
            total_memory_bytes: 25_769_803_776,
            resource_control: ResourceControlV1::MacosNative,
            scheduling: SchedulingIdentityV1 {
                nice_level: 0,
                policy: SchedulerPolicyV1::Other,
                priority: 31,
                reset_on_fork: false,
            },
            resource_limits: ResourceLimitIdentityV1 {
                scope_version: RESOURCE_LIMIT_SCOPE_VERSION,
                values_sha256: "a".repeat(64),
            },
            machine_label: machine_label_from_hostname("bench-host.example").unwrap(),
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn captures_a_complete_identity_on_supported_hosts() {
        let observed = capture_machine_identity().unwrap();
        observed.validate().unwrap();
        assert_eq!(observed.format_version, MACHINE_IDENTITY_FORMAT_VERSION);
        assert!(observed.machine_label.starts_with(HOSTNAME_LABEL_PREFIX));
        #[cfg(target_os = "linux")]
        assert_eq!(observed.os_name, "linux");
    }

    #[test]
    fn serialization_is_ordered_and_unknown_fields_are_refused() {
        let identity = identity();
        let json = serde_json::to_string(&identity).unwrap();
        assert_eq!(
            json,
            format!(
                "{{\"format_version\":1,\"os_name\":\"macos\",\"os_version\":\"26.5\",\"kernel_version\":\"25.5.0\",\"architecture\":\"aarch64\",\"cpu_model\":\"Apple M5 Pro\",\"logical_cores\":15,\"physical_cores\":15,\"total_memory_bytes\":25769803776,\"resource_control\":{{\"kind\":\"macos-native\"}},\"scheduling\":{{\"nice_level\":0,\"policy\":\"other\",\"priority\":31,\"reset_on_fork\":false}},\"resource_limits\":{{\"scope_version\":1,\"values_sha256\":\"{}\"}},\"machine_label\":\"{}\"}}",
                identity.resource_limits.values_sha256, identity.machine_label
            )
        );
        let with_unknown = json.replacen("{", "{\"unreviewed_machine_fact\":\"surprise\",", 1);
        assert!(serde_json::from_str::<MachineIdentityV1>(&with_unknown).is_err());
    }

    #[test]
    fn validation_refuses_unknown_noncanonical_and_impossible_facts() {
        let mut value = identity();
        value.os_version = "unknown".to_string();
        assert_eq!(value.validate().unwrap_err().field, "os_version");

        let mut value = identity();
        value.cpu_model = " Apple   M5 Pro ".to_string();
        assert_eq!(value.validate().unwrap_err().field, "cpu_model");

        let mut value = identity();
        value.physical_cores = value.logical_cores + 1;
        assert_eq!(value.validate().unwrap_err().field, "physical_cores");

        let mut value = identity();
        value.machine_label = "hostname-sha256:ABC".to_string();
        assert_eq!(value.validate().unwrap_err().field, "machine_label");

        let mut value = identity();
        value.resource_limits.scope_version += 1;
        assert_eq!(
            value.validate().unwrap_err().field,
            "resource_limits.scope_version"
        );

        let mut value = identity();
        value.scheduling.reset_on_fork = true;
        assert_eq!(
            value.validate().unwrap_err().field,
            "scheduling.reset_on_fork"
        );
    }

    #[test]
    fn validates_process_effective_linux_resource_controls() {
        let mut value = identity();
        value.os_name = "linux".to_string();
        value.scheduling.priority = 0;
        value.resource_control = ResourceControlV1::LinuxCgroupV2 {
            cpu_affinity_sha256: "a".repeat(64),
            hierarchy_sha256: "b".repeat(64),
            effective_cpu_quota_micros: Some(150_000),
            effective_cpu_period_micros: Some(100_000),
            effective_memory_limit_bytes: Some(value.total_memory_bytes),
        };
        value.validate().unwrap();

        value.total_memory_bytes += 1;
        assert_eq!(value.validate().unwrap_err().field, "total_memory_bytes");

        value.total_memory_bytes -= 1;
        if let ResourceControlV1::LinuxCgroupV2 {
            effective_cpu_period_micros,
            ..
        } = &mut value.resource_control
        {
            *effective_cpu_period_micros = None;
        }
        assert_eq!(
            value.validate().unwrap_err().field,
            "resource_control.cpu_quota"
        );

        value.os_name = "Linux".to_string();
        assert_eq!(value.validate().unwrap_err().field, "resource_control");
    }

    #[test]
    fn hostname_label_is_only_a_canonical_non_secret_correlation_hint() {
        let first = machine_label_from_hostname("Bench-Host.Example.").unwrap();
        let second = machine_label_from_hostname("bench-host.example").unwrap();
        assert_eq!(first, second);
        assert!(first.starts_with(HOSTNAME_LABEL_PREFIX));
        assert!(!first.contains("bench-host"));
        assert_eq!(first.len(), HOSTNAME_LABEL_PREFIX.len() + 64);
    }

    #[test]
    fn scheduling_and_resource_limits_change_machine_identity_bytes() {
        let baseline = serde_json::to_vec(&identity()).unwrap();

        let mut scheduling_changed = identity();
        scheduling_changed.scheduling.nice_level = 1;
        assert_ne!(serde_json::to_vec(&scheduling_changed).unwrap(), baseline);

        let mut limits_changed = identity();
        limits_changed.resource_limits.values_sha256 = "b".repeat(64);
        assert_ne!(serde_json::to_vec(&limits_changed).unwrap(), baseline);
    }

    #[test]
    fn resource_limit_digest_binds_names_order_values_and_infinity() {
        let finite = CanonicalResourceLimit {
            soft: CanonicalLimitValue::Finite(10),
            hard: CanonicalLimitValue::Finite(20),
        };
        let infinite = CanonicalResourceLimit {
            soft: CanonicalLimitValue::Finite(10),
            hard: CanonicalLimitValue::Infinity,
        };
        let baseline = resource_limit_values_sha256(&[(b"open-files", finite)]);
        assert_eq!(
            baseline,
            resource_limit_values_sha256(&[(b"open-files", finite)])
        );
        assert_ne!(
            baseline,
            resource_limit_values_sha256(&[(b"process-count", finite)])
        );
        assert_ne!(
            baseline,
            resource_limit_values_sha256(&[(b"open-files", infinite)])
        );
        assert_ne!(
            resource_limit_values_sha256(&[(b"a", finite), (b"b", infinite)]),
            resource_limit_values_sha256(&[(b"b", infinite), (b"a", finite)])
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn unsupported_macos_scheduler_policy_is_refused() {
        let error = decode_macos_scheduling(i32::MAX, 0, 0).unwrap_err();
        assert_eq!(error.field, "scheduling.policy");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unsupported_parameter_rich_linux_scheduler_policy_is_refused() {
        let error = decode_linux_scheduling(libc::SCHED_DEADLINE, 0, 0).unwrap_err();
        assert_eq!(error.field, "scheduling.policy");

        let error = decode_linux_scheduling(libc::SCHED_OTHER | libc::SCHED_RESET_ON_FORK, 0, 0)
            .unwrap_err();
        assert_eq!(error.field, "scheduling.reset_on_fork");
    }

    #[test]
    fn parses_os_release_without_executing_shell_content() {
        let document = "NAME=\"Example Linux\"\nVERSION_ID='24.04'\nBAD=$(secret)\n";
        assert_eq!(
            os_release_value(document, "NAME").unwrap().unwrap(),
            "Example Linux"
        );
        assert_eq!(
            os_release_value(document, "VERSION_ID").unwrap().unwrap(),
            "24.04"
        );
        assert!(os_release_value(document, "MISSING").is_none());
    }

    #[test]
    fn parses_sparse_linux_cpu_lists_with_hard_bounds() {
        assert_eq!(
            parse_linux_cpu_list("0-3,8,10-11\n").unwrap(),
            vec![0, 1, 2, 3, 8, 10, 11]
        );
        assert!(parse_linux_cpu_list("0-3,3").is_err());
        assert!(parse_linux_cpu_list("4-2").is_err());
        assert!(parse_linux_cpu_list("0-70000").is_err());
        assert_eq!(
            cpu_affinity_sha256(&[0, 1, 2]),
            cpu_affinity_sha256(&[0, 1, 2])
        );
        assert_ne!(
            cpu_affinity_sha256(&[0, 1, 2]),
            cpu_affinity_sha256(&[0, 1, 3])
        );
    }

    #[test]
    fn parses_cpu_model_and_memory_with_exact_units() {
        let cpuinfo = "processor : 0\nmodel name : Example  9000\n\nprocessor : 1\nmodel name : Example 9000\n";
        assert_eq!(linux_cpu_model(cpuinfo).unwrap(), "Example 9000");
        assert_eq!(
            linux_total_memory_bytes("MemTotal:       25165824 kB\n").unwrap(),
            25_769_803_776
        );
        assert!(linux_total_memory_bytes("MemTotal: 1 MB\n").is_err());
    }

    #[test]
    fn parses_and_orders_cgroup_v2_limits_exactly() {
        assert_eq!(parse_cpu_max("max 100000").unwrap(), None);
        assert_eq!(
            parse_cpu_max("150000 100000").unwrap(),
            Some((150_000, 100_000))
        );
        assert!(parse_cpu_max("0 100000").is_err());
        assert!(parse_cpu_max("100000 0").is_err());
        assert!(parse_cpu_max("100000 100000 extra").is_err());

        assert_eq!(parse_memory_max("max").unwrap(), None);
        assert_eq!(parse_memory_max("1048576").unwrap(), Some(1_048_576));
        assert!(parse_memory_max("0").is_err());

        assert!(cpu_quota_is_stricter((50_000, 100_000), (1, 1)));
        assert!(!cpu_quota_is_stricter((2, 1), (150_000, 100_000)));
        assert!(cpu_quota_is_stricter((1, 2), (2, 4)));
    }

    #[test]
    fn parses_cgroup_v2_mount_roots_and_escaped_mount_points() {
        let document = concat!(
            "29 23 0:26 / /proc rw,nosuid - proc proc rw\n",
            "36 25 0:32 /tenant.slice /sys/fs/cgroup\\040bench rw,nosuid - cgroup2 cgroup rw\n",
        );
        assert_eq!(
            parse_cgroup_v2_mounts(document).unwrap(),
            vec![CgroupV2Mount {
                root: PathBuf::from("/tenant.slice"),
                mount_point: PathBuf::from("/sys/fs/cgroup bench"),
            }]
        );
        assert!(normalized_absolute_path("relative", "test path").is_err());
        assert!(normalized_absolute_path("/escape/../other", "test path").is_err());
        assert!(decode_mountinfo_path(r"/bad\999").is_err());
    }

    #[test]
    fn cgroup_membership_resolves_only_through_the_exposed_mount_root() {
        let mount = CgroupV2Mount {
            root: PathBuf::from("/tenant.slice"),
            mount_point: PathBuf::from("/sys/fs/cgroup"),
        };
        assert_eq!(
            cgroup_membership_leaf(Path::new("/tenant.slice/job.scope"), &mount),
            Some(PathBuf::from("/sys/fs/cgroup/job.scope"))
        );
        assert_eq!(
            cgroup_membership_leaf(Path::new("/tenant.slice"), &mount),
            Some(PathBuf::from("/sys/fs/cgroup"))
        );
        assert_eq!(
            cgroup_membership_leaf(Path::new("/different.slice/job.scope"), &mount),
            None,
            "an unrelated membership must not be heuristically rebased onto the mount"
        );
    }

    #[test]
    fn cgroup_mount_inventory_is_bounded() {
        let document = (0..=MAX_CGROUP_V2_MOUNTS)
            .map(|index| {
                format!(
                    "{} 1 0:{} / /sys/fs/cgroup/{index} rw - cgroup2 cgroup rw",
                    index + 2,
                    index + 2
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(parse_cgroup_v2_mounts(&document).is_err());
    }

    #[test]
    fn cgroup_identity_ignores_observations_but_keeps_configuration() {
        for ignored in [
            "cgroup.controllers",
            "cgroup.procs",
            "cpu.stat",
            "memory.current",
            "io.pressure",
            "memory.events.local",
            "memory.reclaim",
        ] {
            assert!(dynamic_or_command_cgroup_file(ignored), "{ignored}");
        }
        for retained in [
            "cgroup.subtree_control",
            "cgroup.max.depth",
            "cgroup.freeze",
            "cpu.max",
            "cpu.max.burst",
            "cpu.weight",
            "cpu.uclamp.max",
            "cpuset.cpus.effective",
            "cpuset.mems.effective",
            "memory.high",
            "memory.swap.max",
            "io.max",
            "io.weight",
            "pids.max",
            "hugetlb.2MB.max",
            "rdma.max",
            "misc.max",
        ] {
            assert!(!dynamic_or_command_cgroup_file(retained), "{retained}");
        }
    }

    #[test]
    fn cgroup_configuration_inventory_reads_all_stable_settings_with_hard_bounds() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join("cpu.max"), "150000 100000\n").unwrap();
        fs::write(directory.path().join("cpuset.mems.effective"), "0-1\n").unwrap();
        fs::write(directory.path().join("memory.swap.max"), "1048576\n").unwrap();
        fs::write(
            directory.path().join("cgroup.controllers"),
            "cpu memory io\n",
        )
        .unwrap();
        fs::write(directory.path().join("memory.current"), "4096\n").unwrap();

        let inventory = cgroup_control_inventory(directory.path()).unwrap();
        assert_eq!(
            inventory.get("cpu.max").map(String::as_str),
            Some("150000 100000")
        );
        assert_eq!(
            inventory.get("cpuset.mems.effective").map(String::as_str),
            Some("0-1")
        );
        assert_eq!(
            inventory.get("memory.swap.max").map(String::as_str),
            Some("1048576")
        );
        assert!(!inventory.contains_key("cgroup.controllers"));
        assert!(!inventory.contains_key("memory.current"));

        fs::write(
            directory.path().join("future-controller.max"),
            vec![b'x'; MAX_CGROUP_VALUE_BYTES + 1],
        )
        .unwrap();
        assert!(cgroup_control_inventory(directory.path()).is_err());
    }
}
