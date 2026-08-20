//! Machine-specification auto-capture (RFC 0039 Environment class: "machine
//! specification: processor, cores, memory, storage class (auto-captured)").
//!
//! Rule 4 makes the machine spec part of every number's identity, so capture
//! must be automatic, never typed by hand. Fields that cannot be determined on
//! this platform are recorded as an honest `"unknown"` / absent rather than
//! guessed: an unknown identity still blocks a silent comparison, a guessed
//! one manufactures a false match.

use std::process::Command;

use crate::record::MachineSpec;

/// Run a command and return trimmed stdout on success. `sysctl`/`diskutil`
/// live in `/usr/sbin`, which is not always on a build tool's PATH, so a bare
/// name is retried under `/usr/sbin` and `/usr/bin` before giving up.
fn cmd_out(program: &str, args: &[&str]) -> Option<String> {
    let candidates = [
        program.to_string(),
        format!("/usr/sbin/{program}"),
        format!("/usr/bin/{program}"),
    ];
    for candidate in &candidates {
        let Ok(out) = Command::new(candidate).args(args).output() else {
            continue;
        };
        if !out.status.success() {
            continue;
        }
        let Ok(text) = String::from_utf8(out.stdout) else {
            continue;
        };
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    None
}

fn sysctl(name: &str) -> Option<String> {
    cmd_out("sysctl", &["-n", name])
}

/// First `/proc/cpuinfo`-style value for `key` ("key<tab>: value").
#[cfg(target_os = "linux")]
fn proc_value(path: &str, key: &str) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    text.lines()
        .find(|line| line.starts_with(key))
        .and_then(|line| line.split(':').nth(1))
        .map(|v| v.trim().to_string())
}

fn capture_macos() -> MachineSpec {
    // `diskutil info /` reports "Solid State: Yes|No" for the boot volume.
    let storage_class = cmd_out("diskutil", &["info", "/"])
        .and_then(|text| {
            text.lines()
                .find(|line| line.contains("Solid State"))
                .map(|line| line.contains("Yes"))
        })
        .map(|ssd| if ssd { "ssd" } else { "rotational" })
        .unwrap_or("unknown")
        .to_string();
    MachineSpec {
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        os_version: cmd_out("sw_vers", &["-productVersion"]).unwrap_or_else(|| "unknown".into()),
        cpu_model: sysctl("machdep.cpu.brand_string").unwrap_or_else(|| "unknown".into()),
        physical_cores: sysctl("hw.physicalcpu").and_then(|v| v.parse().ok()),
        // In-process fallback: sandboxed environments deny the sysctl
        // syscall; available_parallelism needs no privilege.
        logical_cores: sysctl("hw.logicalcpu")
            .and_then(|v| v.parse().ok())
            .or_else(|| {
                std::thread::available_parallelism()
                    .ok()
                    .map(|n| n.get() as u64)
            }),
        memory_bytes: sysctl("hw.memsize").and_then(|v| v.parse().ok()),
        storage_class,
    }
}

#[cfg(target_os = "linux")]
fn capture_linux() -> MachineSpec {
    // MemTotal is reported in kB.
    let memory_bytes = proc_value("/proc/meminfo", "MemTotal")
        .and_then(|v| {
            v.split_whitespace()
                .next()
                .and_then(|kb| kb.parse::<u64>().ok())
        })
        .map(|kb| kb * 1024);
    MachineSpec {
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        os_version: cmd_out("uname", &["-r"]).unwrap_or_else(|| "unknown".into()),
        cpu_model: proc_value("/proc/cpuinfo", "model name").unwrap_or_else(|| "unknown".into()),
        physical_cores: None,
        logical_cores: std::thread::available_parallelism()
            .ok()
            .map(|n| n.get() as u64),
        memory_bytes,
        storage_class: "unknown".to_string(),
    }
}

fn capture_fallback() -> MachineSpec {
    MachineSpec {
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        os_version: "unknown".to_string(),
        cpu_model: "unknown".to_string(),
        physical_cores: None,
        logical_cores: std::thread::available_parallelism()
            .ok()
            .map(|n| n.get() as u64),
        memory_bytes: None,
        storage_class: "unknown".to_string(),
    }
}

/// Auto-capture this machine's specification.
pub fn capture() -> MachineSpec {
    match std::env::consts::OS {
        "macos" => capture_macos(),
        #[cfg(target_os = "linux")]
        "linux" => capture_linux(),
        _ => capture_fallback(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_names_the_platform() {
        let spec = capture();
        assert_eq!(spec.os, std::env::consts::OS);
        assert_eq!(spec.arch, std::env::consts::ARCH);
        // Fields are honest: either a real value or the explicit unknown.
        assert!(!spec.cpu_model.is_empty());
        assert!(!spec.storage_class.is_empty());
    }
}
