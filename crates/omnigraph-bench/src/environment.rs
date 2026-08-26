//! Host-side evidence for local benchmark environment declarations.
//!
//! Case validation proves that factor combinations are internally coherent;
//! execution must additionally prove that the scratch tree really resides on
//! the declared filesystem and storage class. This first runner slice supports
//! the checked-in APFS case on macOS and fails closed elsewhere.

use std::path::Path;
#[cfg(target_os = "macos")]
use std::process::Command;

use serde::Serialize;

use crate::case::{LocalFilesystem, LocalStorageClass};

#[cfg(target_os = "macos")]
const MAX_PROBE_OUTPUT_BYTES: usize = 64 * 1024;

/// Facts observed from the mounted volume that owns the runner scratch tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LocalEnvironmentEvidence {
    pub filesystem: String,
    pub storage_class: String,
    pub mount_point: String,
    pub storage_protocol: String,
    pub available_bytes: u64,
    pub probe: &'static str,
}

/// Verify the local environment declared by a case against `scratch_path`.
pub fn verify_local_environment(
    scratch_path: &Path,
    declared_filesystem: LocalFilesystem,
    declared_storage: LocalStorageClass,
) -> Result<LocalEnvironmentEvidence, String> {
    #[cfg(target_os = "macos")]
    {
        verify_macos(scratch_path, declared_filesystem, declared_storage)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (scratch_path, declared_filesystem, declared_storage);
        Err(
            "local environment verification is implemented only for macOS/APFS in runner-v1; refusing to trust unproved filesystem and storage-class declarations"
                .to_string(),
        )
    }
}

#[cfg(target_os = "macos")]
fn verify_macos(
    scratch_path: &Path,
    declared_filesystem: LocalFilesystem,
    declared_storage: LocalStorageClass,
) -> Result<LocalEnvironmentEvidence, String> {
    let path = scratch_path
        .to_str()
        .ok_or_else(|| "benchmark scratch path is not valid UTF-8".to_string())?;
    let df = command_output("/bin/df", &["-Pk", path])?;
    let mount = parse_df_kilobytes(&df)?;
    let disk = command_output("/usr/sbin/diskutil", &["info", &mount.mount_point])?;
    let filesystem = probe_value(&disk, "Type (Bundle)")?.to_ascii_lowercase();
    let protocol = probe_value(&disk, "Protocol")?.to_string();
    let solid_state = probe_value(&disk, "Solid State")?;
    let device_location = probe_value(&disk, "Device Location")?;

    let observed_filesystem = match filesystem.as_str() {
        "apfs" => LocalFilesystem::Apfs,
        other => {
            return Err(format!(
                "runner-v1 cannot classify macOS filesystem `{other}` for {}",
                scratch_path.display()
            ));
        }
    };
    let observed_storage = classify_macos_storage(&protocol, solid_state, device_location)?;

    if observed_filesystem != declared_filesystem {
        return Err(format!(
            "declared filesystem {declared_filesystem:?} does not match observed {observed_filesystem:?} at {}",
            scratch_path.display()
        ));
    }
    if observed_storage != declared_storage {
        return Err(format!(
            "declared storage class {declared_storage:?} does not match observed {observed_storage:?} (protocol `{protocol}`, solid_state `{solid_state}`, location `{device_location}`) at {}",
            scratch_path.display()
        ));
    }

    Ok(LocalEnvironmentEvidence {
        filesystem,
        storage_class: storage_name(observed_storage).to_string(),
        mount_point: mount.mount_point,
        storage_protocol: protocol,
        available_bytes: mount.available_bytes,
        probe: "macos-df-diskutil-v1",
    })
}

#[cfg(target_os = "macos")]
struct MountEvidence {
    mount_point: String,
    available_bytes: u64,
}

#[cfg(target_os = "macos")]
fn parse_df_kilobytes(output: &str) -> Result<MountEvidence, String> {
    let line = output
        .lines()
        .rfind(|line| !line.trim().is_empty())
        .ok_or_else(|| "df output did not contain a mounted-volume row".to_string())?;
    let fields = line.split_whitespace().collect::<Vec<_>>();
    if fields.len() < 6 {
        return Err(format!(
            "df mounted-volume row has {} fields, expected at least 6",
            fields.len()
        ));
    }
    let available_kib = fields[3]
        .parse::<u64>()
        .map_err(|error| format!("df available-kilobyte field is invalid: {error}"))?;
    let available_bytes = available_kib
        .checked_mul(1024)
        .ok_or_else(|| "df available-byte count overflowed u64".to_string())?;
    Ok(MountEvidence {
        mount_point: fields[5..].join(" "),
        available_bytes,
    })
}

#[cfg(target_os = "macos")]
fn classify_macos_storage(
    protocol: &str,
    solid_state: &str,
    device_location: &str,
) -> Result<LocalStorageClass, String> {
    if !solid_state.eq_ignore_ascii_case("yes") {
        return Err(format!(
            "runner-v1 supports only solid-state local benchmark volumes; diskutil reported `{solid_state}`"
        ));
    }
    if !device_location.eq_ignore_ascii_case("internal") {
        return Err(format!(
            "runner-v1 cannot prove a local storage class for diskutil location `{device_location}`"
        ));
    }
    if protocol.eq_ignore_ascii_case("apple fabric")
        || protocol.eq_ignore_ascii_case("pci-express")
        || protocol.eq_ignore_ascii_case("nvme")
    {
        Ok(LocalStorageClass::NvmeSsd)
    } else if protocol.eq_ignore_ascii_case("sata") {
        Ok(LocalStorageClass::SataSsd)
    } else {
        Err(format!(
            "runner-v1 cannot map diskutil protocol `{protocol}` to a declared storage class"
        ))
    }
}

#[cfg(target_os = "macos")]
fn command_output(program: &str, arguments: &[&str]) -> Result<String, String> {
    let output = Command::new(program)
        .args(arguments)
        .output()
        .map_err(|error| format!("could not execute {program}: {error}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "{program} failed with {}: {}",
            output.status,
            stderr.trim()
        ));
    }
    if output.stdout.len() > MAX_PROBE_OUTPUT_BYTES {
        return Err(format!(
            "{program} emitted more than {MAX_PROBE_OUTPUT_BYTES} bytes"
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|error| format!("{program} output is not valid UTF-8: {error}"))
}

#[cfg(target_os = "macos")]
fn probe_value<'a>(output: &'a str, field: &str) -> Result<&'a str, String> {
    output
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            (name.trim() == field).then_some(value.trim())
        })
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("diskutil output did not contain a non-empty `{field}` field"))
}

#[cfg(target_os = "macos")]
fn storage_name(storage: LocalStorageClass) -> &'static str {
    match storage {
        LocalStorageClass::NvmeSsd => "nvme-ssd",
        LocalStorageClass::SataSsd => "sata-ssd",
        LocalStorageClass::NetworkBlock => "network-block",
        LocalStorageClass::RamDisk => "ram-disk",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "macos")]
    #[test]
    fn parses_diskutil_fields_without_accepting_prefixes() {
        let output = "   Type (Bundle): apfs\n   Protocol: Apple Fabric\n";
        assert_eq!(probe_value(output, "Type (Bundle)").unwrap(), "apfs");
        assert_eq!(probe_value(output, "Protocol").unwrap(), "Apple Fabric");
        assert!(probe_value(output, "Type").is_err());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parses_df_capacity_and_mounts_with_spaces() {
        let output = "Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/disk1 1000 250 750 25% /Volumes/Fast Disk\n";
        let mount = parse_df_kilobytes(output).unwrap();
        assert_eq!(mount.mount_point, "/Volumes/Fast Disk");
        assert_eq!(mount.available_bytes, 750 * 1024);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn classifies_only_proved_internal_ssd_protocols() {
        assert_eq!(
            classify_macos_storage("Apple Fabric", "Yes", "Internal").unwrap(),
            LocalStorageClass::NvmeSsd
        );
        assert_eq!(
            classify_macos_storage("SATA", "Yes", "Internal").unwrap(),
            LocalStorageClass::SataSsd
        );
        assert!(classify_macos_storage("USB", "Yes", "External").is_err());
        assert!(classify_macos_storage("SATA", "No", "Internal").is_err());
    }
}
