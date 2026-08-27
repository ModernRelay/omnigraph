//! Host-side evidence for local benchmark environment declarations.
//!
//! Case validation proves that factor combinations are internally coherent;
//! execution must additionally prove that the scratch tree really resides on
//! the declared filesystem and storage class. This first runner slice supports
//! the checked-in APFS case on macOS and fails closed elsewhere.

use std::path::Path;
#[cfg(target_os = "macos")]
use std::{
    collections::VecDeque,
    ffi::{CString, OsStr},
    io::Read,
    mem::MaybeUninit,
    os::{fd::AsFd, unix::ffi::OsStrExt, unix::process::CommandExt},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, TryRecvError},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use serde::Serialize;

use crate::case::{LocalFilesystem, LocalStorageClass};

#[cfg(target_os = "macos")]
const MAX_PROBE_OUTPUT_BYTES: usize = 64 * 1024;
#[cfg(target_os = "macos")]
const PROBE_DEADLINE: Duration = Duration::from_secs(10);
#[cfg(target_os = "macos")]
const PROBE_REAP_DEADLINE: Duration = Duration::from_secs(2);
#[cfg(target_os = "macos")]
const PROBE_PIPE_STOP_DEADLINE: Duration = Duration::from_secs(1);
#[cfg(target_os = "macos")]
const PROBE_POLL: Duration = Duration::from_millis(5);

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
    let mount = observe_macos_mount(scratch_path)?;
    let disk = command_output(
        "/usr/sbin/diskutil",
        &[OsStr::new("info"), OsStr::new(&mount.mount_point)],
        PROBE_DEADLINE,
    )?;
    let filesystem = mount.filesystem.to_ascii_lowercase();
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
    verify_declarations(
        scratch_path,
        declared_filesystem,
        declared_storage,
        observed_filesystem,
        observed_storage,
        &protocol,
        solid_state,
        device_location,
    )?;

    Ok(LocalEnvironmentEvidence {
        filesystem,
        storage_class: storage_name(observed_storage).to_string(),
        mount_point: mount.mount_point,
        storage_protocol: protocol,
        available_bytes: mount.available_bytes,
        probe: "macos-statfs-diskutil-v1",
    })
}

#[cfg(target_os = "macos")]
#[derive(Debug)]
struct MountEvidence {
    filesystem: String,
    mount_point: String,
    available_bytes: u64,
}

#[cfg(target_os = "macos")]
fn observe_macos_mount(path: &Path) -> Result<MountEvidence, String> {
    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        format!(
            "benchmark scratch path contains a NUL byte: {}",
            path.display()
        )
    })?;
    let mut status = MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `path` is a live, NUL-terminated C string and `status` points to
    // writable storage large enough for one `statfs` result. We read it only
    // after macOS reports success.
    if unsafe { libc::statfs(path.as_ptr(), status.as_mut_ptr()) } != 0 {
        return Err(format!(
            "could not inspect benchmark scratch filesystem: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: a successful `statfs` call initialized the entire output value.
    let status = unsafe { status.assume_init() };
    mount_evidence_from_statfs_fields(
        &status.f_fstypename,
        &status.f_mntonname,
        status.f_bsize,
        status.f_bavail,
    )
}

#[cfg(target_os = "macos")]
fn mount_evidence_from_statfs_fields(
    filesystem: &[libc::c_char],
    mount_point: &[libc::c_char],
    block_size: u32,
    available_blocks: u64,
) -> Result<MountEvidence, String> {
    let filesystem = decode_statfs_field(filesystem, "filesystem type")?;
    let mount_point = decode_statfs_field(mount_point, "mount point")?;
    let available_bytes = u64::from(block_size)
        .checked_mul(available_blocks)
        .ok_or_else(|| "statfs available-byte count overflowed u64".to_string())?;
    Ok(MountEvidence {
        filesystem,
        mount_point,
        available_bytes,
    })
}

#[cfg(target_os = "macos")]
fn decode_statfs_field(field: &[libc::c_char], name: &str) -> Result<String, String> {
    let end = field
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| format!("statfs {name} field was not NUL-terminated"))?;
    if end == 0 {
        return Err(format!("statfs {name} field was empty"));
    }
    let bytes = field[..end]
        .iter()
        .map(|byte| *byte as u8)
        .collect::<Vec<_>>();
    String::from_utf8(bytes).map_err(|error| format!("statfs {name} field is not UTF-8: {error}"))
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
#[allow(clippy::too_many_arguments)]
fn verify_declarations(
    scratch_path: &Path,
    declared_filesystem: LocalFilesystem,
    declared_storage: LocalStorageClass,
    observed_filesystem: LocalFilesystem,
    observed_storage: LocalStorageClass,
    protocol: &str,
    solid_state: &str,
    device_location: &str,
) -> Result<(), String> {
    if observed_filesystem != declared_filesystem {
        return Err(format!(
            "declared filesystem {} does not match observed {} at {}",
            filesystem_name(declared_filesystem),
            filesystem_name(observed_filesystem),
            scratch_path.display()
        ));
    }
    if observed_storage != declared_storage {
        return Err(format!(
            "declared storage class {} does not match observed {} (protocol `{protocol}`, solid_state `{solid_state}`, location `{device_location}`) at {}",
            storage_name(declared_storage),
            storage_name(observed_storage),
            scratch_path.display()
        ));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
struct ProbeCapture {
    tail: Vec<u8>,
    truncated: bool,
    clean_eof: bool,
    read_error: Option<String>,
}

#[cfg(target_os = "macos")]
struct ProbeReader {
    name: &'static str,
    stop: Arc<AtomicBool>,
    result: Receiver<ProbeCapture>,
    thread: JoinHandle<()>,
}

#[cfg(target_os = "macos")]
fn command_output(
    program: &str,
    arguments: &[&OsStr],
    deadline: Duration,
) -> Result<String, String> {
    command_output_with_limit(program, arguments, deadline, MAX_PROBE_OUTPUT_BYTES)
}

#[cfg(target_os = "macos")]
fn command_output_with_limit(
    program: &str,
    arguments: &[&OsStr],
    deadline: Duration,
    output_limit: usize,
) -> Result<String, String> {
    if deadline.is_zero() {
        return Err(format!(
            "{program} probe deadline must be greater than zero"
        ));
    }
    if output_limit == 0 {
        return Err(format!(
            "{program} probe output limit must be greater than zero"
        ));
    }

    let started = Instant::now();
    let mut command = Command::new(program);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = command
        .spawn()
        .map_err(|error| format!("could not execute {program}: {error}"))?;
    let process_group = i32::try_from(child.id()).map_err(|_| {
        let _ = child.kill();
        let _ = child.wait();
        format!("{program} process identifier does not fit the process-group API")
    })?;

    let stdout = child.stdout.take().ok_or_else(|| {
        terminate_probe_after_setup_failure(&mut child, process_group);
        format!("{program} stdout was not piped")
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        terminate_probe_after_setup_failure(&mut child, process_group);
        format!("{program} stderr was not piped")
    })?;
    let stdout = spawn_probe_reader("stdout", stdout, output_limit).map_err(|error| {
        terminate_probe_after_setup_failure(&mut child, process_group);
        format!("could not capture {program} stdout: {error}")
    })?;
    let stderr = match spawn_probe_reader("stderr", stderr, output_limit) {
        Ok(reader) => reader,
        Err(error) => {
            terminate_probe_after_setup_failure(&mut child, process_group);
            let _ = finish_probe_reader(stdout, None, true);
            return Err(format!("could not capture {program} stderr: {error}"));
        }
    };

    let mut status = None;
    let mut stdout_capture = None;
    let mut stderr_capture = None;
    let mut monitor_error = None;
    let timed_out = loop {
        if status.is_none() {
            match child.try_wait() {
                Ok(observed) => status = observed,
                Err(error) => {
                    monitor_error = Some(format!("could not wait for {program}: {error}"));
                    break false;
                }
            }
        }
        poll_probe_reader(&stdout, &mut stdout_capture);
        poll_probe_reader(&stderr, &mut stderr_capture);
        if status.is_some() && stdout_capture.is_some() && stderr_capture.is_some() {
            break false;
        }
        if started.elapsed() >= deadline {
            break true;
        }
        std::thread::sleep(PROBE_POLL);
    };

    if timed_out || monitor_error.is_some() {
        let containment = terminate_probe(&mut child, process_group, status.as_ref());
        let stdout_capture = finish_probe_reader(stdout, stdout_capture, true);
        let stderr_capture = finish_probe_reader(stderr, stderr_capture, true);
        let captures = bounded_capture_diagnostic(stdout_capture.as_ref(), stderr_capture.as_ref());
        let cleanup = containment
            .err()
            .into_iter()
            .chain(stdout_capture.as_ref().err().cloned())
            .chain(stderr_capture.as_ref().err().cloned())
            .collect::<Vec<_>>()
            .join("; ");
        let reason = monitor_error.unwrap_or_else(|| {
            format!(
                "{program} timed out after {} milliseconds",
                deadline.as_millis()
            )
        });
        return Err(format_probe_failure(&reason, &captures, &cleanup));
    }

    let status = status.ok_or_else(|| format!("{program} exited without a wait status"))?;
    let stdout = finish_probe_reader(stdout, stdout_capture, false)?;
    let stderr = finish_probe_reader(stderr, stderr_capture, false)?;
    if !process_group_is_gone(process_group)? {
        let containment = terminate_probe(&mut child, process_group, Some(&status));
        return Err(match containment {
            Ok(()) => format!("{program} left a live descendant process after exiting"),
            Err(error) => format!(
                "{program} left a live descendant process after exiting; containment failed: {error}"
            ),
        });
    }
    validate_probe_capture(program, "stdout", &stdout, output_limit)?;
    validate_probe_capture(program, "stderr", &stderr, output_limit)?;
    if !status.success() {
        let captures = bounded_capture_diagnostic(Ok(&stdout), Ok(&stderr));
        return Err(format_probe_failure(
            &format!("{program} failed with {status}"),
            &captures,
            "",
        ));
    }
    String::from_utf8(stdout.tail)
        .map_err(|error| format!("{program} output is not valid UTF-8: {error}"))
}

#[cfg(target_os = "macos")]
fn spawn_probe_reader<R>(
    name: &'static str,
    mut pipe: R,
    output_limit: usize,
) -> std::io::Result<ProbeReader>
where
    R: Read + AsFd + Send + 'static,
{
    set_nonblocking(&pipe)?;
    let stop = Arc::new(AtomicBool::new(false));
    let reader_stop = stop.clone();
    let (send, result) = mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name(format!("omnigraph-bench-probe-{name}"))
        .spawn(move || {
            let capture = read_probe_pipe(&mut pipe, reader_stop.as_ref(), output_limit);
            let _ = send.send(capture);
        })?;
    Ok(ProbeReader {
        name,
        stop,
        result,
        thread,
    })
}

#[cfg(target_os = "macos")]
fn set_nonblocking<T: AsFd>(pipe: &T) -> std::io::Result<()> {
    use nix::fcntl::{FcntlArg, OFlag, fcntl};

    let flags = fcntl(pipe, FcntlArg::F_GETFL).map_err(std::io::Error::from)?;
    let flags = OFlag::from_bits_truncate(flags) | OFlag::O_NONBLOCK;
    fcntl(pipe, FcntlArg::F_SETFL(flags))
        .map(|_| ())
        .map_err(std::io::Error::from)
}

#[cfg(target_os = "macos")]
fn read_probe_pipe(reader: &mut impl Read, stop: &AtomicBool, output_limit: usize) -> ProbeCapture {
    let mut retained = VecDeque::with_capacity(output_limit);
    let mut truncated = false;
    let mut read_error = None;
    let clean_eof = loop {
        let mut buffer = [0_u8; 8192];
        match reader.read(&mut buffer) {
            Ok(0) => break true,
            Ok(read) => {
                retain_probe_tail(&mut retained, &buffer[..read], output_limit, &mut truncated)
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if stop.load(Ordering::Acquire) {
                    break false;
                }
                std::thread::sleep(PROBE_POLL);
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => {
                read_error = Some(error.to_string());
                break false;
            }
        }
    };
    ProbeCapture {
        tail: retained.into_iter().collect(),
        truncated,
        clean_eof,
        read_error,
    }
}

#[cfg(target_os = "macos")]
fn retain_probe_tail(
    retained: &mut VecDeque<u8>,
    bytes: &[u8],
    output_limit: usize,
    truncated: &mut bool,
) {
    for byte in bytes {
        if retained.len() == output_limit {
            retained.pop_front();
            *truncated = true;
        }
        retained.push_back(*byte);
    }
}

#[cfg(target_os = "macos")]
fn poll_probe_reader(reader: &ProbeReader, capture: &mut Option<ProbeCapture>) {
    if capture.is_some() {
        return;
    }
    match reader.result.try_recv() {
        Ok(observed) => *capture = Some(observed),
        Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
    }
}

#[cfg(target_os = "macos")]
fn finish_probe_reader(
    reader: ProbeReader,
    capture: Option<ProbeCapture>,
    stop: bool,
) -> Result<ProbeCapture, String> {
    if stop {
        reader.stop.store(true, Ordering::Release);
    }
    let capture = match capture {
        Some(capture) => capture,
        None => match reader.result.recv_timeout(PROBE_PIPE_STOP_DEADLINE) {
            Ok(capture) => capture,
            Err(RecvTimeoutError::Timeout) => {
                reader.stop.store(true, Ordering::Release);
                reader
                    .result
                    .recv_timeout(PROBE_PIPE_STOP_DEADLINE)
                    .map_err(|error| {
                        format!(
                            "{} capture did not stop within the bounded deadline: {error}",
                            reader.name
                        )
                    })?
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(format!(
                    "{} capture stopped without reporting a result",
                    reader.name
                ));
            }
        },
    };
    reader
        .thread
        .join()
        .map_err(|_| format!("{} capture thread panicked", reader.name))?;
    Ok(capture)
}

#[cfg(target_os = "macos")]
fn validate_probe_capture(
    program: &str,
    stream: &str,
    capture: &ProbeCapture,
    output_limit: usize,
) -> Result<(), String> {
    if let Some(error) = &capture.read_error {
        return Err(format!("could not read {program} {stream}: {error}"));
    }
    if !capture.clean_eof {
        return Err(format!("{program} {stream} did not reach clean EOF"));
    }
    if capture.truncated {
        return Err(format!(
            "{program} emitted more than {output_limit} bytes on {stream}"
        ));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn terminate_probe_after_setup_failure(child: &mut Child, process_group: i32) {
    let _ = terminate_probe(child, process_group, None);
}

#[cfg(target_os = "macos")]
fn terminate_probe(
    child: &mut Child,
    process_group: i32,
    known_status: Option<&ExitStatus>,
) -> Result<(), String> {
    let kill_error = kill_process_group(process_group).err();
    let reaped = if known_status.is_some() {
        true
    } else {
        reap_probe_child(child, PROBE_REAP_DEADLINE)?
    };
    let group_gone = wait_for_process_group_gone(process_group, PROBE_REAP_DEADLINE)?;
    if let Some(error) = kill_error {
        return Err(format!("could not kill probe process group: {error}"));
    }
    if !reaped {
        return Err("probe direct child was not reaped before the deadline".to_string());
    }
    if !group_gone {
        return Err("probe process group remained live after SIGKILL".to_string());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn reap_probe_child(child: &mut Child, deadline: Duration) -> Result<bool, String> {
    let started = Instant::now();
    loop {
        if child
            .try_wait()
            .map_err(|error| format!("could not reap probe direct child: {error}"))?
            .is_some()
        {
            return Ok(true);
        }
        if started.elapsed() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(PROBE_POLL);
    }
}

#[cfg(target_os = "macos")]
fn kill_process_group(process_group: i32) -> Result<(), String> {
    use nix::errno::Errno;
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    match kill(Pid::from_raw(-process_group), Signal::SIGKILL) {
        Ok(()) | Err(Errno::ESRCH) => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

#[cfg(target_os = "macos")]
fn process_group_is_gone(process_group: i32) -> Result<bool, String> {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    match kill(Pid::from_raw(-process_group), None) {
        Ok(()) => Ok(false),
        Err(Errno::ESRCH) => Ok(true),
        Err(error) => Err(error.to_string()),
    }
}

#[cfg(target_os = "macos")]
fn wait_for_process_group_gone(process_group: i32, deadline: Duration) -> Result<bool, String> {
    let started = Instant::now();
    loop {
        if process_group_is_gone(process_group)? {
            return Ok(true);
        }
        if started.elapsed() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(PROBE_POLL);
    }
}

#[cfg(target_os = "macos")]
fn bounded_capture_diagnostic(
    stdout: Result<&ProbeCapture, &String>,
    stderr: Result<&ProbeCapture, &String>,
) -> String {
    let stream = |name: &str, capture: Result<&ProbeCapture, &String>| match capture {
        Ok(capture) if capture.tail.is_empty() => String::new(),
        Ok(capture) => format!(
            "; {name}: {}{}",
            String::from_utf8_lossy(&capture.tail).trim(),
            if capture.truncated {
                " [truncated]"
            } else {
                ""
            }
        ),
        Err(error) => format!("; {name} capture error: {error}"),
    };
    format!("{}{}", stream("stdout", stdout), stream("stderr", stderr))
}

#[cfg(target_os = "macos")]
fn format_probe_failure(reason: &str, captures: &str, cleanup: &str) -> String {
    if cleanup.is_empty() {
        format!("{reason}{captures}")
    } else {
        format!("{reason}{captures}; cleanup: {cleanup}")
    }
}

#[cfg(target_os = "macos")]
fn probe_value<'a>(output: &'a str, field: &str) -> Result<&'a str, String> {
    let mut values = output.lines().filter_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.trim() == field).then_some(value.trim())
    });
    let value = values
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("diskutil output did not contain a non-empty `{field}` field"))?;
    if values.next().is_some() {
        return Err(format!(
            "diskutil output contained more than one `{field}` field"
        ));
    }
    Ok(value)
}

#[cfg(target_os = "macos")]
fn filesystem_name(filesystem: LocalFilesystem) -> &'static str {
    match filesystem {
        LocalFilesystem::Apfs => "apfs",
        LocalFilesystem::Ext4 => "ext4",
        LocalFilesystem::Xfs => "xfs",
    }
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

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use std::fs;

    use super::*;

    fn statfs_field(bytes: &[u8]) -> Vec<libc::c_char> {
        bytes.iter().map(|byte| *byte as libc::c_char).collect()
    }

    #[test]
    fn parses_exactly_one_nonempty_diskutil_field_without_accepting_prefixes() {
        let output = "   Protocol: Apple Fabric\n";
        assert_eq!(probe_value(output, "Protocol").unwrap(), "Apple Fabric");
        assert!(probe_value(output, "Proto").is_err());
        assert!(probe_value("Protocol:\n", "Protocol").is_err());
        assert!(probe_value("Protocol: NVMe\nProtocol: SATA\n", "Protocol").is_err());
    }

    #[test]
    fn statfs_preserves_mount_whitespace_and_computes_available_bytes() {
        let filesystem = statfs_field(b"apfs\0");
        let mount_point = statfs_field(b"/Volumes/Fast  Disk\0");
        let mount =
            mount_evidence_from_statfs_fields(&filesystem, &mount_point, 4096, 750).unwrap();
        assert_eq!(mount.filesystem, "apfs");
        assert_eq!(mount.mount_point, "/Volumes/Fast  Disk");
        assert_eq!(mount.available_bytes, 750 * 4096);
    }

    #[test]
    fn native_statfs_observes_an_existing_scratch_path() {
        let directory = tempfile::tempdir().unwrap();
        let mount = observe_macos_mount(directory.path()).unwrap();
        assert!(!mount.filesystem.is_empty());
        assert!(Path::new(&mount.mount_point).is_absolute());
    }

    #[test]
    fn statfs_fields_fail_closed_when_malformed_or_overflowing() {
        assert!(decode_statfs_field(&statfs_field(b"apfs"), "filesystem type").is_err());
        assert!(decode_statfs_field(&statfs_field(b"\0"), "filesystem type").is_err());
        assert!(decode_statfs_field(&statfs_field(&[0xff, 0]), "filesystem type").is_err());

        let filesystem = statfs_field(b"apfs\0");
        let mount_point = statfs_field(b"/\0");
        let error =
            mount_evidence_from_statfs_fields(&filesystem, &mount_point, u32::MAX, u64::MAX)
                .unwrap_err();
        assert!(error.contains("overflowed"), "{error}");
    }

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

    #[test]
    fn declaration_mismatches_use_case_vocabulary() {
        let filesystem = verify_declarations(
            Path::new("/scratch"),
            LocalFilesystem::Ext4,
            LocalStorageClass::NvmeSsd,
            LocalFilesystem::Apfs,
            LocalStorageClass::NvmeSsd,
            "Apple Fabric",
            "Yes",
            "Internal",
        )
        .unwrap_err();
        assert_eq!(
            filesystem,
            "declared filesystem ext4 does not match observed apfs at /scratch"
        );

        let storage = verify_declarations(
            Path::new("/scratch"),
            LocalFilesystem::Apfs,
            LocalStorageClass::NetworkBlock,
            LocalFilesystem::Apfs,
            LocalStorageClass::NvmeSsd,
            "Apple Fabric",
            "Yes",
            "Internal",
        )
        .unwrap_err();
        assert_eq!(
            storage,
            "declared storage class network-block does not match observed nvme-ssd (protocol `Apple Fabric`, solid_state `Yes`, location `Internal`) at /scratch"
        );
    }

    #[test]
    fn timed_probe_kills_descendants_and_returns_within_its_deadline_envelope() {
        let directory = tempfile::tempdir().unwrap();
        let process_group_path = directory.path().join("process-group");
        let script = OsStr::new("sleep 300 & printf '%s' \"$$\" > \"$1\"; exit 0");
        let arguments = [
            OsStr::new("-c"),
            script,
            OsStr::new("probe"),
            process_group_path.as_os_str(),
        ];
        let started = Instant::now();
        let error =
            command_output_with_limit("/bin/sh", &arguments, Duration::from_millis(200), 1024)
                .unwrap_err();
        assert!(error.contains("timed out"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(5));

        let process_group = fs::read_to_string(process_group_path)
            .unwrap()
            .parse::<i32>()
            .unwrap();
        assert!(process_group_is_gone(process_group).unwrap());
    }

    #[test]
    fn probe_capture_drains_concurrently_and_caps_both_streams() {
        let successful_arguments = [OsStr::new("-c"), OsStr::new("printf ok")];
        assert_eq!(
            command_output_with_limit(
                "/bin/sh",
                &successful_arguments,
                Duration::from_secs(2),
                1024,
            )
            .unwrap(),
            "ok"
        );

        for (stream, script) in [
            ("stdout", "head -c 8192 /dev/zero"),
            ("stderr", "head -c 8192 /dev/zero >&2; printf ok"),
        ] {
            let arguments = [OsStr::new("-c"), OsStr::new(script)];
            let error =
                command_output_with_limit("/bin/sh", &arguments, Duration::from_secs(2), 1024)
                    .unwrap_err();
            assert!(
                error.contains(&format!("more than 1024 bytes on {stream}")),
                "{error}"
            );
            assert!(error.len() < 2048, "diagnostic was not bounded");
        }
    }
}
