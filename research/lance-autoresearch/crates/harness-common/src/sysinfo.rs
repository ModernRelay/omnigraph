//! Peak resident-set-size readback (Linux only; non-Linux returns 0).
//!
//! Reads `VmHWM` from `/proc/self/status` — the high-water-mark of resident
//! memory pages, not the high-water-mark of virtual address space. `VmPeak`
//! (virtual peak) would include mmap'd files, guard pages, and untouched
//! allocations; `VmHWM` is what users mean by "peak memory".

#[cfg(target_os = "linux")]
pub fn peak_rss_mb() -> f64 {
    let Ok(s) = std::fs::read_to_string("/proc/self/status") else {
        return 0.0;
    };
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            let kb: f64 = rest
                .split_whitespace()
                .next()
                .and_then(|t| t.parse().ok())
                .unwrap_or(0.0);
            return kb / 1024.0;
        }
    }
    0.0
}

#[cfg(not(target_os = "linux"))]
pub fn peak_rss_mb() -> f64 {
    0.0
}
