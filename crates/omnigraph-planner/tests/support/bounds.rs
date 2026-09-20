use super::Bounds;

pub const BOUNDS: Bounds = Bounds {
    hydration_chunk_hard_bytes: 64 * 1024 * 1024,
    key_width_bytes: 48,
    ordered_scan_memory_bytes: 150 * 1024 * 1024,
    ordered_scan_max_input_batch_bytes: 150 * 1024 * 1024 / 4,
    build_key_cap_rows: 1 << 22,
    late_materialization_only: false,
};
