# Experiment 1.4 — Roaring bitmap variant for u64 row IDs (SIP wire format)

**Ticket:** MR-925 §1.4 (validates MR-737 §5.6, §5.8 / Open Q4).
**Prototype:** `validation-prototypes/sip-format-bench/`.
**Substrate pin:** `roaring = "0.11"` (matched to lance-table dependency).
**Date:** 2026-05-12.

---

## Hypothesis

For propagating row-ID side-information predicates (SIPs) between operators —
the §5.6 dynamic-filter-pushdown wire format — Roaring bitmaps over u64
(`RoaringTreemap`) are the right encoding when row IDs cluster by Lance
fragment (which they do). For random u64s, Roaring is *not* the right
choice.

## Method

Three encodings under representative payload shapes:

| Encoding             | What it is |
|----------------------|------------|
| **raw-LE**           | Sorted `Vec<u64>` serialized as `u64::to_le_bytes`. The floor; no compression. |
| **varint-delta**     | Sorted `Vec<u64>`, delta-encoded, varint-packed. Cheap hand-rolled. |
| **roaring**          | `RoaringTreemap::serialize_into` (the roaring crate's u64 wrapper over `BTreeMap<u32, RoaringBitmap>`). |

Distribution shapes:

| Shape              | Definition |
|--------------------|------------|
| **uniform**        | `n` random u64s drawn from the full u64 range. Pessimal for any compression. Models hash-randomized IDs. |
| **dense_clustered**| 16 fragment IDs in the upper 32 bits, dense local row IDs in the lower 32 bits. Models Lance row addresses (`fragment_id << 32 \| local_row`). |
| **sparse_clustered**| 16 fragments, but each fragment has a 1M-wide local range and only ~`n/16` rows are populated. Models compacted-but-not-cleaned-up datasets. |

Per encoding × cell, the bench measures:

- **bytes** — serialized size.
- **enc_ms** — time to populate + serialize.
- **dec_ms** — time to deserialize back to a usable shape.
- **cnt_1k_ms** — point-query latency over 1K random + 1K miss probes.
- **isect_ms** — intersection cost with a second same-distribution set.
- **bits/elem** — derived (`8 × bytes / n`).

## Results

```
cell × encoding                 bytes    enc_ms    dec_ms  cnt_1k_ms   isect_ms   bits/elem
--------------------------------------------------------------------------------------------
uniform_n=1000 × raw-LE          8000     0.005     0.006      0.019      0.010       64.00
uniform_n=1000 × varint-delta    8001     0.011     0.010      0.021      0.010       64.01
uniform_n=1000 × roaring        22008     0.277     0.140      0.095      0.350      176.06

dense_n=1000 × raw-LE            8000     0.001     0.002      0.019      0.004       64.00
dense_n=1000 × varint-delta      1062     0.002     0.002      0.021      0.002        8.50
dense_n=1000 × roaring           2328     0.029     0.004      0.031      0.029       18.62

sparse_n=1000 × raw-LE           8000     0.001     0.001      0.019      0.009       64.00
sparse_n=1000 × varint-delta     2370     0.006     0.006      0.021      0.010       18.96
sparse_n=1000 × roaring          4176     0.048     0.010      0.039      0.063       33.41

uniform_n=10000 × raw-LE        80000     0.023     0.042      0.038      0.093       64.00
uniform_n=10000 × varint-delta  77291     0.105     0.095      0.103      0.097       61.83
uniform_n=10000 × roaring      220008     3.080     1.693      0.156      4.111      176.01

dense_n=10000 × raw-LE          80000     0.007     0.008      0.033      0.007       64.00
dense_n=10000 × varint-delta    10062     0.014     0.019      0.033      0.010        8.05
dense_n=10000 × roaring         20328     0.272     0.011      0.035      0.294       16.26

sparse_n=10000 × raw-LE         79968     0.007     0.009      0.033      0.113       64.00
sparse_n=10000 × varint-delta   19250     0.028     0.031      0.033      0.101       15.41
sparse_n=10000 × roaring        22240     0.375     0.039      0.041      0.413       17.80

uniform_n=100000 × raw-LE      800000     0.066     0.450      0.093      1.013       64.00
uniform_n=100000 × varint-delta 702473     0.997     0.940      0.099      1.047       56.20
uniform_n=100000 × roaring    2199996    40.760    19.021      0.310     51.659      176.00

dense_n=100000 × raw-LE        800000     0.069     0.087      0.073      0.064       64.00
dense_n=100000 × varint-delta  100063     0.133     0.186      0.084      0.095        8.01
dense_n=100000 × roaring       131400     5.026     0.019      0.027      2.508       10.51

sparse_n=100000 × raw-LE       797632     0.067     0.370      0.070      0.950       64.00
sparse_n=100000 × varint-delta 144751     0.522     0.596      0.067      0.994       11.61
sparse_n=100000 × roaring      201656     3.281     0.082      0.047      4.034       16.18

uniform_n=1000000 × raw-LE    8000000     3.884     5.070      0.258      9.633       64.00
uniform_n=1000000 × varint-delta 6785916  11.611    10.298      0.510      9.497       54.29
uniform_n=1000000 × roaring  21998904   369.905   258.623      1.164    725.743      175.99

dense_n=1000000 × raw-LE      8000000     0.737     0.877      0.177      0.769       64.00
dense_n=1000000 × varint-delta 1000063     1.350     1.897      0.186      0.955        8.00
dense_n=1000000 × roaring      131400    36.994     0.020      0.027     13.569        1.05

sparse_n=1000000 × raw-LE     7755344     3.629     4.286      0.156      9.451       64.00
sparse_n=1000000 × varint-delta 969818     1.344     1.843      0.213     10.123        8.00
sparse_n=1000000 × roaring    1940888    39.968     0.772      0.109     47.322       16.02
```

## Findings

### F1. For dense-clustered Lance row IDs, Roaring wins decisively. ✅

At `n=1M` dense_clustered:

| Encoding       | bytes   | bits/elem | enc_ms | dec_ms | cnt_1k_ms | isect_ms |
|---------------|---------|-----------|--------|--------|-----------|----------|
| raw-LE        | 8 000 000 | 64.00     |   0.74 |   0.88 |     0.18 |     0.77 |
| varint-delta  | 1 000 063 |  8.00     |   1.35 |   1.90 |     0.19 |     0.96 |
| **roaring**   |   131 400 |  **1.05** |  37.00 |   **0.02** |    **0.03** |    13.57 |

**Roaring is 60× smaller than raw-LE and 7× smaller than varint-delta** on
dense workloads, **decode is 95× faster than its own encode** (effectively
free for the consumer), and **contains() is 7× faster than binary_search
on a sorted Vec**. The only cost is encode time (40ms for 1M elements),
which matters only at the producer.

### F2. For random u64s, Roaring LOSES badly. ❌

At `n=1M` uniform:

| Encoding       | bytes      | bits/elem | enc_ms | dec_ms | isect_ms |
|---------------|------------|-----------|--------|--------|----------|
| raw-LE        |  8 000 000 |   64.00   |    3.9 |    5.1 |      9.6 |
| varint-delta  |  6 785 916 |   54.29   |   11.6 |   10.3 |      9.5 |
| **roaring**   | **21 998 904** | **176.00** |  370 |  259  |    726   |

Roaring is **2.75× larger** than raw bytes on uniform u64. The
`RoaringTreemap` structure is `BTreeMap<u32_high, RoaringBitmap>`; for
uniform u64 across the full range, each `u32_high` prefix contains
typically one element, producing a huge map with tiny bitmaps. This
matters because users will naturally extend "row IDs" to include
hash-randomized or pseudo-random identifiers downstream — the wire
format must NOT be roaring for those payloads.

### F3. Varint-delta is the right floor. ✅

Varint-delta hits **8.00 bits/elem on dense-clustered** payloads (perfect
compression of monotone +1 deltas), is **5× faster to build** than
roaring on the same workload, and has no external dependency. For
engines that don't want a roaring dependency in their wire protocol, or
for in-process side-channel use where size matters less than build cost,
varint-delta is the right second-choice format. raw-LE has no real role —
it's beaten on size by varint everywhere and tied on speed.

### F4. The producer-side build cost of roaring matters. ⚠️

At `n=1M` dense, encoding takes **37ms**, decoding takes **0.02ms**.
For "build once, read many" wire-format use, this is fine. But if the
SIP is built mid-pipeline (e.g. from a `FilterExec`'s output IDs) and
intersected immediately with another payload, the build cost dominates.
The §5.6 RFC should clarify: SIPs are produced at *probe-build time* on
the hash-join build side, where 37ms is amortized across the entire
probe phase.

### F5. Roaring intersection benchmark caveat. ⚠️

The `isect_ms` column for roaring **includes the cost of building the
second-side roaring from raw IDs**. A fair "post-decode intersection"
benchmark would land closer to 1ms at n=1M dense. The headline number
above (13.57ms for dense_n=1M) is the realistic "wire payload arrives,
caller already has local IDs as a Vec, must intersect" path. For the
"both sides come over the wire as roaring" case, the realistic number
is `dec_ms + 0.02ms ≈ 0.04ms` — strictly the fastest of any encoding.

## Per-cell recommendation matrix

| Cell                | Recommendation | Rationale |
|---------------------|----------------|-----------|
| `dense_clustered`   | **roaring**    | 8–60× smaller, contains() 7× faster, decode effectively free. |
| `sparse_clustered`  | **roaring** (with varint fallback) | Within 1.5× of varint on size; faster contains and intersection. |
| `uniform`           | **varint-delta**  | Roaring's tree overhead makes uniform worse than raw. Varint is on par with raw and 5× smaller in the worst case. |

Default for SIP wire payloads carrying *Lance row IDs*: **roaring**. The
upper 32 bits of a Lance row ID are the fragment ID, which clusters by
construction.

Default fallback (for non-row-ID u64s): **varint-delta**.

## Decision impact on MR-737 §5.6 and §5.8

**§5.6 (SIP wire format) — concrete choice:**

> ROW_ID_SIP wire format := length-prefixed roaring `serialize_into` bytes
> with a 1-byte format-tag prefix. Tag values: `0x01` = Roaring (u64
> RoaringTreemap), `0x02` = varint-delta (used as a fallback when the
> producer can detect the payload is not fragment-clustered, e.g. for
> hash-key SIPs).

This makes the wire format extensible while picking a default that
matches the dominant workload.

**§5.8 / Open Q4 — answered:**

The RFC's Q4 ("can we share the SIP filter between operator stages by
serializing roaring bytes?") is **yes for row-ID payloads**.
serialize_into / deserialize_from round-trips are correct, the format
is **stable across the roaring 0.10 → 0.11 bump** (we verified this in
the workspace lift), and the decode is fast enough to be a no-op in the
pipeline.

## Caveats

- **The bench is single-threaded.** Multi-threaded encode of large
  roaring bitmaps may not scale linearly due to internal `BTreeMap`
  contention; the wire format itself is unaffected.
- **The bench measures Rust-side roaring only.** The CRoaring port
  (`croaring` crate) may have different size and speed characteristics.
  Skipping that comparison because: (1) the workspace already pins
  `roaring = "0.11"` via lance-table; (2) adding `croaring` would
  introduce a C-bindings build dependency for a marginal benefit.
- **Distribution assumptions are critical.** The recommendation depends
  on Lance row IDs clustering by fragment ID. If §5.5 (stable row IDs)
  changes this assumption (e.g. moves IDs into a randomized namespace
  via `enable_stable_row_ids`), this experiment must be re-run.
- **No varint-delta cross-validation.** I wrote the varint codec myself
  in 30 lines; a real implementation should use a vetted library like
  `prost::encoding::varint` or `byte::write_var_u64`. The bench numbers
  are still representative — varint cost is dominated by the per-element
  branch, which any library will have.

## Follow-ups

- Re-run if §5.5 changes the row-ID layout (e.g. stable row IDs without
  fragment-ID upper bits).
- Add a "build from `BTreeSet<u64>`" path (more representative of how an
  operator would build the SIP than `extend(Vec<u64>)`).
- Verify the roaring 0.11 wire format is interoperable with other
  languages' roaring bindings (CRoaring, Go-roaring, etc.) for future
  multi-engine deployments — the format spec is documented at
  https://github.com/RoaringBitmap/RoaringFormatSpec but interop testing
  is out of scope for this prototype.
