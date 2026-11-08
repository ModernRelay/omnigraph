- A `load csv` overwrite now parses its source in up to
  `stage_write_concurrency` streams at once (default 8) instead of one: the
  source is cut on record ends into 8 MiB slices, each stream claims the next
  slice as it finishes one, and the validation puts the slices back in source
  order before judging them, so the rows, their order and a refusal's record
  are those of a single parser. A source under 8 MiB still parses in one
  stream. The explain's `pipelines` entries carry `partitions`, the receipt's
  `executed.operators` carry `partitions` and, on the source, one `slices`
  entry per slice (its rows, the stream that claimed it and its timings), and
  a GQT `load pipeline` line may claim `partitions <n>`.
