- A `load csv` overwrite now writes its Lance fragments while the rows
  drain instead of after the whole delta is validated: each parse
  partition's rows fill a fragment of up to 1 Mi rows and a full fragment
  goes to a writer, `stage_write_concurrency` of them at once, as the next
  rows arrive. A validation with no check that reads the whole delta after
  the rows are in (value, uniqueness and cardinality checks and endpoint
  checks swept against integer keys stream; a retained-edge check or a
  removal scan does not) streams each batch on and the load plans as one
  pipeline from the source through the write; the explain's `pipelines`
  shows it, two pipelines otherwise. Its defects are swept as the rows pass
  and refused in the judgment after the drain, ranked across the parse
  partitions as before, and that refusal is the one the statement reports
  even when the sink's own keyed-write check met the same repeated key first.
  A refusal that reaches the sink after fragments were written deletes them,
  so a refused load leaves no fragment behind and the table version never
  moves. Nothing commits before the statement's one publish, as before. LDBC SF30 loads
  1.2× faster (104 s to 87 s; the write phase after the last row 45 s to
  4 s, the rest of its work now overlapping the parse).
