- A `load csv` overwrite now plans from the object's metadata and each of its
  parse partitions reads its own byte range in 8 MiB pieces, pinned to the
  object's version, instead of one capture of the whole source followed by a
  serial pass that cut it into slices: no whole-object buffer, no serial
  resolve pass, and the memory held for the source is one piece per
  partition. An overwrite whose source's first record is longer than the head
  range the header is read from (8 MiB) is refused. The receipt's source
  `slices` gain `read_ns`, the wait for the slice's bytes. The uniqueness
  check's first holders are one set of maps shared by every partition and
  sized once from the first slice's rows per byte, so nothing merges after
  the last batch. LDBC SF30 loads 1.4× faster (155 s to 110 s; the Comment
  file 2.5×).
