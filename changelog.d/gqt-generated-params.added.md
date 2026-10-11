- GQT steps can generate their parameters: `--- params generate: v1 seed:
  <u64>` holds a YAML `params:` map from each parameter to one column
  generator, evaluated at ordinal zero, in place of a JSON body. A new `blob`
  column (`{kind: blob, byte: <0-255>, length: <bytes>}`) produces the
  `base64:` input of a managed Blob, in generated parameters and generated
  loads alike. A parameter recipe names at most 256 parameters within a
  64 MiB JSON bound, takes no `${` substitution, and is generated when its
  step runs, so a case can pass a 32 MiB Blob without carrying its text.
