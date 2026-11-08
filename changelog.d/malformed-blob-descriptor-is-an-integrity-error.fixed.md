- A malformed external Blob descriptor with an offset but no length fails as
  a Blob integrity error instead of reading past the end of its object.
