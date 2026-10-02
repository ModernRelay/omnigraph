- Export refuses a ranged external Blob descriptor. Export writes an external
  Blob as a bare URI, which reloads as the whole object, so a descriptor
  naming a byte range, which only a writer outside OmniGraph can create, is
  refused instead of silently widened. Change-feed images, the change-feed
  baseline and entity reads by id instead describe it exactly as
  `{"uri", "offset", "length"}` without reading the object, so the feed still
  passes the commit that holds it and a baseline still succeeds; such a
  baseline does not reload with `load`.
