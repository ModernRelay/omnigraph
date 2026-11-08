- Projected parameters retain their declared numeric width and list element type,
  including nulls and empty results. Numeric parameter bounds are checked even
  when a declaration is unused. Mixed integer/float list literals consistently
  have floating-point elements regardless of their order. Saved bound plans now
  carry expression leaf types and use format version 4.
