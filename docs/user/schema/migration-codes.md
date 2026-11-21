# Schema migration codes

Migration rejections may include a stable `OG-...` code. Match automation on the
code rather than the message text.

| Code | Meaning |
|---|---|
| `OG-DS-102` | Drop a node type that contains entities. |
| `OG-DS-103` | Drop an edge type that contains entities. |
| `OG-DS-104` | Drop a populated property. |
| `OG-MF-103` | Add a required property to a populated type. |
| `OG-MF-104` | Make a property required while a stored row holds a null (reported by `schema apply`). |
| `OG-MF-106` | Change a property's type, including enum narrowing or renaming. |
| `OG-MF-107` | Add an edge type with a non-zero `@card` minimum from an existing node type. |
| `OG-MF-108` | Add `@unique` to a node type while two stored rows share a key (reported by `schema apply`). |

`omnigraph schema plan` includes these codes in human and JSON output, except
`OG-MF-104` and `OG-MF-108`, which only `schema apply` can know from the stored
rows.
