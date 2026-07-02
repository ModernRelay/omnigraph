# Branch-Based Review Workflow

Branches let you stage changes off `main`, inspect them in isolation, and merge
only once they look right — Git-style, atomic across the whole graph. This guide
walks a typical "review an incoming batch before it hits main" flow.

See [branches & commits](../branching/index.md) and [merging](../branching/merge.md)
for the underlying model.

## 1. Stage the batch on its own branch

Loading into a branch that does not exist is an error unless you pass `--from`,
which forks it from a base first. So one command both forks the branch and loads
into it:

```bash
omnigraph load --data batch.jsonl --mode merge \
  --branch review/2026-04-25 --from main graph.omni
```

(Equivalently, create the branch first with
`omnigraph branch create review/2026-04-25 --from main graph.omni`, then `load`
without `--from`.)

`main` is untouched — the batch lives only on `review/2026-04-25`.

## 2. Inspect the branch in isolation

Run any read query against the branch with `--branch`:

```bash
omnigraph query --query checks.gq count_by_type \
  --branch review/2026-04-25 --format table --store graph.omni
```

Compare it against `main` — list each branch's commits, or diff them:

```bash
omnigraph branch list graph.omni
omnigraph commit list --branch review/2026-04-25 graph.omni
```

## 3. Merge when it looks right

```bash
omnigraph branch merge review/2026-04-25 --into main graph.omni
```

The merge is three-way and atomic. If both `main` and the branch changed the same
data incompatibly, the merge fails with a structured list of conflicts and
publishes nothing — resolve them and re-merge. See
[merging](../branching/merge.md) for the conflict kinds.

## 4. Clean up

Once merged, delete the review branch:

```bash
omnigraph branch delete review/2026-04-25 graph.omni
```

Branch storage is reclaimed; if a transient error interrupts reclamation, the
[`cleanup`](../operations/maintenance.md) command sweeps the leftovers later.
