---
name: bulk-diff
description: |
  Compare two Amazon DynamoDB tables item by item with Bulk Executor and report
  what differs — added, removed, and changed items — including cross-region and
  cross-account via full ARNs. Use to verify a restore, validate a migration or
  copy, prove a backfill did what was intended, or sample-check two tables
  cheaply.
  Triggers on: "compare two dynamodb tables", "diff my tables", "did the restore
  work", "verify the migration", "what changed between these tables", "are these
  two tables the same", "bulk diff", "validate my backfill".
license: Apache-2.0
compatibility: |
  Read-only on both tables. Requires a bootstrapped account/region. Reads both
  tables and consumes read capacity on each. Cross-account needs a
  resource-based policy on the remote table. Run from tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, diff, compare, verification, restore, migration
---

# `bulk diff`

Compares two tables using segmented scans internally.

```sh
cd tools/bulk_executor
./bulk diff --table tableBeforeRestore --table2 tableAfterRestore
```

| Parameter | Required | Meaning |
|---|---|---|
| `--table` | yes | First table — name or full ARN |
| `--table2` | yes | Second table — name or full ARN |
| `--format` | no | `keys` (default) or `full` |
| `--sample-fraction` | no | Compare a fraction of both tables; `1.0` is a full diff |

Note the parameter names: `--table` / `--table2`, not `--source` / `--target`
(that is `copy`).

## Reading the output

**`--format keys` (default)** lists the primary keys of items that differ, marked:

| Marker | Meaning |
|---|---|
| `+` | added — present in table2, not in table |
| `-` | removed — present in table, not in table2 |
| `*` | changed — present in both, contents differ |

**`--format full`** emits the whole items instead, with `+` / `-` for the before
and after states. Use `keys` to find out *what* differs and `full` to see *how*.

**The full diff always goes to S3; only the first few differences print to the
console.** On a table with many differences the console output is a preview — say
where the rest is rather than implying the diff was small.

## Sampling for a cheap check

A full diff reads both tables completely. `--sample-fraction 0.1` compares 10% of
both and is often enough to answer "did this go badly wrong":

```sh
./bulk diff --table a --table2 b --sample-fraction 0.1
```

Understand what that buys: a sample proves *presence* of differences well and
*absence* only weakly. **A clean 10% sample is not proof the tables match.** If
the user needs certainty — signing off a migration, closing an incident — run the
full diff and say why.

## Cross-region and cross-account

Full ARNs unlock both, and let the two tables live in different places:

```sh
./bulk diff \
  --table  arn:aws:dynamodb:us-east-1:111122223333:table/orders \
  --table2 arn:aws:dynamodb:eu-west-1:111122223333:table/orders-eu
```

Cross-account additionally needs a **resource-based policy on the remote table**.

## Read capacity applies per table

`--XMaxReadRate` is applied **per table**, not split between them — so
`--XMaxReadRate 20000` means up to 20,000 RCU/s on each side, 40,000 total.
Budget accordingly when either table serves live traffic.

## What it is good for

- **Verifying a restore.** Diff the original against the restored table.
- **Validating a `copy`** before decommissioning the source.
- **Proving a `bulk-update` did what was intended** — copy to scratch, update the
  scratch table, diff against the original, and the `*` entries are exactly your
  intended change set. This is the closest thing the tool offers to a dry run.

## Related

`bulk-copy` to create the second table. `bulk-update` — the rehearse-and-verify
loop above. `bulk-tuning` for rate limits and worker sizing.
