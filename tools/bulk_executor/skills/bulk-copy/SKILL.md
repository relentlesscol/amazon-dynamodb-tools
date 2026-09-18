---
name: bulk-copy
description: |
  Copy every item from one Amazon DynamoDB table to another existing table with
  Bulk Executor, including cross-region and cross-account when given full table
  ARNs. Use for cloning prod into a scratch table, seeding a new region,
  rehearsing a migration, or making a working copy before a risky change.
  Triggers on: "copy a dynamodb table", "clone my table", "duplicate a dynamodb
  table", "copy table to another region", "copy table across accounts", "seed a
  table from prod", "bulk copy", "make a scratch copy of my table".
license: Apache-2.0
compatibility: |
  Writes to the target table. Requires point-in-time recovery on the target, a
  bootstrapped account/region, and a READ-WRITE Glue role. Cross-account needs a
  resource-based policy on the remote table. Run from tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, copy, clone, cross-region, cross-account, migration
---

# `bulk copy`

```sh
cd tools/bulk_executor
./bulk copy --source tableOne --target tableTwo
```

| Parameter | Required | Meaning |
|---|---|---|
| `--source` | yes | Source table name or full ARN |
| `--target` | yes | Target table name or full ARN — **must already exist** |

Note the parameter names: `--source` / `--target`, not `--table` / `--table2`
(that is `diff`).

## The target must exist, and you create it

`copy` does not create the target table and does not replicate its schema, GSIs,
TTL config, or capacity settings. Create it first with a **compatible key
schema** — a copy into a table whose keys do not match the source's items fails
per-item.

Match the source's key schema when creating the target, and remember GSIs on the
target will be backfilled by DynamoDB as items land, consuming extra write
capacity beyond the copy itself.

## Cross-region and cross-account: use full ARNs

**Passing a full table ARN is what unlocks cross-region and cross-account
copying** — a bare name resolves in the run's own region.

```sh
./bulk copy \
  --source arn:aws:dynamodb:us-east-1:111122223333:table/orders \
  --target arn:aws:dynamodb:eu-west-1:111122223333:table/orders-eu
```

For cross-account you also need a **resource-based policy on the remote table**
allowing the Glue role access. Without it the run fails with
`AccessDeniedException`.

`--XRegion` decides where the Glue job runs and which table gets validated, but a
full ARN overrides it — which is correct, since a cross-region source and target
legitimately differ.

## Cross-account weakens the PITR interlock

PITR must be enabled on the target. But when a table lives in a **different
account, its PITR status is unreadable, so the check is skipped with a warning
rather than enforced.** On a cross-account copy the safety net you assume is
present may not be. Verify PITR in the owning account first.

## Capacity on both sides

A copy reads the source and writes the target, so it consumes capacity in both
places:

```sh
./bulk copy --source a --target b --XMaxReadRate 20000 --XMaxWriteRate 20000
```

Bound both if either table serves live traffic. Pre-warming the target's write
throughput avoids cold-start throttling on a large copy.

## Verify afterwards

`copy` is the natural setup for a rehearsal, and `diff` is how you prove it
worked:

```sh
./bulk diff --table tableOne --table2 tableTwo
```

## Related

`bulk-diff` to compare source and target. `bulk-update` — rehearse a risky
migration on a copy, then diff. `bulk-load-export` to load from a DynamoDB export
in S3 instead of reading a live table, which is cheaper for a full-table move.
`bulk-tuning` for rate and worker sizing.
