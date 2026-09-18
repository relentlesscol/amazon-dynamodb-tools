---
name: bulk-delete
description: |
  Delete items in bulk from a large Amazon DynamoDB table with Bulk Executor —
  matching a Spark SQL predicate, or the N oldest/newest by any attribute.
  Requires point-in-time recovery on the table. Use for TTL-style cleanups,
  purging a date range, or trimming a table to size.
  Triggers on: "delete items from dynamodb", "bulk delete", "purge old
  records", "delete where", "delete items before date", "delete the oldest N
  items", "clean up my table", "mass delete dynamodb".
license: Apache-2.0
compatibility: |
  DESTRUCTIVE. Requires point-in-time recovery enabled on the table, a
  bootstrapped account/region, and a READ-WRITE Glue role. Runs a real Glue job
  and consumes write capacity. Run from tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, delete, purge, cleanup, destructive
---

# `bulk delete`

**This permanently deletes items. There is no dry-run flag.** Treat every
invocation as irreversible-in-practice and confirm the predicate with the user
before running it.

```sh
cd tools/bulk_executor
./bulk delete --table t --where "timestamp < '2024-01-01'"
./bulk delete --table t --orderby timestamp --limit 100        # 100 oldest
./bulk delete --table t --orderby timestamp desc --limit 100   # 100 newest
```

| Parameter | Required | Meaning |
|---|---|---|
| `--table` | yes | Table name, or full ARN |
| `--where` | no | Match criteria in **Spark SQL** syntax |
| `--orderby` | no | Sort attribute, optional `asc` / `desc` |
| `--limit` | no | Cap the number of items processed |

## Confirm the blast radius first — always

`delete` shares its predicate surface with `find`. **So run the identical
predicate through `bulk-find` or `bulk-count` first**, and show the user what
would be deleted:

```sh
./bulk count --table t --where "timestamp < '2024-01-01'"   # how many
./bulk find  --table t --where "timestamp < '2024-01-01'" --limit 20   # which
```

This is the closest thing to a dry run and it costs one extra read pass. Do it
unless the user has explicitly said not to. **With no `--where` and no `--limit`,
`delete` targets every item in the table** — never issue that form without
explicit, unambiguous confirmation.

## PITR is mandatory, and it is your undo

The command refuses to run unless point-in-time recovery is enabled:

> `For safety, point in time recovery (PITR) must be enabled for table '<t>'
> before performing bulk mutations against it`

That is not red tape — PITR is the only recovery path if the predicate was
wrong. Verify it is genuinely on before deleting:

```sh
aws dynamodb describe-continuous-backups --table-name t \
  --query 'ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus'
```

**Cross-account caution:** when the table is in a different account, PITR status
is unreadable, so the check is **skipped with a warning rather than enforced**.
The interlock you are relying on is not engaged. Confirm PITR manually in the
owning account first.

## Predicates are Spark SQL

`--where` is [Spark SQL](https://spark.apache.org/docs/latest/api/sql/index.html),
not a DynamoDB filter expression. Quoting and type coercion follow Spark rules —
which is exactly why you validate with `count` before deleting. A predicate that
silently matches more than intended (a string/number comparison, a NULL
surprise) is the main failure mode.

## Capacity and tuning

Deletes consume write capacity. Bound it so you do not throttle production
traffic:

```sh
./bulk delete --table t --where "..." --XMaxWriteRate 20000
```

A large `--orderby` is a sort on one worker — use `--XWorkerType R.1X` if it runs
out of memory. See `bulk-tuning`. A `ProvisionedThroughputExceededException`
means give the table more capacity or lower `--XMaxWriteRate`.

## Related

`bulk-find` / `bulk-count` to preview. `bulk-update` to modify rather than
remove. `bulk-revert-export` to undo an incremental export's writes. If the goal
is expiring items by timestamp on an ongoing basis, native DynamoDB TTL is
cheaper than a recurring bulk delete — say so.
