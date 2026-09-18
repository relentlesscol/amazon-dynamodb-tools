---
name: bulk-count
description: |
  Count items in a large Amazon DynamoDB table, optionally only those matching
  a Spark SQL predicate, using Bulk Executor's Glue job. Use for "how many
  items match X" over a whole table. For an unfiltered total, prefer
  bulk-scancount or DescribeTable; for the matching items themselves, use
  bulk-find.
  Triggers on: "count items in my dynamodb table", "how many rows in table",
  "count where", "count matching items", "bulk count", "count items older
  than", "how many records match".
license: Apache-2.0
compatibility: |
  Requires a bootstrapped account/region and AWS credentials. Runs a real Glue
  job (~1 min startup) and consumes DynamoDB read capacity. Run from
  tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, count, spark-sql, query
---

# `bulk count`

Counts items via the Glue DynamoDB connector, which reads items into Spark. It
shares its implementation with `find` — same predicate and ordering surface,
but it returns a number instead of the items.

```sh
cd tools/bulk_executor
./bulk count --table t
./bulk count --table t --where "age > 21"
```

| Parameter | Required | Meaning |
|---|---|---|
| `--table` | yes | Table name, or a full ARN for cross-region / cross-account |
| `--where` | no | Match criteria in **Spark SQL** syntax |

## Reach for a cheaper answer first

**An unfiltered count of a whole table is already available for free and exact
from `DescribeTable`.** Do not run a Glue job for it:

```sh
aws dynamodb describe-table --table-name t --query 'Table.ItemCount'
```

That figure is not live — Bulk Executor's own docs note table metadata refreshes
about every 4 hours — so it is stale rather than wrong. If
the user needs an exact live unfiltered count, `bulk-scancount` is usually
faster than `count` because it scans directly instead of loading items into
Spark.

`count` earns its cost when there is a **`--where` predicate** that DynamoDB
itself cannot evaluate.

## Predicates are Spark SQL, not DynamoDB

This is the most common mistake. `--where` is
[Spark SQL](https://spark.apache.org/docs/latest/api/sql/index.html), so you get
real expressiveness — including comparing two attributes to each other, which
DynamoDB cannot do:

```sh
./bulk count --table t --where "a > b and ts < '2024' and val IN ('x','y')"
```

If you instead want a **pushed-down DynamoDB `FilterExpression`** — evaluated
server-side, cheaper on transfer — that is `bulk-scancount --filter-expression`,
a different syntax entirely.

## Cost and time

Roughly a minute of Glue startup on top of the work, so a small-table count
still takes about 2 minutes. The run prints table metrics and a DynamoDB cost
estimate before starting, and Ctrl-C cancels. On a small table the **Glue** cost
dominates and the default is 220 workers — pass `--XNumberOfWorkers 10`. See
`bulk-tuning`.

## Related

`bulk-find` returns the items. `bulk-scancount` counts by parallel scan with a
DynamoDB filter expression and per-segment skew reporting. `bulk-sql` runs
arbitrary aggregate queries such as `SELECT COUNT(*) ... GROUP BY`.
