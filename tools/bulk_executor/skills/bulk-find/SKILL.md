---
name: bulk-find
description: |
  Find and print items from a large Amazon DynamoDB table using Bulk Executor —
  filtered with a Spark SQL predicate, optionally sorted by any attribute with
  no GSI required, and limited to the top N. Writes the full result set to S3
  and prints the first few to the console.
  Triggers on: "find items in my dynamodb table", "show me items where",
  "oldest items", "newest items", "top N items by", "query without a GSI",
  "sort my dynamodb table by", "bulk find", "export matching items".
license: Apache-2.0
compatibility: |
  Requires a bootstrapped account/region and AWS credentials. Runs a real Glue
  job (~1 min startup) and consumes DynamoDB read capacity. Large result sets
  land in S3. Run from tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, query, spark-sql, sort, orderby
---

# `bulk find`

Reads via the Glue DynamoDB connector and returns matching items.

```sh
cd tools/bulk_executor
./bulk find --table t
./bulk find --table t --where "status = 'pending'"
./bulk find --table t --orderby timestamp --limit 100          # 100 oldest
./bulk find --table t --orderby timestamp desc --limit 100     # 100 newest
```

| Parameter | Required | Meaning |
|---|---|---|
| `--table` | yes | Table name, or a full ARN for cross-region / cross-account |
| `--where` | no | Match criteria in **Spark SQL** syntax |
| `--orderby` | no | Sort attribute, optional `asc` / `desc` suffix (default asc) |
| `--limit` | no | Cap the number of items processed |

## The headline capability

**`--orderby` sorts on any attribute with no GSI required.** That is the thing
`find` does that DynamoDB cannot do natively, and it is usually why someone
reaches for this tool: "the 100 oldest records" needs no schema change, no index
backfill, no extra write capacity.

## Where the output goes

**Only the first ~10 items print to the console. The full result set is written
to S3**, in the bootstrap bucket. On a broad query the console output is a
preview, not the answer — tell the user where the rest is rather than letting
them assume the query returned 10 rows.

## `--orderby` is the memory risk

A sort has to gather matching items on a single worker, so a large sorted result
is the classic `OutOfMemoryError`. The fix is a bigger *worker type*, not more
workers — **`--XWorkerType R.1X` has double the heap of the default `G.1X`**:

```sh
./bulk find --table t --orderby timestamp --limit 10000000 --XWorkerType R.1X
```

Pair `--orderby` with `--limit` and the narrowest `--where` you can. See
`bulk-tuning`.

## Predicates are Spark SQL

`--where` is [Spark SQL](https://spark.apache.org/docs/latest/api/sql/index.html),
not DynamoDB filter syntax, so it can compare two attributes to each other:

```sh
./bulk find --table t --where "a > b and ts < '2024' and val IN ('x','y')"
```

For a pushed-down DynamoDB `FilterExpression` instead, see `bulk-scancount`.

## Cost and time

About a minute of Glue startup on top of the work. The run prints table metrics
and a DynamoDB cost estimate first and Ctrl-C cancels. Lower
`--XNumberOfWorkers` on small tables — the default is 220 and Glue cost
dominates small jobs.

## Related

`bulk-count` returns just the number. `bulk-sql` runs arbitrary Spark SQL
including aggregates and grouping. `bulk-scancount` is the faster path for
counting.
