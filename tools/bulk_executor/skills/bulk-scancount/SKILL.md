---
name: bulk-scancount
description: |
  Count items in a large Amazon DynamoDB table by parallel segmented scan —
  usually faster than bulk-count because it never loads items into Spark. Takes
  a pushed-down DynamoDB FilterExpression, can count against a secondary index,
  reports per-segment counts to expose hot partitions and key skew, and can
  statistically estimate a filtered count from a sample with a 95% confidence
  interval.
  Triggers on: "count items fast", "scancount", "count with a filter
  expression", "count items in a GSI", "sparse index count", "hot partition",
  "key skew", "uneven key distribution", "estimate how many items match",
  "sample count", "count without scanning the whole table".
license: Apache-2.0
compatibility: |
  Requires a bootstrapped account/region and AWS credentials. Runs a real Glue
  job (~1 min startup) and consumes DynamoDB read capacity. Run from
  tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, count, scan, filter-expression, skew, sampling
---

# `bulk scancount`

Counts using a parallel scan rather than the Glue DynamoDB connector, so items
are never loaded into Spark memory. **This is usually the faster way to count.**

```sh
cd tools/bulk_executor
./bulk scancount --table t
```

| Parameter | Required | Meaning |
|---|---|---|
| `--table` | yes | Table name |
| `--index` | no | Count against a secondary index instead of the table |
| `--filter-expression` | no | Pushed-down DynamoDB `FilterExpression` |
| `--expression-names` | no | JSON map of expression names used above |
| `--expression-values` | no | JSON map of expression values used above |
| `--per-segment` | no | Print each segment's count |
| `--segments` | no | Parallel scan segments, default **200** |
| `--sample-fraction` | no | Scan only this fraction of segments and extrapolate |

## Filter syntax here is DynamoDB, not Spark SQL

`scancount` takes a real DynamoDB `FilterExpression`, pushed down to the service.
That is a different language from `count`/`find`'s Spark SQL `--where`. **Quote
it carefully: single quotes around the JSON, double quotes within.**

```sh
./bulk scancount --table audit \
  --filter-expression "#ts > :ts" \
  --expression-names '{"#ts": "timestamp"}' \
  --expression-values '{":ts":"2025-01-01"}'
```

## Do not use it for an unfiltered total

An unfiltered count of the whole table is **already free and exact from
`DescribeTable`** (`Table.ItemCount`, refreshed roughly every 4 hours). Paying
for a scan to get a number you can have for nothing is the main way this command
is misused. `scancount` earns its cost when there is a filter, an index, or a
skew question.

## Finding hot partitions

`--per-segment` prints each segment's item count, sorted, with a skew ratio, and
**warns when the hottest segment exceeds 5× the mean.** This is the quickest
read on uneven key distribution:

```sh
./bulk scancount --table orders --per-segment
```

`--segments` controls the resolution: fewer for a small table, more to see finer
detail. Default 200.

## Estimating a filtered count from a sample

`--sample-fraction` scans a fraction of segments and extrapolates a total with a
**95% confidence interval**. Intended specifically for a rough *filtered* count
of a large table — pair it with `--filter-expression`, because the unfiltered
number is free anyway.

```sh
# Divide into 10,000 segments, sample 1%, estimate how many items predate 2025
./bulk scancount --table audit \
  --filter-expression "#ts < :cutoff" \
  --expression-names '{"#ts": "timestamp"}' \
  --expression-values '{":cutoff":"2025-01-01"}' \
  --segments 10000 --sample-fraction 0.01 --per-segment
```

Add `--per-segment` when you care *why* the interval is wide: skew is what widens
it. A heavily skewed table gives a poor estimate from a small sample, and the
per-segment output is the evidence.

## Related

`bulk-count` for a Spark SQL predicate that DynamoDB cannot express (such as
comparing two attributes). `bulk-find` to get the items. `bulk-tuning` for
worker sizing.
