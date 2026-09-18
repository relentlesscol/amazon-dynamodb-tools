---
name: bulk-sql
description: |
  Run arbitrary Spark SQL against an Amazon DynamoDB table with Bulk Executor —
  aggregates, GROUP BY, HAVING, ORDER BY, window functions — similar to using
  Athena but reading the live table. Use for analytical questions DynamoDB
  cannot answer, such as distributions, top-N by group, or attribute
  cardinality.
  Triggers on: "run sql against dynamodb", "group by on dynamodb", "aggregate
  my dynamodb table", "distribution of values", "count by category", "sql query
  on a dynamodb table", "athena-like query", "bulk sql", "average item size by".
license: Apache-2.0
compatibility: |
  Requires a bootstrapped account/region and AWS credentials. Runs a real Glue
  job (~1 min startup) and reads the whole table through the connector. Run from
  tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, spark-sql, analytics, aggregate, groupby
---

# `bulk sql`

Executes Spark SQL against the table, read through the Glue DynamoDB connector.

```sh
cd tools/bulk_executor
./bulk sql --table products --query "SELECT COUNT(*) FROM products"
./bulk sql --table users --query "SELECT age, COUNT(*) FROM users GROUP BY age" --limit 100
```

| Parameter | Required | Meaning |
|---|---|---|
| `--table` | yes | Table name, or full ARN for cross-region / cross-account |
| `--query` | yes | The Spark SQL query |
| `--limit` | no | Cap the number of results returned |

## Two hard constraints, both easy to trip

**1. The view name is the table name with `-` and `.` replaced by `_`.**

The table is registered as a Spark temp view named after the table, but hyphens
and dots become underscores. So a table called `my-orders.v2` must be referenced
as `my_orders_v2`:

```sh
./bulk sql --table my-orders --query "SELECT COUNT(*) FROM my_orders"
```

Hyphenated table names are common, and using the literal table name in `FROM`
fails with an unresolved-relation error. Always translate.

**2. The query must begin with `SELECT`.** It is validated before execution and
anything else is rejected with `Only SELECT queries are supported`. That means:

- No `WITH` / CTEs — rewrite as a subquery in the `FROM` clause.
- No `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `EXPLAIN`. Mutations go through
  `bulk-update` / `bulk-delete`, which have their own safety interlocks.

`--limit` must be a positive integer or the run is rejected.

## What it is for

This is the analytical escape hatch. Anything needing a `GROUP BY`, a `HAVING`,
a join against itself, a percentile or a window function belongs here rather
than in `count`/`find`:

```sh
# Distribution of a status attribute
./bulk sql --table orders --query \
  "SELECT status, COUNT(*) AS n FROM orders GROUP BY status ORDER BY n DESC"

# Cardinality check before choosing a GSI partition key
./bulk sql --table orders --query \
  "SELECT COUNT(DISTINCT customer_id) FROM orders"
```

Dialect reference:
<https://spark.apache.org/docs/latest/api/sql/index.html>.

## Cost: it reads the whole table

A `--limit` bounds the **rows returned, not the rows read.** The connector still
pulls the table into Spark to evaluate the query, so a `SELECT ... LIMIT 10` over
a billion-item table costs the same reads as any other full pass. If the user
expects `--limit` to make it cheap, correct that.

Consequently:

- For a plain unfiltered count, use `DescribeTable` or `bulk-scancount` — never
  `sql`.
- To cut the read cost, cut the data with a `WHERE` in the query, or accept the
  full-table read as the price of the aggregate.
- The run prints a DynamoDB cost estimate before starting; Ctrl-C cancels.

## Errors

A malformed query surfaces as a Spark SQL failure in the job output. The tool
reports the underlying message rather than relabelling everything as a generic
"SQL query error", so read the actual text — it usually names the unresolved
column or the syntax position.

## Related

`bulk-count` / `bulk-find` for simple predicate filtering without the SQL
ceremony. `bulk-scancount` for fast counting. `bulk-tuning` if the query runs out
of memory or time — a large `GROUP BY` or `ORDER BY` is a sort, so
`--XWorkerType R.1X` is the lever.
