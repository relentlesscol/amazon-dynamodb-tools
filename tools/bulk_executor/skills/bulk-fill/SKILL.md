---
name: bulk-fill
description: |
  Populate an Amazon DynamoDB table with synthetic test data using Bulk
  Executor — millions of generated items from a Python generator module, with
  faker available on the workers. Use for load testing, building a realistic
  scratch table, reproducing a scale problem, or benchmarking capacity.
  Triggers on: "fill my table with test data", "generate test data for
  dynamodb", "load test data", "populate a dynamodb table", "seed a table",
  "create a million items", "synthetic data dynamodb", "bulk fill", "benchmark
  table".
license: Apache-2.0
compatibility: |
  Writes data. Requires point-in-time recovery on the table, a bootstrapped
  account/region, and a READ-WRITE Glue role. Consumes write capacity and costs
  real money at scale. Run from tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, test-data, faker, load-testing, seed
---

# `bulk fill`

```sh
cd tools/bulk_executor
./bulk fill --table mytable --generator default --numitems 1000000
```

| Parameter | Required | Meaning |
|---|---|---|
| `--table` | yes | Target table (must already exist) |
| `--numitems` | yes | How many items to generate |
| `--generator` | yes | Python module producing the items |

A million items takes about 2 minutes.

## The generators that actually ship

`--generator` resolves to `python_modules.fill.<name>`, and the modules present
are:

| Generator | Produces |
|---|---|
| `default` | Simple generic items — the right choice for "just give me data" |
| `users` | User-shaped records |
| `nosk` | Items for a partition-key-only table (no sort key) |
| `multi_entity_relationship` | Related entities in a single-table design |

> **The documented example is wrong.** Both the README and `bulk fill --help`
> show `--generator fakeusers`. **No such module exists** — that command fails
> with `ModuleNotFoundError`. Use `users`.

Match the generator to the table's key schema: `nosk` for a partition-key-only
table, `default`/`users` for classic pk/sk. A mismatch surfaces as a complaint
that the item is missing a key attribute, naming which key and what the item
actually contained.

## Writing your own

A generator exposes `generate()` returning **one item as a dict, or many as a
list**. `faker` is available on the workers and is the suggested way to make
realistic values.

**It must be deployed by `bootstrap` before the job can import it** — the Glue
job loads modules from S3, not from your disk:

```sh
./bulk bootstrap                                  # deploys your module
./bulk fill --table t --generator mygen --numitems 1000
```

Use `--XDev` to re-upload without a full bootstrap while iterating. See
`bulk-custom-verbs`, and note the executor-code rule: never raise from a
generator, accumulate into `error_accumulator` instead.

The tool peeks at generator output to report average item size, so a wildly
variable generator gives a rough size estimate — that is expected.

## Prepare the table first

Two things make a large fill go well:

- **PITR must be enabled** or the run is refused — `fill` is a mutation.
- **Set warm throughput** so you are not throttled from cold. For an on-demand
  table:

  ```sh
  aws dynamodb create-table --table-name mytable \
    --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --warm-throughput WriteUnitsPerSecond=40000
  ```

## Cost is real at scale

`--numitems 100000000` is a hundred million writes. The run prints a DynamoDB
cost estimate before starting and Ctrl-C cancels — **surface that estimate to the
user before letting a big fill proceed.** Bound the rate with `--XMaxWriteRate`
if the table is shared with anything that matters.

## Related

`bulk-scancount` to confirm the load landed. `bulk-load` to ingest real data from
S3 (CSV/JSON/Parquet) instead of generating it. `bulk-delete` to clean up, or just
delete the table.
