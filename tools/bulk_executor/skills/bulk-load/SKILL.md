---
name: bulk-load
description: |
  Load data from S3 into an existing Amazon DynamoDB table with Bulk Executor —
  CSV, line-oriented JSON, or Parquet — through the Glue connector, with
  format-specific parsing options. Use for importing a data dump, migrating from
  another store, or ingesting an ETL output.
  Triggers on: "load csv into dynamodb", "import data into dynamodb", "load
  parquet to dynamodb", "load json into dynamodb", "ingest s3 data into
  dynamodb", "bulk load", "import a dump into my table", "restore from csv".
license: Apache-2.0
compatibility: |
  Writes data. Requires point-in-time recovery on the table, a bootstrapped
  account/region, and a READ-WRITE Glue role. The Glue role needs read access to
  the source S3 path. Run from tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, load, import, csv, json, parquet, etl
---

# `bulk load`

Reads from S3 and writes to an existing table via the Glue DynamoDB connector.

```sh
cd tools/bulk_executor
./bulk load --table orders --format csv --s3-path s3://my-bucket/orders/
```

| Parameter | Required | Meaning |
|---|---|---|
| `--table` | **yes** | Target table — must already exist |
| `--format` | **yes** | `csv`, `json`, or `parquet` |
| `--s3-path` | **yes** | Source path, e.g. `s3://bucket/prefix` |
| `--removeEmptyStringAttributes` | no | Drop attributes whose value is empty rather than writing them |

> **The built-in help is incomplete.** `bulk load --help` does not list
> `--s3-path`, and its example (`bulk load --table orders --format csv`) is
> missing it — that command fails, because `--s3-path` is required. Always pass
> it.

`--removeEmptyStringAttributes` matters most for CSV, where a missing value
becomes an empty string and would otherwise be written as a real empty attribute.
Decide deliberately: dropping it means the attribute is absent, which changes how
queries and `attribute_exists` behave.

## CSV options

| Parameter | Meaning |
|---|---|
| `--separator` | Delimiter character. Default comma |
| `--quoteChar` | Quote character. Default `"`. Set to `-1` to disable quoting entirely |
| `--escaper` | Escape character; the character after it is taken as-is |
| `--withHeader` | Treat the first line as a header (**default**) |
| `--withoutHeader` | Do not treat the first line as a header |
| `--skipFirst` | Skip the first data line. Default false |
| `--multiLine` | A record may span multiple lines (quoted newlines) |
| `--mappings` | `s3://…/mappings.json` mapping auto-generated column names (`col0`, `col1`, …) to real names and non-String types |

**`--withoutHeader` is real but undocumented in the built-in help.** Without a
header, columns arrive as `col0`, `col1`, … and you almost certainly want
`--mappings` to name and type them — otherwise every attribute lands as a String
called `colN`.

`--multiLine` reduces performance because file splitting has to be more cautious.
Only set it if records genuinely contain quoted newlines.

## JSON options

Expects **line-oriented JSON, not DynamoDB-JSON.** One object per line.

| Parameter | Meaning |
|---|---|
| `--jsonPath` | JsonPath selecting the objects to read — needed when records are nested inside an outer array |
| `--multiline` | A record may span multiple lines |

> **Watch the casing.** CSV uses `--multiLine` (capital L); JSON uses
> `--multiline` (lowercase). They are separate flags on the same command. Using
> the wrong one for your format silently does nothing.

If your data is DynamoDB-JSON from an export, this is the wrong verb — use
`bulk-load-export`.

## Parquet options

| Parameter | Meaning |
|---|---|
| `--compression` | `uncompressed`, `snappy` (default), `gzip`, `lzo` |
| `--blockSize` | Row-group buffer size in bytes. Default 134217728 (128 MB) |
| `--pageSize` | Page size in bytes. Default 1048576 (1 MB) |

Full format semantics are Glue's:
[CSV](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-csv-home.html) ·
[JSON](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-json-home.html) ·
[Parquet](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-parquet-home.html)

## Keys must be present in the data

The load writes items as it finds them. Every record needs the target table's
partition key (and sort key, if any) with the right names and types. A record
missing a key attribute fails, reporting which key was missing and what the item
contained — that message is the fastest route to a mapping mistake.

## Throughput

`load` is the highest-write-volume verb and the one worth rate-bounding:

```sh
./bulk load --table t --format parquet --s3-path s3://b/p/ --XMaxWriteRate 40000
```

Pre-warm the target's write throughput for a large load, or you throttle from
cold. If the rate would not finish inside `--XTimeout` the run warns at the start
— read it, since that warning does not block. See `bulk-tuning`.

## Related

`bulk-load-export` for a DynamoDB export (DynamoDB-JSON) rather than plain data.
`bulk-fill` to generate synthetic data instead of importing it. `bulk-scancount`
to confirm the loaded count. `bulk-diff` to verify against a source of truth.
