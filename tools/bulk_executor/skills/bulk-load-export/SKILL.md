---
name: bulk-load-export
description: |
  Load a DynamoDB export from S3 into an existing table with Bulk Executor, with
  an optional Python transform applied per record. Use to restore or replay a
  full or incremental export, move a table via export instead of a live read,
  or mask/strip PII while loading — shipped transforms cover attribute masking,
  attribute removal, and adding an MD5 of the partition key.
  Triggers on: "load a dynamodb export", "restore from an export", "replay an
  export", "load-export", "import a DynamoDB export from S3", "mask PII while
  loading", "strip an attribute during load", "transform records on load",
  "AWSDynamoDB export folder".
license: Apache-2.0
compatibility: |
  Writes data. Requires point-in-time recovery on the target table, a
  bootstrapped account/region, a READ-WRITE Glue role, and Glue read access to
  the export's S3 path. A custom --transform module must be deployed via
  ./bulk bootstrap. Run from tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, export, restore, pii, transform, migration
---

# `bulk load-export`

Loads a **DynamoDB export** (DynamoDB-JSON, as written by the export-to-S3
feature) into an existing table.

```sh
cd tools/bulk_executor
./bulk load-export --table users \
  --s3-path s3://bucket/prefix/AWSDynamoDB/01716790307109-5f9d6aaa
```

| Parameter | Required | Meaning |
|---|---|---|
| `--table` | yes | Destination table — must already exist |
| `--s3-path` | yes | S3 path where the export resides |
| `--transform` | no | Module with `transform_full_record` and/or `transform_incremental_record` |

## Getting `--s3-path` right

Point at the **export instance directory**, the one named
`<timestamp>-<hash>` under `AWSDynamoDB/` — not the bucket root and not the
`data/` subdirectory:

```
s3://exported-data/prod/AWSDynamoDB/01716790307109-5f9d6aaa   ← this level
```

That directory holds the manifest the loader reads. A path one level too high or
too low is the most common failure, and it surfaces as a manifest or validation
error rather than as "no data found".

## This is not `bulk load`

`load` expects plain CSV / line-oriented JSON / Parquet. `load-export` expects
**DynamoDB-JSON with an export manifest.** Feeding an export to `load` (or plain
JSON to `load-export`) fails on structure. If the user has an `AWSDynamoDB/`
folder, they want this verb.

## Transforms, including the PII ones

`--transform` names a module applied per record before the write. It implements
`transform_full_record` (for full exports) and/or `transform_incremental_record`
(for incremental ones). Shipped examples:

| Transform | Effect |
|---|---|
| `pii_mask_attribute` | Mask a named attribute's value |
| `pii_remove_attribute` | Drop a named attribute entirely |
| `pkmd5_add_attribute` | Add an MD5 hash of the partition key as an attribute |

This makes `load-export` the practical way to **produce a scrubbed copy of
production**: export prod, load into a scratch table with a PII transform, and
the sensitive values never land.

```sh
./bulk load-export --table users-scratch \
  --s3-path s3://exported-data/prod/AWSDynamoDB/0171679…-5f9d6aaa \
  --transform pii_mask_attribute
```

Read the transform's source before relying on it to protect anything — which
attribute it targets is defined in the module, not on the command line. See
`server/src/python_modules/load_export/transform/` and that directory's README.

**A custom transform must be deployed by `./bulk bootstrap` before the Glue job
can import it** — otherwise `ModuleNotFoundError` mid-run. See
`bulk-custom-verbs`.

## Why export-and-load beats a live copy

For moving a whole table, loading an export reads **S3 instead of the source
table**, so it consumes no read capacity on production and cannot throttle it.
`bulk-copy` is simpler for small tables and needs no export step, but at scale
this path is usually kinder to the source.

## Requirements and cost

PITR must be enabled on the destination — and when the destination is in another
account, that check is **skipped with a warning rather than enforced.** Writes
consume write capacity, so bound them with `--XMaxWriteRate` and pre-warm the
target for a large load. See `bulk-tuning`.

## Related

`bulk-revert-export` to undo an incremental export's writes. `bulk-load` for
plain CSV/JSON/Parquet. `bulk-diff` to verify the result against the source.
