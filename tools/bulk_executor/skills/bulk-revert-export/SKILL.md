---
name: bulk-revert-export
description: |
  Undo the writes captured in a DynamoDB incremental export, rolling a table
  back over that time window with Bulk Executor. Only works on incremental
  exports created with the NEW_AND_OLD_IMAGES view type. Use to reverse a bad
  deploy's writes, roll back a botched backfill, or selectively revert a subset
  via a transform filter.
  Triggers on: "undo an incremental export", "revert-export", "roll back writes
  to my table", "undo a bad backfill", "reverse changes in a time window", "undo
  what that deploy wrote", "revert dynamodb changes".
license: Apache-2.0
compatibility: |
  DESTRUCTIVE — reverses writes. Requires an incremental export with
  NEW_AND_OLD_IMAGES, point-in-time recovery on the table, a bootstrapped
  account/region, and a READ-WRITE Glue role. Run from tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, revert, rollback, incremental-export, recovery
---

# `bulk revert-export`

Reverses every write recorded in an incremental export window, restoring each
touched item to its pre-window state.

```sh
cd tools/bulk_executor
./bulk revert-export --table users \
  --s3-path s3://bucket/prefix/AWSDynamoDB/01716790307109-5f9d6aaa
```

| Parameter | Required | Meaning |
|---|---|---|
| `--table` | yes | Table to roll back |
| `--s3-path` | yes | S3 path of the **incremental** export |
| `--transform` | no | Module filtering which records to revert, applied **before** the revert logic |

## The hard prerequisite

**The export must be an incremental export with the `NEW_AND_OLD_IMAGES` view
type.** Nothing else works, and the reason is structural: reverting a write needs
the *old* image to restore. An incremental export taken with `NEW_IMAGES_ONLY`
does not contain it, and a full export has no concept of a window.

Check this before promising a rollback. If the export is the wrong type, this
verb cannot help and the fallback is PITR restore to a timestamp — which restores
the **whole table** to a point in time rather than selectively undoing writes, and
is a different operation with different consequences. Say which one you are
proposing.

## What "revert" means precisely

For each record in the window: an update or overwrite is rolled back to its old
image, and an insert is removed. It is a targeted inverse of that window's
writes, **not** a point-in-time restore — items *not* touched in the window are
left exactly as they are now, including any changes made after the window closed.

That is usually what you want after a bad deploy. But it means **a later
legitimate write to an item the window also touched will be clobbered** by the
old image. If real traffic has continued since, reverting is not automatically
safe — establish with the user whether anything wrote to those items afterwards.

## Reverting only part of a window

`--transform` filters records before the revert applies, so you can undo one
entity type, one key prefix, or one attribute's worth of damage rather than the
whole window:

```sh
./bulk revert-export --table users \
  --s3-path s3://bucket/prefix/AWSDynamoDB/0171679…-5f9d6aaa \
  --transform my_filter
```

**A custom transform must be deployed by `./bulk bootstrap` first** or the job
fails with `ModuleNotFoundError`. See `bulk-custom-verbs`.

## Before running it

This is one of the most consequential commands in the tool. Reasonable care:

1. **Confirm the export type** is incremental + `NEW_AND_OLD_IMAGES`.
2. **Confirm the window** is the one that contains the bad writes, and only
   those. `--s3-path` points at the `<timestamp>-<hash>` directory under
   `AWSDynamoDB/`.
3. **Confirm PITR is on** — required, and your safety net if the revert is itself
   wrong. Note that for a table in another account the check is **skipped with a
   warning, not enforced**.
4. **Consider rehearsing**: `bulk-copy` the table to scratch, revert there,
   `bulk-diff` against the original to see exactly what would change.

Step 4 costs a copy and a diff and turns an irreversible action into a reviewable
one. Offer it for anything touching production.

## Related

`bulk-load-export` to load an export forward instead of reversing it.
`bulk-copy` + `bulk-diff` for the rehearsal loop. `bulk-tuning` for rate limits —
a revert consumes write capacity like any mutation.
