---
name: bulk-update
description: |
  Modify items across a large Amazon DynamoDB table with Bulk Executor by
  supplying a Python generator module that sees each item and returns an update
  expression. Use for backfilling a new attribute, renaming or removing an
  attribute, reshaping keys, or any schema migration over an existing table.
  Requires point-in-time recovery.
  Triggers on: "backfill an attribute", "add a field to every item", "bulk
  update dynamodb", "remove an attribute from all items", "rename attribute",
  "migrate my dynamodb schema", "update every item", "write an update
  generator". To mask or strip PII while loading an export, use
  bulk-load-export — the shipped PII transforms belong to that verb, not this
  one.
license: Apache-2.0
compatibility: |
  DESTRUCTIVE. Requires point-in-time recovery on the table, a bootstrapped
  account/region, a READ-WRITE Glue role, and the generator module deployed via
  ./bulk bootstrap. Run from tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, update, backfill, migration, schema, pii
---

# `bulk update`

Performs a parallel update driven by your code. The generator is handed **each
item in turn** and returns either nothing (no update needed) or the update
expression kwargs to apply.

```sh
cd tools/bulk_executor
./bulk update --table tickets --generator backfillSeatPK
```

| Parameter | Required | Meaning |
|---|---|---|
| `--table` | yes | Table name, or full ARN |
| `--generator` | yes | Python module deciding the per-item update |

There is no `--where`. **Filtering is the generator's job** — return empty for
items you do not want to touch. That is the idiom, and it is why `update` can
express migrations a predicate cannot.

## The deployment step people miss

**The generator must be uploaded to S3 by `bootstrap` before the Glue job can
import it.** Putting the file in your source tree is not enough — the job loads
modules from the bucket. Skipping this produces `ModuleNotFoundError` mid-run.

```sh
./bulk bootstrap            # deploys generators present in your tree
./bulk update --table t --generator mygen
```

Generator modules live under the `update` folder; **`touched`** ships as the
worked example (it adds a `touched` timestamp attribute). On a shared account,
whoever holds bootstrap permissions must run bootstrap **with your module in
their tree**. While iterating, `--XDev` re-uploads modules without a full
bootstrap. See `bulk-custom-verbs` for writing one.

## Preview before you commit

There is no dry-run. Get confidence cheaply instead:

1. **Read the generator** and state plainly which items it will touch and what
   it changes. If it is not obvious from the code, do not run it.
2. **Sample the shape** of affected items with `bulk-find` before mutating:
   `./bulk find --table t --where "attribute_missing_or_whatever" --limit 20`
3. **Rehearse on a copy.** `bulk-copy` into a scratch table, run the update
   there, `bulk-diff` the two. For a risky migration on a large table this is
   the honest way to validate, and it is what the tool's own shape encourages.

## PITR is mandatory, and it is your undo

The run is refused unless point-in-time recovery is enabled on the table. It is
the only rollback for a wrong generator. Note that when the table is in a
**different account** the PITR check is **skipped with a warning, not enforced** —
verify manually in the owning account before mutating.

## Write inside the generator carefully

Two rules from the harness that bite in generator code:

- **Never raise from executor code.** Many workers hit the same condition and you
  get thousands of duplicate traces. Add to the `error_accumulator` and return;
  the driver raises the first accumulated error. `fill` demonstrates it.
- **Rate limiting is not optional.** Any DynamoDB access must be bounded, either
  through the connector's throughput options or the
  `python_modules.shared.rate_limiter` library.

## Capacity and tuning

Updates consume write capacity — bound it with `--XMaxWriteRate` so a backfill
does not starve production traffic:

```sh
./bulk update --table t --generator mygen --XMaxWriteRate 20000
```

See `bulk-tuning`. A `ProvisionedThroughputExceededException` means raise table
capacity or lower the rate.

## Related

`bulk-delete` to remove rather than modify. `bulk-copy` + `bulk-diff` to rehearse
and verify. `bulk-custom-verbs` to write the generator. `bulk-troubleshooting`
for `ModuleNotFoundError` and `AccessDeniedException`.
