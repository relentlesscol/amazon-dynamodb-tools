# First run: a throwaway table, end to end

Use this when the user wants to try Bulk Executor without touching real data.
Every step is real AWS and costs real money, though a million-row table is
cents. Total wall time is about 10 minutes, most of it Glue startup.

Confirm the account and region with the user before step 1.

## 1. Bootstrap the account and region

```sh
cd <checkout>/tools/bulk_executor
./bulk bootstrap --XRole READ-WRITE
```

`READ-WRITE` is needed because this walkthrough writes. Omitting `--XRole`
gives an interactive prompt instead. See the `bulk-bootstrap` skill for the
read-only and custom-role variants.

## 2. Create a table

Warm throughput is set high so the fill is not throttled from cold.

```sh
aws dynamodb create-table \
  --table-name mytable \
  --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --warm-throughput WriteUnitsPerSecond=40000
```

## 3. Enable PITR — not optional

Bulk Executor refuses to mutate a table without it.

```sh
aws dynamodb wait table-exists --table-name mytable
aws dynamodb update-continuous-backups --table-name mytable \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
```

If this errors, the table was created too recently. Wait and retry.

## 4. Fill a million rows

```sh
./bulk fill --table mytable --generator default --numitems 1000000
```

`default` is a generator that ships with the tool, so this needs no custom
code. Expect about 2 minutes.

## 5. Confirm the load

```sh
./bulk scancount --table mytable
```

`scancount` uses a parallel scan rather than the Glue connector, so it is
usually the faster way to count. Expect about 2 minutes and a result of
1000000.

## 6. Try a read

```sh
./bulk find --table mytable --limit 10
```

The first few items print to the console; a full result set goes to S3.

## 7. Clean up

Delete the table, and tear down the Glue infrastructure only if the user is
done with Bulk Executor entirely — `teardown` is account/region-wide, not
per-table.

```sh
aws dynamodb delete-table --table-name mytable
./bulk teardown   # optional; removes the shared Glue job, role and bucket
```
