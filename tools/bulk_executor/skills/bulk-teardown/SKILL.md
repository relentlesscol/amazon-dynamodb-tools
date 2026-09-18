---
name: bulk-teardown
description: |
  Remove the Bulk Executor infrastructure from an AWS account and region — the
  bulk_dynamodb Glue job, the bootstrap-created execution role, and the S3
  script bucket. Explains what teardown deliberately does not delete.
  Triggers on: "teardown bulk executor", "bulk teardown", "remove the glue
  job", "uninstall bulk executor", "clean up bulk executor", "delete the bulk
  s3 bucket".
license: Apache-2.0
compatibility: |
  Requires the same IAM/Glue/S3 delete permissions as bootstrap. Destructive
  and account/region-wide. Run from tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, teardown, cleanup
---

# `bulk teardown`

```sh
cd tools/bulk_executor
./bulk teardown
```

## Scope — say this to the user before running it

Teardown is **account- and region-wide, not per-table.** It removes the shared
infrastructure that every Bulk Executor user in that account and region depends
on. On a shared account, confirm nobody else is relying on it. There is no
per-user teardown.

## What it removes

- The `bulk_dynamodb` Glue job.
- The Glue execution role — **only if bootstrap created it.** A role you
  supplied with `--XRole <rolename>` is left alone, which is correct: the tool
  does not delete IAM principals it does not own.
- The S3 script bucket `aws-glue-bulk-dynamodb-<region>-<account>-<random>`.

## What it does not remove

- **Any output your jobs wrote to S3.** `find`, `diff` and friends write full
  result sets to the bucket, and teardown does not touch them. If the user wants
  those gone, that is a separate, explicit deletion.
- **The bucket itself, if it holds anything outside `<root>/server`.** Teardown
  refuses and leaves the bucket intact for manual review in the console, rather
  than deleting data it does not recognise. Expect this whenever jobs have
  written output — it is the normal outcome, not a failure.
- **Your DynamoDB tables.** Nothing about teardown touches table data.
- CloudWatch log groups and their contents.

So the common real-world result is: job and role gone, bucket still there with
your output in it. Tell the user that rather than reporting a partial failure.

## Reversing it

Re-run `bulk-bootstrap`. Nothing about teardown is permanent except deleted
scripts, which bootstrap re-uploads. Note the new bucket gets a fresh random
suffix.
