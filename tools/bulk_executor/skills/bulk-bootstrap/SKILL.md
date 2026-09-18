---
name: bulk-bootstrap
description: |
  Run the one-time Bulk Executor bootstrap for an AWS account and region:
  creates the S3 script bucket, the Glue execution role, and the shared
  bulk_dynamodb Glue job. Also the way to deploy a custom verb, generator or
  transform module. Covers --XRole READ-ONLY / READ-WRITE / custom-role-name
  and the requirements a custom role must satisfy.
  Triggers on: "bootstrap bulk executor", "bulk bootstrap", "set up the glue
  job", "XRole", "READ-ONLY vs READ-WRITE", "which role does bulk use",
  "deploy my generator", "glue job not found", "re-bootstrap".
license: Apache-2.0
compatibility: |
  Requires bootstrap-tier AWS permissions (Glue, S3 and IAM create). Creates
  billable resources. Run from tools/bulk_executor/.
metadata:
  tags: dynamodb, aws, glue, iam, bootstrap, setup
---

# `bulk bootstrap`

One-time setup **per account and per region**. Being bootstrapped in
`us-east-1` says nothing about `eu-west-1`.

```sh
cd tools/bulk_executor
./bulk bootstrap --XRole READ-WRITE
```

## What it creates

- An S3 bucket named `aws-glue-bulk-dynamodb-<region>-<account>-<random>`,
  holding the execution scripts. TLS in transit is already enforced.
- A Glue execution role, unless you supply your own.
- A Glue job named `bulk_dynamodb`, pointing at that role and those scripts.
- CloudWatch log groups, with retention set to 365 days.

Everything the tool runs afterwards goes through that single shared Glue job.

## Choosing the role

`--XRole` is the decision that matters. Omit it and you get an interactive
prompt that walks you through it.

| Value | Effect |
|---|---|
| `READ-ONLY` | Creates a role that can read DynamoDB. Reads only — `update`, `delete`, `fill`, `load`, `copy` targets and the export verbs will fail with `AccessDeniedException`. |
| `READ-WRITE` | Creates a role with full DynamoDB access. Needed for any mutation. |
| `<rolename>` | Uses a pre-existing role you control — the option for scoping access to specific tables. |

Choose `READ-ONLY` when the user only wants to inspect data. It is the safer
default and re-bootstrapping later is cheap.

## Supplying your own role

The bootstrap validates a custom role before creating any infrastructure, so a
role that does not meet these requirements fails fast:

- **Name must start with `AWSGlueServiceRole`** (the generated one is
  `AWSGlueServiceRoleBulkDynamoDB`).
- Trust policy allowing `glue.amazonaws.com` to assume it.
- Managed policy `AWSGlueServiceRole` for baseline Glue execution.
- `AWSPriceListServiceFullAccess`, so cost estimates work.
- Optional: `ServiceQuotasReadOnlyAccess`.
- Optional: `application-autoscaling:DescribeScalableTargets` and
  `DescribeScalingPolicies` for rate-limiting heuristics. These do not support
  resource-level scoping, so they must be granted on `"Resource": "*"`.
- DynamoDB access — `AmazonDynamoDBReadOnlyAccess`,
  `AmazonDynamoDBFullAccess`, or a tighter table-scoped policy.

Missing the autoscaling actions is not fatal; it degrades the capacity warnings
rather than blocking the run.

## Two tiers of user

The design assumes a **powerful admin** who runs bootstrap once (needs Glue, S3
and IAM create), and **ordinary users** who afterwards only need permission to
invoke the Glue job, read its CloudWatch logs, and read the output bucket. If
the user hits `AccessDenied` on bootstrap itself, they are the wrong tier — that
is not something to work around.

## Re-running it

Bootstrap is also the deployment mechanism for custom code. A custom verb,
`--generator` or `--transform` module is only importable by the Glue job after a
bootstrap uploads it — see `bulk-custom-verbs`. On a shared account, that means
whoever holds bootstrap permissions must run it with your module in their tree.

`--XMaxConcurrentRuns` (default 20) caps concurrent runs of the shared job.
`--XRetries` defaults to 0 on purpose, so a misconfigured job fails fast.

## Afterwards

Harden if you care to: adjust the bucket's encryption, lifecycle or versioning
per S3 security best practices, and change the log groups'
`LogRetentionPeriod` from 365 days if that does not suit.

Remove it all with `bulk-teardown`.
