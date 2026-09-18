---
name: bulk-executor-setup
description: |
  Get Bulk Executor for Amazon DynamoDB ready to run, and diagnose why a bulk
  command will not start. Checks the repo checkout, boto3, AWS credentials,
  region, whether bootstrap has been run, and whether the target table meets
  the PITR requirement. Use FIRST, before any other bulk-* skill, whenever the
  user has not run a bulk command in this session.
  Triggers on: "set up bulk executor", "install bulk executor", "get started
  with bulk executor", "bulk executor prerequisites", "is bulk executor ready",
  "bulk command not found", "./bulk fails", "which region does bulk use".
license: Apache-2.0
compatibility: |
  Requires a local checkout of awslabs/amazon-dynamodb-tools, python3 with
  boto3, and AWS credentials. Bulk commands run a real AWS Glue job and incur
  DynamoDB + Glue cost. Does not work where a CloudWatch Logs VPC endpoint is
  configured (breaks CloudWatch LiveTail) — this rules out AWS CloudShell.
metadata:
  tags: dynamodb, aws, glue, setup, prerequisites, bootstrap
---

# Bulk Executor setup and readiness

Bulk Executor is a CLI front end (`./bulk`) that runs an AWS Glue Spark job to
operate on DynamoDB tables in parallel. Every command needs three things in
place: a checkout, credentials, and a one-time bootstrap in the target account
and region.

## The one rule that breaks everything else

**`./bulk` only runs from inside `tools/bulk_executor/`.** It resolves
`client/src/`, `HELP.md`, and its upload paths relative to the working
directory. Always `cd` there first; never invoke it by absolute path from
elsewhere.

```sh
cd <checkout>/tools/bulk_executor && ./bulk count --table t
```

## Readiness check

Run these before the user's first bulk command. Report what you find; do not
guess.

```sh
# 1. Locate the checkout. If you cannot find it, ask — do not clone silently.
ls tools/bulk_executor/bulk

# 2. Dependency (boto3 is the only one)
cd tools/bulk_executor && python3 -c 'import boto3; print(boto3.__version__)'

# 3. Credentials and the region the job will actually use
aws sts get-caller-identity
aws configure get region || echo "$AWS_DEFAULT_REGION"

# 4. Has bootstrap run in this account/region? Look for the Glue job.
#    The job is named bulk_dynamodb (underscore), its generated role is
#    AWSGlueServiceRoleBulkDynamoDB, and its bucket is
#    aws-glue-bulk-dynamodb-<region>-<account>-<random>.
aws glue get-job --job-name bulk_dynamodb 2>&1 | head -5
```

If step 4 fails with an entity-not-found error, the account/region has not been
bootstrapped — use the `bulk-bootstrap` skill. Bootstrap is **per account and
per region**; being set up in `us-east-1` says nothing about `eu-west-1`.

## Environment traps

- **CloudWatch Logs VPC endpoint.** Bulk Executor streams job output via
  CloudWatch LiveTail, which does not work when the local environment has a
  CloudWatch Logs VPC endpoint. **AWS CloudShell is such an environment** —
  bulk works there only if you target a *different* region. A laptop or EC2 box
  is the intended client.
- **Region ambiguity.** `--XRegion` decides where the Glue job runs *and* which
  table is validated. A full table ARN overrides it. If the user has not been
  explicit and more than one region is plausible, ask.
- **Wrong account.** `--XAccount` exists but the credentials still have to
  resolve there. Confirm `sts get-caller-identity` matches the user's intent
  before running anything that mutates data.

## PITR gates every mutation

Bulk Executor **refuses to mutate a table unless point-in-time recovery is
enabled.** This applies to `update`, `delete`, `fill`, `load`, `load-export`,
`revert-export`, and the target of `copy`. Check before promising a mutation
will run:

```sh
aws dynamodb describe-continuous-backups --table-name <t> \
  --query 'ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus'
```

Enable it with `aws dynamodb update-continuous-backups --table-name <t>
--point-in-time-recovery-specification PointInTimeRecoveryEnabled=true`. On a
freshly created table this call fails if issued too soon — wait and retry.

## What every run costs and how long it takes

Set expectations up front, because the numbers surprise people:

- **Every command carries roughly a minute of Glue startup overhead.** A
  trivial count on a tiny table still takes ~2 minutes. This is normal.
- **Each run prints discovered table metrics and a DynamoDB cost estimate
  before doing work, and the user can Ctrl-C to cancel.** Surface that estimate
  to the user rather than swallowing it.
- **For small jobs the Glue cost dominates**, and the default is 220 workers.
  Lowering `--XNumberOfWorkers` is the single biggest small-job saving. See the
  `bulk-tuning` skill.

## Then pick the command skill

`bulk-count`, `bulk-find`, `bulk-scancount`, `bulk-sql`, `bulk-update`,
`bulk-delete`, `bulk-fill`, `bulk-copy`, `bulk-diff`, `bulk-load`,
`bulk-load-export`, `bulk-revert-export`, plus `bulk-bootstrap` /
`bulk-teardown` for lifecycle. `bulk-troubleshooting` covers failures once a
command has started.

For a first-run walkthrough that creates a throwaway table and loads a million
rows, read `references/first-run.md`.
