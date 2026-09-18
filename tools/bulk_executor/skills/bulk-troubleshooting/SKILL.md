---
name: bulk-troubleshooting
description: |
  Diagnose a Bulk Executor run that failed, hung, produced no output, or was
  refused before starting. Covers the four failures the tool itself diagnoses
  (out of memory, AccessDenied, ModuleNotFound, DynamoDB throttling), the PITR
  refusal, missing-bootstrap errors, and the CloudWatch LiveTail blind spot.
  Triggers on: "bulk job failed", "bulk executor error", "AccessDeniedException
  in glue job", "ModuleNotFoundError generator", "OutOfMemoryError", "PITR must
  be enabled", "bulk job hangs", "no output from bulk", "job failed but no
  error", "ProvisionedThroughputExceededException".
license: Apache-2.0
metadata:
  tags: dynamodb, aws, glue, troubleshooting, errors, debugging
---

# Diagnosing a Bulk Executor failure

Read the run's own output first. Bulk Executor watches its Glue log stream for
known-bad signals and, when it sees one, stops the job and prints both a
one-line summary and the next thing to try. **If the user pasted output, the
diagnosis is probably already in it** — find it before theorising.

## The four failures the tool diagnoses for you

| Log signal | What happened | Fix |
|---|---|---|
| `OutOfMemoryError:` | Job ran out of memory | `--XWorkerType R.1X` — double the heap of the default `G.1X`. Usually a large `--orderby`. |
| `AccessDeniedException:` | Glue role lacks a permission | The denied action is named in the output. Re-bootstrap with a role that allows it: `./bulk bootstrap --XRole READ-WRITE` for anything that writes. |
| `ModuleNotFoundError:` | Job could not import a Python module | A `--generator` or `--transform` module must be deployed by `./bulk bootstrap` before a job can import it. See `bulk-custom-verbs`. |
| `ProvisionedThroughputExceededException:` | DynamoDB throttled past the SDK's retries | Give the table more capacity, or hold bulk back with `--XMaxReadRate` / `--XMaxWriteRate`. |

## Refused before the job started

These are client-side and fail fast, before any Glue cost.

- **`point in time recovery (PITR) must be enabled ... before performing bulk
  mutations`** — enable PITR on the table. Not overridable; it is the safety
  interlock on every mutating command.

  ```sh
  aws dynamodb update-continuous-backups --table-name <t> \
    --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
  ```

  On a just-created table this call errors if issued too soon. Wait and retry.

- **`Could not locate action named '<verb>'`** — the verb name is wrong, or a
  custom verb was never deployed. The tool prints the available actions
  underneath; use that list rather than guessing.

- **Glue job not found / entity does not exist** — the account and region have
  not been bootstrapped. Bootstrap is per account *and* per region. Use
  `bulk-bootstrap`.

- **`./bulk: command not found` or import errors** — you are not in
  `tools/bulk_executor/`. The CLI resolves `client/src/`, `HELP.md` and its
  upload paths relative to the working directory, so it must be run from there.

## Cross-account: the PITR check goes quiet

When the target table lives in a different account, **PITR status is unreadable
and the check is skipped with a warning rather than enforced.** The run
proceeds. So on a cross-account mutation the safety interlock you are relying on
is not actually engaged — verify PITR yourself in the owning account before
mutating. Cross-account access also needs a resource-based policy on the table.

## Hangs and silence

- **No output at all, job appears stuck.** Bulk Executor streams progress via
  **CloudWatch LiveTail, which does not work where a CloudWatch Logs VPC
  endpoint exists.** AWS CloudShell is such an environment. The Glue job may be
  running fine while you see nothing — check the job run in the Glue console,
  and re-run from a laptop or EC2, or target a different region.
- **Roughly a minute of silence at the start of every run is normal** — that is
  Glue startup, not a hang.
- **Job ran long then timed out.** Usually a rate problem rather than a timeout
  problem. The run logs a warning at the start when the effective rate against
  the table's size will not finish inside `--XTimeout`; that warning is
  observational and does not block. Raise the rate, raise the timeout, or narrow
  the work. See `bulk-tuning`.

## Escalating

`--XDebug` turns on debug logging. Beyond that, the Glue job is named
`bulk_dynamodb` — its run history, arguments and full logs are visible in the
Glue console and CloudWatch, which is where to look when the client-side output
is not enough.
