---
name: bulk-custom-verbs
description: |
  Write a custom Bulk Executor command (verb), or a --generator / --transform
  module for fill, update, load-export and revert-export. Covers the
  client-side/server-side split, where modules must live, why bootstrap is
  required to deploy them, the --XDev fast redeploy loop, and the exception
  rules that separate driver code from executor code.
  Triggers on: "write a custom bulk verb", "add a bulk executor command",
  "custom generator for fill", "write an update generator", "transform module
  for load-export", "extend bulk executor", "ModuleNotFoundError generator",
  "my generator isn't found", "XDev".
license: Apache-2.0
metadata:
  tags: dynamodb, aws, glue, spark, extensibility, custom-verb, generator
---

# Writing custom verbs and generator modules

Bulk Executor is a harness plus a set of command scripts. Every built-in verb is
just a pair of Python modules, so a custom verb is written the same way the
built-ins are — read the closest built-in and follow it.

## The two halves

| Half | Lives in | Runs on | Job |
|---|---|---|---|
| Client | `client/src/python_modules/<verb>.py` | Your machine | Parse and validate args, check the table exists, decide whether the server half needs to run |
| Server | `server/src/python_modules/<verb>.py` or `<verb>/__init__.py` | Glue Spark workers | Do the bulk work in parallel |

Each exposes a `run(...)` entry point. The client's `run(env_configs)` returns
`(is_client_and_server_action, processed_args)` — return `False` for the first
element and the Glue job is skipped entirely, which is how a client-only verb
works.

Give the client module a `help_text` string. `./bulk <verb> --help` prints it,
and it is the only user-facing documentation the verb gets. Copy the shape used
by `count.py` or `scancount.py`.

A verb whose behaviour is close to an existing one can delegate rather than
duplicate: `count.py` is a handful of lines that calls `find`'s `run` with
`verb="count"`.

## Deploying it — the step people miss

**A custom module has to be uploaded to S3 by `bootstrap` before a Glue job can
import it.** Dropping the file in the source tree is not enough; the Glue job
loads modules from the bucket, not from your disk. Skipping this is exactly what
produces `ModuleNotFoundError` mid-run.

```sh
cd tools/bulk_executor
./bulk bootstrap              # full bootstrap; deploys your module
./bulk <verb> --table t       # now importable
```

Whoever runs `bootstrap` needs the bootstrap-tier permissions, so on a shared
account the person with those permissions has to run it **with your module
present in their tree**.

While iterating, `--XDev` re-uploads the Python modules without a full
bootstrap:

```sh
./bulk <verb> --table t --XDev
```

That is the development loop. It is undocumented and unsupported, but it is what
makes writing a verb tolerable.

## Generators and transforms

Several built-ins take a module name rather than a verb:

- `fill --generator <mod>` — a `generate()` returning one item as a dict, or
  many as a list. `faker` is available on the workers and is the suggested way
  to make realistic data. Resolved as `python_modules.fill.<mod>`, and the
  modules that actually ship are **`default`, `users`, `nosk`, and
  `multi_entity_relationship`** (defaults to `default` when omitted).

  > Note: the README and `bulk fill --help` both show `--generator fakeusers`.
  > No such module exists — that example fails with `ModuleNotFoundError`. Use
  > `users` instead.

- `update --generator <mod>` — receives each item, returns update-expression
  kwargs, or empty when no update is needed. Resolved under the `update`
  folder; **`touched`** is the shipped example.
- `load-export --transform <mod>` / `revert-export --transform <mod>` — supply
  `transform_full_record` and/or `transform_incremental_record`.

These are deployed by `bootstrap` exactly like a verb, and they fail the same
way if they are not.

## Exception handling — the rule that matters

Where the exception is raised changes what you must do. Getting this wrong turns
one problem into thousands of stack traces.

**In driver code (the main control flow):**

- A genuine code defect — let it propagate. Glue handles it and the user gets a
  stack trace, which is the right outcome for a bug.
- A bad-user-input problem — `raise BulkExecutorError("reason") from None`.
  Execution halts cleanly with no stack trace. The `from None` suppresses the
  original exception when you are inside an `except` clause, which is what keeps
  the output readable.

**In executor code (running in parallel on the workers):**

**Do not raise.** Many workers hit the same condition at once and you get a
fireworks display of duplicate errors. Instead add a description to the
`error_accumulator` and return; then in the driver, find the first accumulated
error and raise it. The `fill` verb demonstrates the pattern — copy it.

## Two constraints to state plainly

- **The harness-to-script interface is not formalised.** Harness and scripts are
  expected to be co-developed and released together. A custom script is tied to
  the version of the tool it was written against.
- **Rate limiting is not optional.** Any code path that reads or writes DynamoDB
  must be bounded — either through the Glue connector's throughput options, or
  through the `python_modules.shared.rate_limiter` library
  (`RateLimiterSharedConfig`, `RateLimiterAggregator`, `RateLimiterWorker`) for
  direct boto3 access. An unbounded bulk scan or write is a defect even if
  another path in the same verb is limited. The repo enforces this by review, in
  `ai_lint/rules/commands_rate_limited.md`.

If the verb is generally useful, contribute it upstream — otherwise it will not
be kept current as the harness changes.

## Testing

`make test` is offline and fast (`awsglue`/`pyspark` are mocked); add unit tests
mirroring the source tree under `tests/client/` and `tests/server/`. The
`make test-e2e-*` suites hit real Glue and real DynamoDB, cost money, and are
opt-in — read `tests/e2e/AGENTS.md` before touching them.
