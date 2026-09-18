---
name: bulk-tuning
description: |
  Tune the cost, speed, and throughput of a Bulk Executor run: Glue worker
  count and type, FLEX vs STANDARD execution, job timeout, and the
  --XMaxReadRate / --XMaxWriteRate caps that bound DynamoDB capacity consumed.
  Use when a bulk command is too slow, too expensive, times out, throttles a
  table, or runs out of memory on a sort.
  Triggers on: "bulk executor is slow", "make bulk cheaper", "reduce glue
  cost", "bulk job timed out", "how many workers", "XNumberOfWorkers",
  "XWorkerType", "XMaxWriteRate", "rate limit the bulk job", "bulk job
  throttling my table", "out of memory on orderby".
license: Apache-2.0
metadata:
  tags: dynamodb, aws, glue, cost, performance, throughput, tuning
---

# Tuning a Bulk Executor run

All tuning flags are prefixed `--X` to separate them from a command's own
parameters, and they attach to any command. Run from `tools/bulk_executor/`.

## Cost: the default is sized for big tables

**The default is 220 Glue workers.** On a small table the Glue cost dominates
the DynamoDB cost, so the highest-leverage change by far is fewer workers:

```sh
./bulk count --table small-table --XNumberOfWorkers 10
```

Each run prints a DynamoDB cost estimate before doing work and the user can
Ctrl-C. The estimate covers DynamoDB only — Glue cost depends on DPU-hours,
which cannot be predicted in advance. Add `--XWaitForDPU` to make the run wait
40 seconds at the end and report actual DPU-hours consumed, which is how you
learn what a job shape really costs.

## FLEX is usually the wrong lever

`--XExecutionClass FLEX` runs on spare capacity at a lower DPU price. It is
tempting and often counterproductive: on a 1.7-billion-item count, the
project's own measurements were

| Execution class | Duration | DPU-hours |
|---|---|---|
| `STANDARD` (default) | 0:05:31 | 3.93 |
| `FLEX` | 0:11:34 | 7.57 |

FLEX took twice as long *and* burned twice the DPUs. Reach for
`--XNumberOfWorkers` before FLEX.

## Worker type: raise it for sorts, not for throughput

`--XWorkerType` accepts `G.1X` (default), `G.2X`, `G.4X`, `G.8X`, `G.12X`,
`G.16X`, `R.1X`, `R.2X`, `R.4X`, `R.8X`.

A bigger worker gives one worker more compute, memory, and local storage. That
matters when a single worker has to gather a large set locally — the classic
case is `--orderby` over many matching items, which is also the classic
out-of-memory failure. If a sort dies, raise the worker *type* before raising
the worker *count*.

For an out-of-memory failure specifically, the tool's own advice is **`R.1X`,
which has double the heap of the default `G.1X`** — the `R.` family is
memory-optimised, so `R.1X` beats `G.2X` for a heap problem at a similar size.

## Throughput caps

`--XMaxReadRate` and `--XMaxWriteRate` bound the read/write units per second
consumed against a table. Left unset, the tool inspects the table and derives a
ceiling from its current capacity.

```sh
./bulk load --table t --format csv --XMaxWriteRate 40000
```

Behaviour worth knowing before you promise a number:

- It is a **maximum, not a target**. The run may not reach it.
- **Very low values do not work as expected.** Below roughly 200, the effective
  floor stays near 200, because Glue runs many shards and each needs a minimum
  allowance.
- Expect **a few minutes of oscillation** before the rate settles.
- For `diff`, which reads two tables, the cap applies **per table**.

## Timeout, and the warning that predicts it

`--XTimeout` is in minutes, default 60, max 10080 (7 days).

At the start of a run the tool compares the effective rate against the table's
size: if moving that much data at that rate would exceed the timeout, it logs a
warning that the job will likely time out. **The warning is observational and
never blocks the run** — read it and act. It fires whether the rate was set
explicitly or derived. You also get warned when a rate is below the recommended
minimum, or above what the table can actually deliver given its
provisioned/autoscaling/on-demand ceiling.

So a timeout is usually a rate problem, not a timeout problem: raise
`--XMaxReadRate`/`--XMaxWriteRate`, or raise `--XTimeout`, or narrow the work.

## Full flag reference

`references/glue-parameters.md` lists every `--X` flag with its default.

## Worked example

A large sorted delete, tuned deliberately: fewer but larger workers so the sort
fits, and DPU reporting on so the cost is measurable next time.

```sh
./bulk delete --table t --orderby timestamp --limit 10000000 \
  --XNumberOfWorkers 100 --XWorkerType G.4X \
  --XWaitForDPU
```
