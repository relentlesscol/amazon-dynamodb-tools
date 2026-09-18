# Every `--X` flag

Taken from the CLI's own argument parsers. All attach to any command unless
noted.

## Glue execution

| Flag | Type | Default | Effect |
|---|---|---|---|
| `--XExecutionClass` | `STANDARD` \| `FLEX` | `STANDARD` | FLEX uses spare capacity at lower DPU price; measured slower and more DPU-hungry in practice. |
| `--XNumberOfWorkers` | int | 220 | Worker count. Lower it for small tables — biggest small-job cost lever. |
| `--XWorkerType` | `G.1X` `G.2X` `G.4X` `G.8X` `G.12X` `G.16X` `R.1X` `R.2X` `R.4X` `R.8X` | `G.1X` | Per-worker compute/memory/storage. Raise for large local sorts (`--orderby`). |
| `--XTimeout` | int (minutes) | 60 | Job timeout. Range 1–10080 (7 days). |
| `--XWaitForDPU` | flag | off | Wait 40s at the end so DPU-hour metrics arrive and get reported. |
| `--XContinuousLogging` | flag | off | Use default Glue continuous logging instead of Log4J. |

## DynamoDB throughput

| Flag | Type | Default | Effect |
|---|---|---|---|
| `--XMaxReadRate` | int (RCU/s) | derived from table | Max read units/sec consumed. Per table for `diff`. |
| `--XMaxWriteRate` | int (WCU/s) | derived from table | Max write units/sec consumed. |

Effective floor is around 200 regardless of a lower setting, because each Glue
shard needs a minimum allowance. Expect oscillation before steady state.

## Environment

| Flag | Type | Default | Effect |
|---|---|---|---|
| `--XAccount` | string | from environment | AWS account for the Glue job. |
| `--XRegion` | string | from environment | Region for the Glue job. **Also decides which table gets validated.** A full table ARN overrides it. |

## Bootstrap only

| Flag | Type | Default | Effect |
|---|---|---|---|
| `--XRole` | `READ-ONLY` \| `READ-WRITE` \| role name | interactive prompt | Glue execution role. A custom name must begin with `AWSGlueServiceRole`. |
| `--XMaxConcurrentRuns` | int | 20 | Max concurrent runs of the single shared Glue job. |
| `--XRetries` | int | 0 | Glue job retries. Zero on purpose, so a misconfigured job fails fast. |

## Undocumented but present

`--XDebug` enables debug logging. `--XDev` re-uploads the Python modules to S3
without a full bootstrap — useful when developing a custom verb, and covered by
the `bulk-custom-verbs` skill. Both are hidden from `--help` and are not part of
the supported surface.
