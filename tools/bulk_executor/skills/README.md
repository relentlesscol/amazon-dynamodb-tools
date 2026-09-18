# Bulk Executor — agent skill library

An agent skill library for Bulk Executor. One skill per command, plus four
cross-cutting helpers. Ask in plain language — "how many items in orders match
status pending", "backfill a field on every item", "why did my bulk job get
AccessDenied" — and the matching skill loads itself.

Skills follow the [Agent Skills specification](https://agentskills.io/specification),
so the library is **tool-portable**: `skills/` is a single shared source of truth
and each agentic tool gets a thin manifest beside it. Claude Code and Codex
manifests ship today; see [Supporting another tool](#supporting-another-tool).

The plugin is **documentation only**. It adds no code to Bulk Executor, changes
no behaviour, and has no runtime dependency on Claude Code. Nothing here affects
the CLI.

## Install

**Claude Code:**

```
/plugin marketplace add awslabs/amazon-dynamodb-tools
/plugin install dynamodb-bulk-executor@amazon-dynamodb-tools
```

**Codex:** the manifest is at `.codex-plugin/plugin.json`; install per Codex's
plugin instructions pointing at `tools/bulk_executor`.

To test an unmerged branch, add the checkout by path instead:

```
/plugin marketplace add /path/to/amazon-dynamodb-tools
/plugin install dynamodb-bulk-executor@amazon-dynamodb-tools
```

Verify what loaded:

```sh
claude plugin validate tools/bulk_executor --strict
claude plugin details dynamodb-bulk-executor
```

## What's in it

**Commands** — one skill per verb, named after it:

| Skill | Command |
|---|---|
| `bulk-bootstrap` | one-time per-account/region setup; also deploys custom modules |
| `bulk-teardown` | remove the Glue job, role and bucket |
| `bulk-count` | count items, optionally matching a Spark SQL predicate |
| `bulk-find` | find and print items; sort by any attribute with no GSI |
| `bulk-scancount` | fast count by parallel scan; skew reporting and sampled estimates |
| `bulk-sql` | arbitrary Spark SQL — aggregates, `GROUP BY`, windows |
| `bulk-update` | modify items via a generator module (backfills, migrations) |
| `bulk-delete` | delete by predicate, or the N oldest/newest |
| `bulk-fill` | generate synthetic test data |
| `bulk-copy` | copy a table, including cross-region and cross-account |
| `bulk-diff` | compare two tables; verify a restore or migration |
| `bulk-load` | load CSV / JSON / Parquet from S3 |
| `bulk-load-export` | load a DynamoDB export, with optional PII transforms |
| `bulk-revert-export` | undo an incremental export's writes |

**Helpers:**

| Skill | Covers |
|---|---|
| `bulk-executor-setup` | prerequisites, readiness checks, PITR, the CloudShell/VPC-endpoint trap. Start here |
| `bulk-tuning` | worker count and type, FLEX, timeouts, `--XMaxReadRate` / `--XMaxWriteRate` |
| `bulk-troubleshooting` | the four failures the tool self-diagnoses, plus hangs and refusals |
| `bulk-custom-verbs` | writing a verb, generator or transform, and the exception rules |

## Design notes

- **Grounded in the source, not the prose.** Flags come from the argument
  parsers, error text from `UNHEALTHY_STATE_LOG_SIGNALS`, generator names from
  the modules that actually exist. Where the README and the code disagree, the
  skills follow the code and say so.
- **Cost and safety are stated up front.** Every command runs a real Glue job and
  consumes DynamoDB capacity; the mutating ones require PITR. Skills surface the
  pre-run cost estimate and the Ctrl-C escape rather than hiding them, and the
  destructive verbs push a `count`/`find` preview or a copy-and-diff rehearsal
  first.
- **Progressive disclosure.** Each `SKILL.md` stays short; detail lives in
  `references/`, loaded only when needed.
- **`./bulk` runs from `tools/bulk_executor/`.** It resolves `client/src/`,
  `HELP.md` and its upload paths relative to the working directory, so every
  skill `cd`s there first.

## Supporting another tool

The layout deliberately separates portable content from per-tool packaging, the
same way [`awslabs/agent-plugins`](https://github.com/awslabs/agent-plugins)
does — every plugin there ships a `.codex-plugin/` alongside its
`.claude-plugin/`, over one shared `skills/` tree.

```
tools/bulk_executor/
├── .claude-plugin/plugin.json    # Claude Code manifest
├── .codex-plugin/plugin.json     # Codex manifest
└── skills/                       # shared, spec-compliant — the source of truth
    └── <skill>/
        ├── SKILL.md              # portable: name + description frontmatter
        └── references/           # portable detail, loaded on demand
```

**To add a tool, add a manifest — do not fork `skills/`.** Two things to know:

1. **Manifests are near-identical but not interchangeable.** Claude's
   deliberately **omits** a `skills` key, because for Claude that key *adds to*
   the default `skills/` scan and declaring it double-registers every skill.
   Codex's declares `skills: "./skills/"` explicitly and adds an `interface`
   block (`displayName`, `defaultPrompt`, `capabilities`, …) that Claude has no
   equivalent for. Copy the convention from the tool's own docs rather than
   assuming symmetry.

2. **Genuinely tool-specific instructions belong in a `platforms/` file inside
   the relevant skill, not in a forked copy of it.** That is the AWS pattern —
   `skills/dsql/mcp/platforms/{claude-code,codex,gemini,kiro}.md` in
   `databases-on-aws`. If a tool needs different invocation or setup steps, add
   `skills/<skill>/references/platforms/<tool>.md` and link it from `SKILL.md`.

Keep frontmatter to spec-defined fields only — `name`, `description`, `license`,
`compatibility`, `metadata` — since anything else is not guaranteed to travel.
The skills here already hold to that.

**Kiro** reads `AGENTS.md` conventions rather than a plugin manifest, so it needs
a `platforms/kiro.md` and a pointer from this package's `AGENTS.md` rather than a
third manifest directory. That work is unstarted and is best done by someone
driving Kiro directly — the seam above is where it goes.

## Documentation bugs these skills work around

Found while grounding the skills in the source. Each is a real defect in the
existing docs, worth fixing separately:

| Where | Problem |
|---|---|
| `README.md` + `bulk fill --help` | Both show `--generator fakeusers`. No such module exists — the example fails with `ModuleNotFoundError`. Shipped generators are `default`, `users`, `nosk`, `multi_entity_relationship`. |
| `bulk load --help` | Omits `--s3-path`, which is **required**. Its example `bulk load --table orders --format csv` therefore fails. |
| `bulk load --help` | Omits `--withoutHeader`, which exists. |
| `bulk sql` docs | Do not mention that the Spark view name replaces `-` and `.` with `_`, so a hyphenated table must be referenced as `my_table`, nor that only `SELECT` is accepted (no CTEs). |
