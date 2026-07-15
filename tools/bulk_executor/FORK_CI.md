# FORK_CI.md — fork-only CI workflow (do NOT send upstream)

> **This file lives only on the `fork-ci` branch of the fork
> (`relentlesscol/amazon-dynamodb-tools`). It must never reach
> `awslabs/amazon-dynamodb-tools`.** If you are reading this on `main` or in an
> upstream PR, something bled — stop and remove it.

Operating manual for AI agents working on the fork's CI. Read this before
touching any `.github/workflows/` file or running e2e on the fork.

## The two-branch model

The fork has to be two things at once: a faithful **mirror** of upstream, and a
carrier of **fork-only** CI infrastructure that upstream will never accept
(real-AWS e2e, `E2E_AWS_*` secrets). Those fight, so they are split:

```
awslabs/main ──fast-forward──► fork/main   (PURE MIRROR — zero local commits)
                                   │  git merge main
                                   ▼
                                fork-ci     (mirror + fork-only CI; Actions run HERE)
```

- **`fork/main` is a pure mirror of `origin/main`.** It carries **no** local
  commits, ever. That keeps upstream sync a lossless fast-forward:
  `git fetch origin && git push fork origin/main:main`.
- **`fork-ci` branches off `fork/main`** and carries everything fork-only:
  - `.github/workflows/bulk-executor-ci.yml` — the e2e pipeline
    (unit → connector-smoke → command-e2e) against real Glue + DynamoDB.
  - this `FORK_CI.md`.
  - Node-24 action pins (`checkout@v5`, `setup-python@v6`).
  Refresh it after each upstream sync with `git merge main`. Only the workflow
  file and this doc diverge, so merges are trivial.

## Iron rules (violating these is how mainline gets polluted)

1. **Never commit anything to `fork/main`.** It is a mirror. One local commit
   and the next upstream sync stops being a fast-forward.
2. **Open upstream PRs from a dedicated branch cut off `fork/main`** (e.g.
   `ci/node24-action-bumps`) — **never from `main` and never from `fork-ci`.**
   A fork PR's head *is* a live branch: anything pushed to that branch after the
   PR opens is added to the PR. We learned this the hard way — PR #238 (head =
   `main`) silently absorbed the fork-only e2e workflow the instant it landed on
   `main`; it had to be closed and reopened as #239 from a dedicated branch.
3. **The e2e workflow triggers on `fork-ci` only** (`push`/`pull_request` on
   `[fork-ci]`), not `main`. If you retarget it to `main`, you reintroduce the
   collision in rule 2.
4. **`FORK_CI.md` never goes upstream.** It has no home in the mirror.

## Running e2e

The e2e jobs hit **real** Glue + DynamoDB and cost real money (~24 min for a
full run). They are gated to `relentlesscol/*` or `awslabs/*` and skipped on
`pull_request`. They depend on repo secrets: `E2E_AWS_ROLE_ARN` (OIDC),
`E2E_AWS_REGION`, `E2E_AWS_ACCOUNT_ID`, `E2E_READ_TABLE`, `E2E_WRITE_TABLE`.

To run: push to `fork-ci` (touching `tools/bulk_executor/**` or the workflow),
or `workflow_dispatch` the `bulk_executor CI` workflow on `fork-ci`.

## Syncing upstream (routine)

```sh
git fetch origin
git push fork origin/main:main          # lossless FF of the mirror
git fetch fork
git switch fork-ci && git merge main     # bring the mirror's new commits into CI
git push fork fork-ci
# then push fork-ci and let Actions run e2e against the freshly-synced tree
```

## Known open issue

The `github-actions-e2e-runner` OIDC role authenticates fine but the `count`
connector smoke failed with "no Glue job-run id in output — job never launched"
(failed in ~2s, before hitting Glue). Suspected missing Glue `StartJobRun`
permission or absent bootstrap in the CI account — **not** a code regression.
The synced upstream commits are therefore not yet CI-proven end-to-end.
