# Harness Run Failure Standard

## Purpose

This standard fixes the current single-run control plane at two levels:

- human-readable verdicts for operators and developers
- machine-readable run summaries for tooling, audit, and future automation

It does not change business rules. It only standardizes how a run reports what happened and what to do next.

## Current Failure Layers

Every terminal failure belongs to one `failure_layer`:

| failure_layer | meaning | typical examples |
| --- | --- | --- |
| `preflight` | static eligibility checks failed before execution | missing config, missing required input |
| `setup` | run root or task spec could not be materialized | directory creation failure, task spec write failure |
| `runtime` | execution started but a runtime step failed | import failure, scrape failure, child runner failure |

Rule:

- `preflight` only answers whether the run can start
- `setup` is the first side-effect layer
- `runtime` covers all post-setup execution failures

## Current Failure Categories

Each structured failure is classified into a `category`:

| category | meaning | default handling |
| --- | --- | --- |
| `configuration` | required config is missing or invalid | manual fix |
| `input` | upstream artifact, workbook, or user input is missing or unusable | manual fix |
| `filesystem` | path, permission, or local disk state prevents materialization | manual fix |
| `dependency` | local runtime dependency or import chain is broken | manual fix |
| `runtime` | run logic failed and needs inspection | manual investigation |
| `orchestration` | child run failed or handoff state is inconsistent | manual investigation |
| `external_runtime` | likely transient external or scraping issue | auto retry first |

## Resolution Mode

`resolution_mode` is the first recommended handling policy:

| resolution_mode | meaning | retry rule |
| --- | --- | --- |
| `manual_fix` | fix config, input, dependency, or filesystem state first | do not auto retry |
| `manual_investigation` | inspect summary, step outputs, and logs first | do not auto retry blindly |
| `auto_retry` | likely transient runtime issue | auto retry is allowed |

## Retryable And Manual Intervention Rules

Current decision policy:

- `retryable=true` only when `resolution_mode=auto_retry`
- `requires_manual_intervention=true` for `manual_fix` and `manual_investigation`
- `requires_manual_intervention=false` for `auto_retry`
- `configuration`, `input`, `filesystem`, and `dependency` failures should be treated as non-retryable until corrected
- `external_runtime` failures may be retried automatically once or a small bounded number of times before escalation
- `orchestration` and generic `runtime` failures should be investigated before retry because the next action depends on child summary or step state

## Summary Output Contract

Runner summaries now expose these standard fields:

| field | meaning |
| --- | --- |
| `contract_version` | run summary contract draft version |
| `failure_schema_version` | structured failure schema draft version |
| `failure` | full structured failure payload |
| `failure_decision` | condensed failure handling decision |
| `verdict` | single operator-facing conclusion for the whole run |

`failure` is the full payload.

`failure_decision` is the compact handling slice.

`verdict` is the canonical top-level conclusion a person or machine should read first.

## Verdict Semantics

`verdict.outcome` is one of:

- `completed`
- `stopped`
- `failed`
- `blocked`
- `running`
- `unknown`

Interpretation:

- `completed`: terminal success, outputs can be consumed
- `stopped`: intentional stop such as `stop_after=*`, resume point should be used
- `failed`: terminal failure
- `blocked`: terminal failure caused by an unmet input boundary, such as missing profiles
- `running`: non-terminal in-progress summary
- `unknown`: summary exists but current state still needs manual interpretation

## Machine-Readable Drafts

Current schema drafts live at:

- `/Users/tiancaijiaoshou/Desktop/Coding/global-creator-screening/harness/schemas/run-summary.schema.json`
- `/Users/tiancaijiaoshou/Desktop/Coding/global-creator-screening/harness/schemas/failure.schema.json`

Design intent:

- `run-summary.schema.json` fixes the outer run contract while allowing runner-specific extension fields
- `failure.schema.json` fixes the structured failure payload and the retry/manual-decision fields

## Non-Goals In This Round

- no scoped runtime refactor
- no scheduler or queue layer
- no heavy autonomy or agent orchestration
- no rewrite of business logic or runner step semantics
