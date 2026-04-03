# Harness Chapter 1 Baseline

## Purpose

This document is the single checkpoint for Chapter 1 in this repository.

Use it to answer three questions:

- what Chapter 1 was trying to establish
- what has already been built into the current runtime baseline
- what later observations or small corrections should be appended without re-opening the whole chapter

## Chapter 1 Goal

Chapter 1 in this repo was treated as the initial Harness baseline for single-run execution.

Its practical goal was to make one run no longer depend on ad hoc local context or raw logs, but instead have:

- a bounded runtime environment
- a fixed task intent
- a usable feedback conclusion

In repo language, that means:

- environment is not scattered
- intent is not reconstructed on the fly
- feedback is not primarily read from raw logs

## What Was Optimized

### 1. Configuration Control Plane

Configuration resolution was pulled into `harness/config.py` so runners no longer rely on loosely repeated local parsing logic.

Current baseline:

- config sources are resolved through `cli / env_file / default / derived`
- `.env` is treated as a source, not the sole hard gate
- sensitive config provenance is retained without writing secret values into summaries

### 2. Run-Scoped Runtime Layout

Run paths were normalized through `harness/paths.py`.

Current baseline:

- each run has a stable `run_id`
- each run has a dedicated `run_root`
- each run has standard locations for `summary.json` and `task_spec.json`
- final runs also reserve standard child locations for upstream and downstream runs

### 3. Task Intent Was Frozen Into `task_spec`

Single-run intent is now written to `task_spec.json` instead of staying implicit in CLI arguments and ad hoc state.

Current baseline:

- final, upstream, downstream, and operator runs all write a run-scoped `task_spec.json`
- `task_spec` stores normalized intent, controls, environment provenance, and standard paths
- later stages can reason about the run using a real artifact instead of reconstructing the task from raw parameters

### 4. Execution Was Split Into `preflight / setup / runtime`

The run lifecycle is now explicitly layered instead of mixing eligibility checks, side effects, and real execution.

Current baseline:

- `preflight` performs static readiness checks
- `setup` materializes run directories and control artifacts
- `runtime` performs the actual execution
- failures now explicitly state which layer they belong to

### 5. Failure Handling Was Structured

Failure reporting was turned into a first-class control surface instead of only exposing raw exceptions.

Current baseline:

- failures are normalized into a structured `failure` payload
- failures are classified through `failure_decision`
- the current taxonomy includes `configuration`, `input`, `filesystem`, `dependency`, `runtime`, `orchestration`, and `external_runtime`
- failure handling now distinguishes between `manual_fix`, `manual_investigation`, and `auto_retry`

### 6. Top-Level Conclusions Were Added

Human and machine consumers no longer need to start from raw logs to decide what a run means.

Current baseline:

- every runner summary now exposes `verdict`
- `verdict` gives a top-level conclusion such as `completed`, `stopped`, `failed`, `blocked`, `running`, or `unknown`
- `failure_decision` gives the next-action hint

### 7. Critical Flag Semantics Were Fixed Through Real Observation

This chapter was not only shaped through tests; it was also adjusted using real CLI observations.

Current baseline:

- `--skip-scrape` is now treated as `staging-only / local observation run`
- when `--skip-scrape` is used, `vision probe` is skipped instead of blocking the run
- this semantic is now aligned across runtime behavior, tests, and CLI help text

## Current Chapter 1 Baseline

At this point, a single run in this repository already has the core Chapter 1 properties:

- run boundaries are explicit
- task intent is fixed through `task_spec`
- execution layers are separated
- failures are categorized
- verdicts are readable
- key entry semantics have been stabilized with real observation

This means Chapter 1 is considered phase-complete as a working baseline.

The strategy after this point is:

- treat Chapter 1 as the current production baseline
- validate it with real runs
- make only small corrective changes when observations expose drift
- do not keep expanding Chapter 1 control surfaces as the main line of work

## Canonical Supporting Files

The current Chapter 1 baseline is backed by these repo artifacts:

- `harness/config.py`
- `harness/paths.py`
- `harness/spec.py`
- `harness/preflight.py`
- `harness/setup.py`
- `harness/failures.py`
- `harness/contract.py`
- `harness/schemas/run-summary.schema.json`
- `harness/schemas/failure.schema.json`
- `docs/standards/harness-run-failure-standard.md`

## How To Append Future Notes

Future notes for this chapter should be appended here only when they are one of the following:

- a real run observation that confirms or challenges the current baseline
- a small semantic correction to an already-established Chapter 1 control surface
- a note explaining why a drift was accepted or fixed

Do not use this document for:

- large feature planning
- new chapter design work
- broad architecture brainstorming beyond the current baseline

## Suggested Append Format

Use a short dated section when adding future notes:

```md
## Observation YYYY-MM-DD

- scenario:
- expected:
- actual:
- impact on current baseline:
- decision:
```

## Status

Current status:

- Chapter 1 is treated as stabilized enough to serve as the baseline
- branch-wise, Chapter 1 should be treated as a Harness/foundation baseline rather than a long-lived chapter development line
- later chapter work should branch from `develop`, while shared execution or control-plane refactors can branch from a dedicated `feature/harness-*` line
- further work should move to real-run validation, integration discipline, and next-stage capability building
