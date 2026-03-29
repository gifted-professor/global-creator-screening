# Phase 21: Replace workbook/dashboard/project-home runtime paths with repo-local implementations - Context

**Gathered:** 2026-03-29
**Status:** Ready for planning
**Source:** User confirmed the real target is "当前仓库单目录独立跑通", so future deployment, automation, and operator use do not depend on a sibling full `email` project. A thin future single-entry backend/UI is useful, but only after this decoupled runtime ownership is stable.

<domain>
## Phase Boundary

Phase 21 is the implementation phase for `DEP-01/02/03`.

The repo already has a proven repo-local mainline:

- `task upload -> keep-list`
- `keep-list -> final export`
- a thin wrapper proving bounded `task upload -> final export`

What is still missing is runtime ownership for the leftover legacy workbook / dashboard / project-home surfaces. Today those surfaces still terminate in the external full `email` project's DB, read-model rebuilds, and dashboard export helpers.

Phase 21 therefore does not re-prove the whole pipeline and does not invent a new orchestration product surface. It replaces the remaining legacy runtime paths so those user workflows resolve inside the current repository by default.

</domain>

<decisions>
## Implementation Decisions

### Legacy Command Treatment
- `python3 -m feishu_screening_bridge import-from-feishu` and `python3 -m feishu_screening_bridge sync-task-upload-view` should stop being "legacy bridge first" commands.
- Keep their operator-facing CLI surfaces if that reduces handoff friction, but make their default execution repo-local.
- External `--email-project-root` / `EMAIL_PROJECT_ROOT` may remain only as explicit compatibility mode; the intended path for this milestone must not require them.
- If exact legacy behavior is expensive to preserve, prefer repo-local replacement or explicit deprecation over silently keeping external runtime ownership.

### Workbook Replacement Strategy
- Reuse the existing Feishu download, workbook parsing, and flexible compile logic already in `feishu_screening_bridge/task_upload_sync.py`.
- Repo-local workbook import should write into current-repo-owned artifacts and runtime state, not into the external `email` project's DB.
- For single-workbook import, converge on the same repo-local compile/persist helpers used by task-upload-driven sync, instead of keeping a separate legacy-only bridge path.
- The output contract should expose deterministic local paths for downloaded workbook, parsed template artifacts, and any repo-local runtime files it updates.

### Dashboard Replacement Strategy
- Do not preserve the old external `exports/index.html` dependency as the default success surface.
- Treat "dashboard" in this phase as a repo-local operator visibility surface: machine-readable summary first, optional local HTML or backend-backed page second.
- Prefer reusing existing repo-local backend export/state surfaces and runner summary conventions instead of recreating the sibling project's dashboard stack byte-for-byte.
- Any human-readable output should be generated from repo-local state owned by this repository.

### Project-Home / Workbench Replacement Strategy
- Do not reintroduce the external `project_home` / `project_workbench` read-model rebuild pipeline as a hidden requirement.
- Repo-local replacement may be thinner than the old stack: a stable manifest / summary / derived runtime state is acceptable if it covers the operator workflow that currently depends on those commands.
- The replacement should capture enough metadata to support future automation and a later single-entry orchestration API:
  - task name / project code
  - downloaded workbook path
  - parsed template / rulespec outputs
  - resolved repo-local runtime locations
  - latest generated artifacts or operator next step

### Output And Path Contract
- All default paths must resolve from the current repo, explicit CLI flags, or explicit env files in the current repo.
- No hard-coded machine paths, no sibling checkout assumptions, and no requirement to source a second project's `.env`.
- Return payloads and CLI summaries should explicitly show the repo-local paths they materialized so operators can verify what changed without guessing.

### Verification Target For This Phase
- Phase 21 should add or update targeted tests for repo-local command routing and artifact ownership changes.
- Legacy external-project integration coverage remains optional and gated behind `CHUHAI_LEGACY_EMAIL_PROJECT_ROOT`.
- Full bounded regression proof belongs to Phase 22, not Phase 21.

### Claude's Discretion
- Exact helper/module extraction between `feishu_screening_bridge` and repo-local backend/script helpers.
- Exact manifest filename and summary shape, as long as it is machine-readable and repo-local.
- Whether some legacy command names remain as thin repo-local wrappers or become explicit deprecation shims that point to canonical repo-local runners.

</decisions>

<specifics>
## Specific Ideas

- The milestone success test is still the user's wording:
  - "当前仓库单目录独立跑通，不再绑着外部 `email` 项目活着。"
- This phase should optimize for future deployment and automation, not just one developer's laptop ergonomics.
- A future "输入任务名就开始执行" thin backend/UI is explicitly useful, but it should be built on top of the repo-local ownership contract delivered here, not mixed into this phase.
- The committed mainline remains:
  - `scripts/run_task_upload_to_keep_list_pipeline.py`
  - `scripts/run_keep_list_screening_pipeline.py`
  - `scripts/run_task_upload_to_final_export_pipeline.py`

</specifics>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone Continuity
- `.planning/PROJECT.md` — current milestone framing and the self-contained repo goal
- `.planning/REQUIREMENTS.md` — `DEP-01/02/03` definitions and out-of-scope guardrails
- `.planning/ROADMAP.md` — Phase 21 goal, success criteria, and planned split
- `.planning/STATE.md` — current position and accumulated decisions
- `.planning/phases/20-baseline-legacy-dependency-surfaces-and-lock-repo-local-replacement-contract/20-CONTEXT.md` — fixed replacement contract and scope boundary from the previous phase
- `.planning/phases/20-baseline-legacy-dependency-surfaces-and-lock-repo-local-replacement-contract/20-LEGACY-DEPENDENCY-INVENTORY.md` — exact legacy surfaces to replace in this phase

### Current Repo-Local Mainline
- `README.md` — operator path, bounded-proof limitations, and developer handoff guidance
- `scripts/run_task_upload_to_keep_list_pipeline.py` — canonical upstream runner
- `scripts/run_keep_list_screening_pipeline.py` — canonical downstream runner
- `scripts/run_task_upload_to_final_export_pipeline.py` — canonical top-level wrapper with `keep-list` as internal boundary
- `scripts/prepare_screening_inputs.py` — existing repo-local staging contract for creator workbook/template -> backend runtime
- `backend/app.py` — repo-local upload parsing, rulespec persistence, runtime exports, and job state surfaces

### Phase 21 Code Surfaces
- `feishu_screening_bridge/__main__.py` — legacy command entrypoints whose default execution needs to become repo-local
- `feishu_screening_bridge/bridge.py` — single-workbook legacy bridge currently bound to external project-home workflow
- `feishu_screening_bridge/task_upload_sync.py` — task-upload bulk sync path and flexible workbook compile logic
- `feishu_screening_bridge/email_project.py` — explicit compatibility boundary; external loader should stop being the default owner of these workflows
- `tests/test_main_cli.py` — CLI contract tests that should evolve with repo-local routing
- `tests/test_feishu_screening_bridge.py` — unit coverage for bridge behavior and optional legacy integration gating

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `feishu_screening_bridge.task_upload_sync.inspect_task_upload_assignments(...)` already downloads templates and can emit parsed template artifacts without touching the external project.
- `feishu_screening_bridge.task_upload_sync._compile_workbook_for_import(...)` and `_compile_flexible_screening_workbook(...)` already encapsulate workbook -> compiled-row behavior that can be reused in a repo-local replacement.
- `scripts.prepare_screening_inputs.prepare_screening_inputs(...)` already owns a repo-local pattern for task/template/rulespec inputs landing in backend runtime state.
- `backend.app` already persists repo-local upload metadata and active rulespecs, and already produces machine-readable job/export outputs.

### Established Patterns
- This repo prefers machine-readable summaries, explicit resolved-input reporting, and deterministic repo-local artifact paths.
- `keep-list` remains the canonical internal boundary; Phase 21 must not move or blur it.
- Legacy compatibility coverage is already opt-in through `CHUHAI_LEGACY_EMAIL_PROJECT_ROOT`, so new default behavior can be tested without depending on the sibling repo.

### Gaps The Planner Should Assume
- There is currently no repo-local equivalent of the external `project_home` / `project_workbench` modules inside `email_sync`.
- `feishu_screening_bridge/bridge.py` still centers on `build_screening_workbook_upload_bridge_payload(...)`, which is an external-project abstraction and should not remain the primary path.
- `sync_task_upload_view_to_email_project(...)` still encodes the old compile -> import requirements -> rebuild read models -> export dashboard contract; Phase 21 needs to replace that ownership model instead of merely renaming it.

### Integration Points
- The most likely implementation seam is: keep Feishu fetch/parse in `feishu_screening_bridge`, then call repo-local persistence helpers that write backend/runtime artifacts owned by this repo.
- CLI output and JSON payloads should align with the summary style already used by the repo-local runners so operators see resolved inputs, output paths, and next-step hints consistently.
- Any repo-local dashboard replacement should be derived from data already present in current runtime files or generated during the new repo-local sync/import flow.

</code_context>

<deferred>
## Deferred Ideas

- Task-name single-entry orchestration API and any thin operator UI on top of it
- Full result write-back to Feishu as a formal mainline contract
- `backend/app.py` modularization, shared settings unification, shared `pipeline_runtime.py`, SQLite WAL/FTS, upload/job hardening, LLM config consolidation, and packaging/tooling cleanup
- Large-sample or multi-platform regression proof beyond the bounded validation reserved for Phase 22

</deferred>

---

*Phase: 21-replace-workbook-dashboard-project-home-runtime-paths-with-repo-local-implementations*
*Context gathered: 2026-03-29*
