# Phase 24: Add post-visual-review positioning-card analysis step for approved creators - Context

**Gathered:** 2026-03-30
**Status:** Ready for planning
**Source:** 用户明确要求在现有 visual review 之后新增“定位卡分析”步骤。这个步骤不是替代当前的 Pass/Reject 视觉审核，而是在通过视觉审核的达人上追加一层内容定位与品牌适配分析。

<domain>
## Phase Boundary

Phase 24 adds a new downstream analysis surface after visual review.

The repo already has:

- a repo-local single-entry path from `task upload` through `keep-list`
- a downstream screening runner that performs scrape -> prescreen -> visual review -> export
- Phase 23 scoped to wiring template-derived visual runtime contracts into existing visual review

What does not exist yet is a separate “定位卡分析” step that:

- runs only on creators who already passed visual review
- classifies creator positioning / content category fit instead of only returning `Pass/Reject`
- produces structured outputs that operators can inspect and potentially export

This phase therefore adds a new post-visual-review step. It is not a rework of the visual-review bugfix itself, and it must not silently expand Phase 23.

</domain>

<decisions>
## Implementation Decisions

### Step Placement
- 定位卡分析 should happen after visual review, not before.
- The minimum safe scope is: only creators with visual-review pass status enter the positioning-card step.
- Existing visual-review pass/reject semantics remain the gating contract; positioning analysis is an additional surface, not a replacement.

### Product Contract
- The new step should answer “这个达人是什么定位、为什么适合或不适合品牌”，rather than only “是否通过”.
- Minimum structured output should include:
  - positioning/category label(s)
  - fit summary or recommendation
  - concise evidence/signals
- The output should be machine-readable first, with operator-readable export/reporting layered on top.

### Runtime Integration
- The new step should plug into the existing repo-local pipeline surfaces rather than becoming a standalone sidecar script only usable from terminal memory.
- Preferred integration point is the keep-list downstream runner / final-export wrapper path, because that is already the canonical downstream boundary.
- The step should be explicitly stageable/skippable in runner summaries, so operators can see whether it ran, was skipped, or failed.

### Provider / Prompt Strategy
- The prompt strategy can reuse the repo’s existing OpenAI-compatible provider abstraction instead of inventing a new transport path.
- If provider-specific positioning prompts are needed, the design should preserve the same general pattern already used for visual prompts: platform/provider/model-aware resolution with clear fallbacks.
- The phase should not assume the external benchmark assets are available locally at planning time.

### External Benchmark Constraint
- The user referenced `/Users/tiancaijiaoshou/Downloads/11111/` as an existing benchmark/prompt source.
- That path is not present in the current environment at planning time, so this phase cannot rely on directly importing those files as a hard dependency.
- Planning should therefore treat external benchmark content as optional reference input, not as a blocker for architecture or first implementation.

### Scope Guardrails
- Do not reopen Phase 23’s visual runtime contract work as part of this phase.
- Do not require a large live run or a full benchmark migration to declare initial implementation complete.
- Do not let positioning-card analysis block current final export by default unless the new contract explicitly chooses blocking behavior.

### Claude's Discretion
- Exact artifact filenames and whether positioning analysis lands in JSONL, Excel columns, or both.
- Whether the first implementation is synchronous within the existing downstream run or a separately stageable sub-step inside the same runner.
- Exact schema of positioning labels and fit dimensions, as long as the output remains structured and operator-readable.

</decisions>

<specifics>
## Specific Ideas

- Treat the new step as “analysis after pass”, not “another screening gate”.
- Reuse existing downstream stage summaries so the new stage appears beside scrape / prescreen / visual review / export.
- Prefer leaving one bounded proof artifact under `temp/` plus regression coverage, instead of tying planning success to external prompt benchmark imports.
- If needed, define an explicit stage name such as `positioning_card_analysis` so summaries and exports do not overload `visual_review`.

</specifics>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone Continuity
- `.planning/PROJECT.md` — current pipeline boundary and explicit note that 定位卡分析 was out of scope for Phase 23
- `.planning/REQUIREMENTS.md` — current requirement inventory; new positioning-card requirements will likely need to be added or mapped
- `.planning/ROADMAP.md` — current phase ordering and dependency on Phase 23
- `.planning/STATE.md` — latest roadmap evolution and current in-progress phase state

### Existing Downstream Pipeline
- `scripts/run_keep_list_screening_pipeline.py` — canonical downstream runner and likely insertion point for the new stage
- `scripts/run_task_upload_to_final_export_pipeline.py` — top-level wrapper that will need to surface the new stage if it becomes part of final delivery
- `backend/app.py` — existing visual-review/runtime orchestration and provider abstraction
- `backend/screening.py` — existing profile-review data structures and export-facing runtime metadata

### Existing Visual Runtime Contract
- `.planning/phases/23-wire-template-compiled-visual-prompts-into-runtime-and-define-visual-feature-group-contract/23-CONTEXT.md` — what Phase 23 owns and what this phase must not re-scope
- `.planning/phases/23-wire-template-compiled-visual-prompts-into-runtime-and-define-visual-feature-group-contract/23-02-PLAN.md` — visual runtime rules-consumption plan already in flight
- `.planning/phases/23-wire-template-compiled-visual-prompts-into-runtime-and-define-visual-feature-group-contract/23-03-PLAN.md` — proof/documentation plan for the visual runtime contract

### Current User Intent
- User-described requirement in chat: 定位卡分析 is a separate step after approved creators pass visual review; benchmark path may exist externally but is not currently available in this environment

</canonical_refs>

<code_context>
## Existing Code Insights

### Current Downstream Contract Stops At Visual Review + Export
- `scripts/run_keep_list_screening_pipeline.py` stages inputs, runs downstream screening, and summarizes scrape / prescreen / visual review / export.
- There is no current repo-local stage named for positioning-card analysis.

### Current Backend Contract Is Built Around Screening Decisions
- `backend/app.py` and related tests focus on screening-style outputs such as pass/reject, provider selection, and visual diagnostics.
- A positioning-card step will likely need a new output contract rather than overloading the visual-review decision schema.

### Planning Must Tolerate Missing External Prompt Assets
- The user’s referenced external directory is not available in the current environment.
- Initial planning must therefore design for local prompts/contracts first, with optional later import/alignment of benchmark assets.

</code_context>

<deferred>
## Deferred Ideas

- Importing the entire external benchmark or prompt-tuning repo into this workspace
- Turning positioning-card analysis into a hard export blocker before the first version proves useful
- Expanding this phase into a general recommendation/ranking engine across all downstream stages

</deferred>

---

*Phase: 24-add-post-visual-review-positioning-card-analysis-step-for-approved-creators*
*Context gathered: 2026-03-30*
