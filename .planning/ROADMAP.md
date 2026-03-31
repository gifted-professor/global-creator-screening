# Roadmap: chuhaihai

## Milestones

- ✅ **v1.0.0 Consolidated Local Creator Screening Pipeline** — Phases 1-13 shipped 2026-03-28. Archive: `.planning/milestones/v1.0.0-ROADMAP.md`
- ✅ **v1.1.0 Visual Provider Reliability and Downstream Hardening** — Phases 14-15 shipped 2026-03-28. Archive: `.planning/milestones/v1.1.0-ROADMAP.md`
- ✅ **v1.2.0 End-to-End Single-Entry Pipeline Verification** — Phases 16-19 shipped 2026-03-29. Archive: `.planning/milestones/v1.2.0-ROADMAP.md`
- 🚧 **v1.3.0 External Email Dependency Decoupling** — Phases 20-25 implemented; gap-closure Phases 26-29 planned before milestone closeout

## Overview

`v1.3.0` 先聚焦 `DEP-01`：把 workbook / dashboard / project-home 对 external full `email` 项目的剩余耦合从运行主线里拆掉，确保 operator 在当前仓库里就能完成关键入口流程。`QTE-01` 与 `REL-01` 保留为后续里程碑，避免本轮把 dependency removal、数据接入和大样本稳定性验证混成同一交付面。

`Phase 23` 和 `Phase 24` 是在 decoupling proof 之后补录的 repo-local follow-up：前者把模板编译出的品牌视觉 prompt / feature contract 真正接回 runtime，后者把 visual-pass 之后的定位卡分析接入 downstream / final-wrapper 可观察 contract。2026-03-30 用户又把原本 deferred 的“本地薄 operator UI”提前拉进当前 roadmap，作为 local-only follow-up，不改写主 runner，只在现有 Flask/backend 和 single-entry pipeline 之上补一个控制页。

## Phases

- [x] **Phase 20: Baseline legacy dependency surfaces and lock repo-local replacement contract** - 盘点 workbook / dashboard / project-home 的外部依赖触点并定义统一替换规则
- [x] **Phase 21: Replace workbook/dashboard/project-home runtime paths with repo-local implementations** - 把核心入口从 external full `email` 依赖切到当前仓库
- [x] **Phase 22: Validate decoupled runtime stability and operator fallback contract** - 回归主链并收口兼容回退说明，确保迁移可安全落地
- [x] **Phase 23: Wire template-compiled visual prompts into runtime and define visual feature-group contract** - 把模板编译出的品牌视觉 prompt 接回 runtime，并定义 `visual_feature_group` 的消费契约
- [x] **Phase 24: Add post-visual-review positioning-card analysis step for approved creators** - 在 visual review 之后增加 repo-local 定位卡分析，并把结构化输出接入 downstream / final wrapper summary
- [x] **Phase 25: Build local thin operator UI for task-driven screening runs** - 提供本地可视化控制页，读取飞书任务、触发现有 task-driven runner，并展示 summary / 导出产物
- [ ] **Phase 26: Backfill decoupling verification bundle for Phases 20-21** - 把已实现的 repo-local decoupling 结果补齐成可审计的 SUMMARY / VERIFICATION / traceability 证据链
- [ ] **Phase 27: Reconstruct runtime safety proof and fallback certification for Phase 22** - 把 decoupled bounded regression 与 fallback contract 整理成正式的 Phase 22 planning / verification bundle
- [ ] **Phase 28: Reconstruct visual runtime and positioning evidence chain for Phases 23-24** - 为 visual prompt contract 与 positioning-card analysis 补建缺失 phase artifacts，并补齐跨阶段验证链
- [ ] **Phase 29: Reconstruct operator UI evidence bundle and milestone closeout consistency** - 为 operator UI 补建 Phase 25 planning bundle，并把 roadmap / state / audit closeout 口径收口一致

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 20. Baseline legacy dependency surfaces and lock repo-local replacement contract | 2/2 | Completed | 2026-03-29 |
| 21. Replace workbook/dashboard/project-home runtime paths with repo-local implementations | 2/2 | Completed | 2026-03-29 |
| 22. Validate decoupled runtime stability and operator fallback contract | 2/2 | Completed | 2026-03-29 |
| 23. Wire template-compiled visual prompts into runtime and define visual feature-group contract | 3/3 | Completed | 2026-03-30 |
| 24. Add post-visual-review positioning-card analysis step for approved creators | 2/2 | Completed | 2026-03-30 |
| 25. Build local thin operator UI for task-driven screening runs | 2/2 | Completed | 2026-03-30 |
| 26. Backfill decoupling verification bundle for Phases 20-21 | 0/2 | Planned | Pending |
| 27. Reconstruct runtime safety proof and fallback certification for Phase 22 | 0/2 | Planned | Pending |
| 28. Reconstruct visual runtime and positioning evidence chain for Phases 23-24 | 0/3 | Planned | Pending |
| 29. Reconstruct operator UI evidence bundle and milestone closeout consistency | 0/2 | Planned | Pending |

### Phase 20: Baseline legacy dependency surfaces and lock repo-local replacement contract

**Goal**: 明确 external full `email` 的残余触点与行为依赖，形成 repo-local replacement contract 和迁移边界。
**Depends on**: Phase 19
**Requirements**: [DEP-04]
**Success Criteria** (what must be TRUE):
  1. workbook / dashboard / project-home 三条入口的 external 依赖触点被逐项列出，且每项都有对应的 repo-local replacement strategy
  2. 命令入口的 early diagnostics 统一为 repo-local contract，不再要求 operator 推断外部路径
  3. 迁移边界和不在本轮处理的能力被明确记录，避免 Phase 21 范围漂移
**Plans**: 2 plans

Plans:
- [x] 20-01: Inventory external dependency surfaces and runtime callsites across workbook/dashboard/project-home
- [x] 20-02: Define replacement contract and update operator diagnostics/remediation language for repo-local execution

### Phase 21: Replace workbook/dashboard/project-home runtime paths with repo-local implementations

**Goal**: 将 workbook / dashboard / project-home 入口改为默认依赖当前仓库运行资源，去除 external full `email` 的硬依赖。
**Depends on**: Phase 20
**Requirements**: [DEP-01, DEP-02, DEP-03]
**Success Criteria** (what must be TRUE):
  1. workbook 相关入口在当前仓库可独立运行，不需要 external full `email` 项目
  2. dashboard 相关入口在当前仓库可独立运行，不需要 external full `email` 项目
  3. project-home 相关入口在当前仓库可独立运行，不需要 external full `email` 项目
**Plans**: 2 plans

Plans:
- [x] 21-01: Implement repo-local dependency replacement for workbook and dashboard runtime paths
- [x] 21-02: Implement repo-local dependency replacement for project-home runtime paths and remove hard-coded external fallbacks

### Phase 22: Validate decoupled runtime stability and operator fallback contract

**Goal**: 对 decoupled 结果做 bounded 回归验证并补齐 operator 兼容/回退指引，确保切换可恢复、可维护。
**Depends on**: Phase 21
**Requirements**: [SAF-01, SAF-02]
**Success Criteria** (what must be TRUE):
  1. `task upload -> final export` bounded run 在 decoupling 后保持可运行且关键 summary contract 不回退
  2. workbook / dashboard / project-home 入口的运行文档与故障排障路径和代码行为一致
  3. 提供明确兼容/回退步骤（配置或文档化流程），operator 可在异常时快速回到可用状态
**Plans**: 2 plans

Plans:
- [x] 22-01: Execute bounded regression validation on decoupled runtime and capture proof artifacts
- [x] 22-02: Finalize operator runbook for fallback/recovery and align planning docs with observed behavior

**Details:**
Phase 22 已完成。回归 suite `python3 -m unittest tests.test_main_cli tests.test_feishu_screening_bridge tests.test_task_upload_to_keep_list_pipeline tests.test_task_upload_to_final_export_pipeline -v` 在 decoupling 后通过（`52 tests`, `OK (skipped=3)`）。fresh bounded wrapper run 留在 `temp/phase22_decoupled_bounded_validation`：上游 fresh 跑到了 keep-list，top-level 失败点是外部 `openai` vision probe 返回 `HTTP 503 No available channel for model gpt-5.4 under group default (distributor)`。这轮 run 同时证明了 operator fallback contract 已可用，因为 top-level summary 暴露了 `resume_points.keep_list.recommended_command`，而 downstream 从 keep-list 切到 `qiandao` 后在 `temp/phase22_keep_list_resume_qiandao` 完成恢复并产出 `instagram_final_review.xlsx`。当前 milestone 的交付面已经完整，下一步应转入 audit / closeout，而不是继续扩 scope。

### Phase 23: Wire template-compiled visual prompts into runtime and define visual feature-group contract

**Goal:** 把模板编译出的品牌视觉 prompt / feature contract 真正接回 visual-review runtime，确保视觉复核不再对所有品牌复用同一套通用规则，并保留现有 provider-specific fallback 能力。
**Requirements**: [VIS-01, VIS-02, VIS-03]
**Depends on:** Phase 22
**Success Criteria** (what must be TRUE):
  1. `prepare_screening_inputs` / smoke compile path 会把 `visual_prompts.json` 持久化为 runtime active artifact，`build_visual_review_prompt()` 对有模板场景优先使用品牌/平台 prompt，并对无模板场景继续回退现有通用常量
  2. `active_rulespec.rules` 中的 `visual_feature_group` 与支持的视觉排除项在 runtime 有实际消费面，至少影响 cover limit、prompt fallback 或等价 diagnostics，而不是只停留在编译 JSON
  3. regression tests 和一次 repo-local proof 能证明更换模板会改变 runtime visual contract，同时不破坏 `mimo` / `qwen-vl-max` / generic fallback 分支
**Plans:** 3 plans

Plans:
- [x] 23-01: Persist compiled visual prompt bundles into active runtime and resolve brand/provider prompt selection
- [x] 23-02: Consume `visual_feature_group` / supported visual exclusions in runtime cover selection and prompt fallback
- [x] 23-03: Verify the visual runtime contract with regression coverage, smoke artifacts, and operator docs

### Phase 24: Add post-visual-review positioning-card analysis step for approved creators

**Goal:** 在不改变现有 visual review gate 的前提下，为 visual-review-pass 的达人新增 repo-local positioning-card analysis 步骤，并让其结构化输出和阶段状态进入 downstream runner / final wrapper 的可观察 contract。
**Requirements**: [POS-01, POS-02, POS-03]
**Depends on:** Phase 23
**Success Criteria** (what must be TRUE):
  1. downstream runtime 只会在 visual review 已通过的达人上执行 positioning-card analysis，不会替换或前移现有 visual review Pass/Reject gate
  2. positioning-card analysis 会留下 machine-readable 结果，至少包含定位标签、品牌适配结论/建议和简明证据，并能被 operator 查看或导出
  3. keep-list downstream runner 与 final wrapper summary 会显式暴露 positioning-card analysis 的 stage 状态、artifact 或摘要，且第一版默认不会因为该步骤失败就阻断 final export
**Plans:** 2 plans

Plans:
- [x] 24-01: Add backend-owned positioning-card analysis contract and wire it into the downstream runner stage graph
- [x] 24-02: Surface positioning artifacts through wrapper/docs and leave repo-local verification guidance

**Details:**
Phase 24 已完成。定位卡分析现在作为 `positioning_card_analysis` stage 运行在 visual-review-pass 达人之后，并通过 backend artifact、downstream summary、top-level final-wrapper summary 暴露结构化产物与阶段状态。第一版默认保持 non-blocking：stage 可 skipped / failed，但不会单独把 final export 提升为硬失败。contract note 留在 `.planning/phases/24-add-post-visual-review-positioning-card-analysis-step-for-approved-creators/24-POSITIONING-CARD-CONTRACT.md`，回归验证为 `PYTHONPYCACHEPREFIX=/tmp/pycache backend/.venv/bin/python -m unittest tests.test_visual_provider_diagnostics tests.test_keep_list_screening_pipeline tests.test_task_upload_to_final_export_pipeline -v`，结果 `85 tests, OK`。

### Phase 25: Build local thin operator UI for task-driven screening runs

**Goal**: 在当前 repo-local 单入口主线之上提供一个本地薄 operator UI，让操作者可以读取飞书任务、触发既有 runner、查看阶段状态与 summary/artifact 路径，而不需要手敲长命令。
**Requirements**: [OPS-UI-01, OPS-UI-02, OPS-UI-03]
**Depends on:** Phase 24
**Success Criteria** (what must be TRUE):
  1. 本地页面可以读取 task-upload 任务列表，并展示足够的任务信息让 operator 选择要跑的任务
  2. 页面可以触发现有 `task upload -> final export` canonical runner，并持续显示当前 stage、错误与关键产物路径
  3. 页面可以直接暴露最终 Excel 导出和 summary 路径；v1 明确保持 local-only，不承诺飞书写回
**Plans:** 2 plans

Plans:
- [x] 25-01: Add a backend-served local operator control plane for task discovery, run launch, and summary polling
- [x] 25-02: Add the first local HTML operator page and prove one bounded MINISO run through the page-driven contract

**Details:**
Phase 25 已完成。`25-01` 落下第一版 backend-served control plane：`/operator` 页面、`/api/operator/tasks`、`/api/operator/runs`、`/api/operator/runs/<id>`、`/api/operator/file` 已落地，并通过 `backend/.venv/bin/python -m unittest tests.test_visual_provider_diagnostics tests.test_task_upload_to_final_export_pipeline -v` 的 77 个 targeted tests。`25-02` 又用 operator API 真实发起了一轮 bounded `MINISO`，运行根目录留在 `temp/operator_runs/20260330_150053_MINISO_5a2afccf`，最终 top-level `summary.json` 与 `downstream/summary.json` 都为 `completed`，并产出 `instagram_final_review.xlsx` 与定位卡导出。

### Phase 25.1: Fix MINISO LLM auth failures, redact summary secrets, surface missing duet sending-list diagnostics, and clarify LLM stage observability (INSERTED)

**Goal**: 修复 `task upload -> keep-list` 上游链路里已经实跑暴露的紧急问题：让文本 LLM 调用的鉴权/路由可以被清晰归因、让 summary 不再落敏感字段、并把缺 `发信名单` 与 `llm_candidates`/`llm_review` 的失败边界拆清楚。
**Requirements**: [OPS-REL-01, OPS-REL-02, OPS-REL-03]
**Depends on:** Phase 25
**Success Criteria** (what must be TRUE):
  1. `task upload -> keep-list` 上游 summary 会显式暴露文本 LLM review 实际使用的 provider/base/model/wire_api，并且 `auth_not_found` 之类的失败归因到 `llm_review` 调用面，而不是模糊地挂到 `llm_candidates`
  2. `summary.json` 不再直接持久化 `imapCode`、飞书 file token、未脱敏邮箱地址或整包 raw task/mail payload，同时 downstream 仍能通过最小 handoff contract 读取任务 owner 上下文
  3. 缺 `发信名单`、任务上传重复命中、`llm_candidates_prepare`、`llm_review_call`、`llm_review_writeback` 都会成为独立、可机读的诊断块，排查时不需要靠聊天上下文还原失败位置
**Plans:** 2 plans

Plans:
- [ ] 25.1-01: Redact upstream summary payloads and surface explicit task-assets diagnostics
- [ ] 25.1-02: Add LLM review config diagnostics and split `llm_review` failure attribution

### Phase 26: Backfill decoupling verification bundle for Phases 20-21

**Goal**: 把 Phase 20-21 已经实现的 repo-local decoupling 结果补齐成可审计的 phase-local evidence bundle，并让 DEP requirements 的 traceability 从“历史完成”切回“待重新认证”。
**Depends on:** Phase 25
**Requirements**: [DEP-01, DEP-02, DEP-03, DEP-04]
**Gap Closure:** Closes milestone audit gaps for missing verification on Phases 20-21
**Success Criteria** (what must be TRUE):
  1. Phase 20 与 Phase 21 都有正式 `VERIFICATION.md`，能把实现、测试和 repo-local contract 证据串起来
  2. `SUMMARY.md` / frontmatter / `REQUIREMENTS.md` / `ROADMAP.md` 对 DEP requirements 的口径一致，不再出现“完成但无法审计”的状态分裂
  3. re-audit 时，DEP requirements 不再因为 Phase 20-21 缺 verification 而被判为 partial
**Plans:** 2 plans

Plans:
- [ ] 26-01: Backfill Phase 20-21 verification artifacts and normalize summary requirement frontmatter
- [ ] 26-02: Reconcile decoupling traceability across roadmap, requirements, and repo-local operator docs

### Phase 27: Reconstruct runtime safety proof and fallback certification for Phase 22

**Goal**: 用现有 bounded regression proof 与 fallback artifact 重建正式的 Phase 22 planning / summary / verification bundle，完成 SAF requirements 的审计闭环。
**Depends on:** Phase 26
**Requirements**: [SAF-01, SAF-02]
**Gap Closure:** Closes milestone audit gaps for missing Phase 22 artifact bundle
**Success Criteria** (what must be TRUE):
  1. Phase 22 目录补齐 context / summary / verification artifact，并明确引用 `temp/phase22_decoupled_bounded_validation` 等现有 proof
  2. `task upload -> final export` bounded regression 与 fallback contract 的 operator 路径可以被 phase-local 文档和验证文件直接证明
  3. re-audit 时，SAF requirements 不再因为 Phase 22 空目录而被判为 unsatisfied
**Plans:** 2 plans

Plans:
- [ ] 27-01: Rebuild the Phase 22 planning bundle from existing bounded regression and fallback artifacts
- [ ] 27-02: Add formal verification coverage for repo-local runtime safety and fallback recovery

### Phase 28: Reconstruct visual runtime and positioning evidence chain for Phases 23-24

**Goal**: 为 visual runtime contract 与 positioning-card analysis 补齐缺失的 phase artifacts、verification 和跨阶段证据链，使 VIS/POS requirements 可以重新通过 audit。
**Depends on:** Phase 27
**Requirements**: [VIS-01, VIS-02, VIS-03, POS-01, POS-02, POS-03]
**Gap Closure:** Closes milestone audit gaps for missing Phase 23 artifacts and missing Phase 24 verification
**Success Criteria** (what must be TRUE):
  1. 缺失的 Phase 23 目录、plan/summary/verification artifacts 被补建，并能引用已有实现提交与回归测试
  2. Phase 24 获得正式 `VERIFICATION.md`，且 `SUMMARY.md` frontmatter / contract note / requirements traceability 一致
  3. re-audit 时，`23 -> 24` 的 visual-to-positioning integration chain 可被 phase-local artifacts 直接证明
**Plans:** 3 plans

Plans:
- [ ] 28-01: Recreate the missing Phase 23 planning bundle for the visual runtime contract
- [ ] 28-02: Backfill verification evidence linking Phase 23 visual runtime outputs into Phase 24 positioning behavior
- [ ] 28-03: Normalize Phase 24 requirement traceability and verification coverage for POS requirements

### Phase 29: Reconstruct operator UI evidence bundle and milestone closeout consistency

**Goal**: 为 operator UI 补建 Phase 25 planning bundle，并把 roadmap checklist / state / audit closeout 口径收口，准备重新执行 milestone audit。
**Depends on:** Phase 28
**Requirements**: [OPS-UI-01, OPS-UI-02, OPS-UI-03]
**Gap Closure:** Closes milestone audit gaps for missing Phase 25 artifacts and closeout inconsistencies
**Success Criteria** (what must be TRUE):
  1. Phase 25 对应的 planning / summary / verification artifacts 补齐，并引用现有 `/operator` proof 和 bounded run artifact
  2. `ROADMAP.md` / `STATE.md` / audit prerequisites 对 operator UI 的状态一致，不再出现 checklist 与 progress table 冲突
  3. re-audit 时，OPS-UI requirements 与 local operator flow 可以被正式认证，并直接衔接 milestone closeout
**Plans:** 2 plans

Plans:
- [ ] 29-01: Recreate the missing Phase 25 planning bundle for the local operator UI
- [ ] 29-02: Add operator verification coverage and align milestone closeout docs for re-audit
