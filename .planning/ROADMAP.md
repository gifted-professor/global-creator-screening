# Roadmap: chuhaihai

## Milestones

- ✅ **v1.0.0 Consolidated Local Creator Screening Pipeline** — Phases 1-13 shipped 2026-03-28. Archive: `.planning/milestones/v1.0.0-ROADMAP.md`
- ✅ **v1.1.0 Visual Provider Reliability and Downstream Hardening** — Phases 14-15 shipped 2026-03-28. Archive: `.planning/milestones/v1.1.0-ROADMAP.md`
- ✅ **v1.2.0 End-to-End Single-Entry Pipeline Verification** — Phases 16-19 shipped 2026-03-29. Archive: `.planning/milestones/v1.2.0-ROADMAP.md`
- 🚧 **v1.3.0 External Email Dependency Decoupling** — Phases 20-22 completed; follow-up Phase 23 added for visual runtime contract work

## Overview

`v1.3.0` 先聚焦 `DEP-01`：把 workbook / dashboard / project-home 对 external full `email` 项目的剩余耦合从运行主线里拆掉，确保 operator 在当前仓库里就能完成关键入口流程。`QTE-01` 与 `REL-01` 保留为后续里程碑，避免本轮把 dependency removal、数据接入和大样本稳定性验证混成同一交付面。

`Phase 23` 是在 decoupling proof 之后补录的视觉 runtime contract follow-up：重点不是继续做 dependency removal，而是把模板编译出的品牌视觉 prompt / feature contract 真正接入 runtime。当前先按 repo-local follow-up phase 收口，避免“编译有品牌规则、runtime 还在跑通用 prompt”的断裂继续带进下游。

## Phases

- [x] **Phase 20: Baseline legacy dependency surfaces and lock repo-local replacement contract** - 盘点 workbook / dashboard / project-home 的外部依赖触点并定义统一替换规则
- [x] **Phase 21: Replace workbook/dashboard/project-home runtime paths with repo-local implementations** - 把核心入口从 external full `email` 依赖切到当前仓库
- [x] **Phase 22: Validate decoupled runtime stability and operator fallback contract** - 回归主链并收口兼容回退说明，确保迁移可安全落地
- [ ] **Phase 23: Wire template-compiled visual prompts into runtime and define visual feature-group contract** - 把模板编译出的品牌视觉 prompt 接回 runtime，并定义 `visual_feature_group` 的消费契约

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 20. Baseline legacy dependency surfaces and lock repo-local replacement contract | 2/2 | Completed | 2026-03-29 |
| 21. Replace workbook/dashboard/project-home runtime paths with repo-local implementations | 2/2 | Completed | 2026-03-29 |
| 22. Validate decoupled runtime stability and operator fallback contract | 2/2 | Completed | 2026-03-29 |
| 23. Wire template-compiled visual prompts into runtime and define visual feature-group contract | 0/3 | Planned | — |

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
- [ ] 23-01: Persist compiled visual prompt bundles into active runtime and resolve brand/provider prompt selection
- [ ] 23-02: Consume `visual_feature_group` / supported visual exclusions in runtime cover selection and prompt fallback
- [ ] 23-03: Verify the visual runtime contract with regression coverage, smoke artifacts, and operator docs
