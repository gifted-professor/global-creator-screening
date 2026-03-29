# Roadmap: chuhaihai

## Milestones

- ✅ **v1.0.0 Consolidated Local Creator Screening Pipeline** — Phases 1-13 shipped 2026-03-28. Archive: `.planning/milestones/v1.0.0-ROADMAP.md`
- ✅ **v1.1.0 Visual Provider Reliability and Downstream Hardening** — Phases 14-15 shipped 2026-03-28. Archive: `.planning/milestones/v1.1.0-ROADMAP.md`
- ✅ **v1.2.0 End-to-End Single-Entry Pipeline Verification** — Phases 16-19 shipped 2026-03-29. Archive: `.planning/milestones/v1.2.0-ROADMAP.md`
- 🚧 **v1.3.0 External Email Dependency Decoupling** — Phases 20-22 planned (in progress)

## Overview

`v1.3.0` 先聚焦 `DEP-01`：把 workbook / dashboard / project-home 对 external full `email` 项目的剩余耦合从运行主线里拆掉，确保 operator 在当前仓库里就能完成关键入口流程。`QTE-01` 与 `REL-01` 保留为后续里程碑，避免本轮把 dependency removal、数据接入和大样本稳定性验证混成同一交付面。

## Phases

- [ ] **Phase 20: Baseline legacy dependency surfaces and lock repo-local replacement contract** - 盘点 workbook / dashboard / project-home 的外部依赖触点并定义统一替换规则
- [ ] **Phase 21: Replace workbook/dashboard/project-home runtime paths with repo-local implementations** - 把核心入口从 external full `email` 依赖切到当前仓库
- [ ] **Phase 22: Validate decoupled runtime stability and operator fallback contract** - 回归主链并收口兼容回退说明，确保迁移可安全落地

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 20. Baseline legacy dependency surfaces and lock repo-local replacement contract | 2/2 | Completed | 2026-03-29 |
| 21. Replace workbook/dashboard/project-home runtime paths with repo-local implementations | 2/2 | Completed | 2026-03-29 |
| 22. Validate decoupled runtime stability and operator fallback contract | 0/0 | Not Started | — |

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
- [ ] 22-01: Execute bounded regression validation on decoupled runtime and capture proof artifacts
- [ ] 22-02: Finalize operator runbook for fallback/recovery and align planning docs with observed behavior
