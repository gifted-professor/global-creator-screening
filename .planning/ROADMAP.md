# Roadmap: chuhaihai

## Milestones

- ✅ **v1.0.0 Consolidated Local Creator Screening Pipeline** — Phases 1-13 shipped 2026-03-28. Archive: `.planning/milestones/v1.0.0-ROADMAP.md`
- ✅ **v1.1.0 Visual Provider Reliability and Downstream Hardening** — Phases 14-15 shipped 2026-03-28. Archive: `.planning/milestones/v1.1.0-ROADMAP.md`
- 🚧 **v1.2.0 End-to-End Single-Entry Pipeline Verification** — Phases 16-18 planned

## Overview

`v1.2.0` 的目标是把已经分别验证过的两段真实业务链拼成一个 repo-local 的单入口 E2E：从任务上传起点出发，经过 mail sync、creator enrichment、duplicate review、keep-list、screening downstream，最终落到导出。重点不是重写业务逻辑，而是把 orchestration、runtime contract、early failure 和 bounded proof run 收口。

## Phases

- [ ] **Phase 16: Build single-entry task-to-keep-list orchestrator** - 把任务上传起点到 keep-list 生成编排成单入口，并定义稳定 handoff contract
- [ ] **Phase 17: Close repo-local runtime dependency gaps** - 收口 env、任务附件、数据库和 legacy 依赖解析，让失败语义前移
- [ ] **Phase 18: Validate real bounded end-to-end pipeline** - 用真实 `MINISO` 跑单入口 bounded E2E，并收口 operator 文档

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 16. Build single-entry task-to-keep-list orchestrator | 0/2 | Planned | |
| 17. Close repo-local runtime dependency gaps | 0/2 | Not Started | |
| 18. Validate real bounded end-to-end pipeline | 0/2 | Not Started | |

### Phase 16: Build single-entry task-to-keep-list orchestrator

**Goal**: 把 task upload 起点到 keep-list 生成编排成一个 repo-local 单入口，并定义可 resume 的 handoff summary contract。
**Depends on**: Phase 15
**Requirements**: [E2E-01, E2E-02, RTM-03]
**Success Criteria** (what must be TRUE):
  1. 操作人可以基于任务标识触发单入口 runner，一次性跑完 task asset prep、mail sync、creator enrichment、duplicate review 和 keep-list 产出
  2. runner 会输出机器可读 summary，逐步记录每个阶段的状态、输入边界和 artifact 路径
  3. keep-list handoff 到 screening downstream 的契约稳定，允许从已生成 artifact resume
**Plans**: 2 plans

Plans:
- [ ] 16-01: Define end-to-end orchestration contract, step summary schema, and keep-list handoff boundaries
- [ ] 16-02: Add single-entry runner through keep-list generation with bounded controls and resume support

### Phase 17: Close repo-local runtime dependency gaps

**Goal**: 收口单入口运行所需的 env、任务附件、数据库和 legacy 外部依赖解析，让失败在早期且可诊断。
**Depends on**: Phase 16
**Requirements**: [RTM-01, RTM-02]
**Success Criteria** (what must be TRUE):
  1. 单入口运行所需的 env、task attachments、数据库和中间产物路径都有明确的 repo-local 解析规则
  2. 如果仍依赖外部全量 `email` 项目或其他 legacy 资产，runner 会在早期暴露明确原因和 remediation，而不是中途模糊失败
  3. README / summary 会明确列出单入口运行的前置条件和诊断信息
**Plans**: 2 plans

Plans:
- [ ] 17-01: Normalize env, attachment, database, and intermediate artifact resolution for single-entry runs
- [ ] 17-02: Add early-failure diagnostics for legacy dependencies and missing prerequisites

### Phase 18: Validate real bounded end-to-end pipeline

**Goal**: 基于单入口 runner 对真实 `MINISO` 执行一轮 bounded E2E proof run，并把 operator path 文档化。
**Depends on**: Phase 17
**Requirements**: [E2E-03, VAL-01, VAL-02]
**Success Criteria** (what must be TRUE):
  1. 真实 `MINISO` bounded run 可以从任务上传起点跑到最终导出
  2. summary 会明确记录 bounded controls、各阶段状态和关键产物路径
  3. README 与 planning 文档给出精确复跑命令、限制条件和 proof artifact 位置
**Plans**: 2 plans

Plans:
- [ ] 18-01: Execute a real bounded MINISO end-to-end run and capture proof artifacts
- [ ] 18-02: Document rerun path, limitations, and final validation outcomes
