# Requirements: chuhaihai

**Defined:** 2026-03-29
**Milestone:** `v1.3.0 External Email Dependency Decoupling`
**Core Value:** 在不打断现有本地工作流的前提下，把飞书内容获取、筛选导入和相关配置集中到一个可持续维护的仓库里。

## v1 Requirements

### Dependency Decoupling

- [ ] **DEP-01**: 操作人可以在当前仓库直接完成 workbook 相关流程，不再要求 external full `email` 项目存在
- [ ] **DEP-02**: 操作人可以在当前仓库直接完成 dashboard 相关流程，不再要求 external full `email` 项目存在
- [ ] **DEP-03**: 操作人可以在当前仓库直接完成 project-home 相关流程，不再要求 external full `email` 项目存在
- [ ] **DEP-04**: 所有 legacy external dependency 检查点都改为 repo-local contract，错误信息只给出当前仓库内可执行 remediation

### Runtime Safety

- [ ] **SAF-01**: 拆依赖后，`task upload -> final export` 的 bounded runner 回归不出现功能倒退
- [ ] **SAF-02**: 迁移结果提供明确的兼容与回退策略（配置开关或文档化手工回退步骤），避免一次性切换导致主流程不可恢复

## v2 Requirements

### Deferred Follow-Ups

- **QTE-01**: 把报价结果正式接入 `筛号` 运行态或最终导出链
- **REL-01**: 为多平台或更大批量补充稳定性证明，而不只停留在 bounded validation

### Visual Runtime Contract

- [ ] **VIS-01**: 如果模板编译产出了 `visual_prompts.json`，runtime visual review 必须按 platform/provider 消费它，而不是继续对所有品牌复用同一套通用 prompt
- [ ] **VIS-02**: `active_rulespec.rules` 中的 `visual_feature_group` 与支持的视觉排除项必须在 runtime 有实际消费面，至少影响 cover count、prompt contract 或等价 runtime diagnostics
- [ ] **VIS-03**: 保留 `mimo` / `qwen-vl-max` 的 provider-specific prompt capability，且无模板场景继续 fallback 到现有通用 prompt 变体

### Positioning Card Analysis

- [ ] **POS-01**: visual review 通过的达人可以在 visual review 之后进入独立的 positioning-card analysis；现有 visual review 的 Pass/Reject gate 保持不变
- [ ] **POS-02**: positioning-card analysis 产出 machine-readable 的结构化结果，至少包含定位标签、品牌适配结论/建议、以及支撑这些判断的简明证据
- [ ] **POS-03**: keep-list downstream runner 与 final wrapper 必须显式暴露 positioning-card analysis 的 stage 状态、artifact 路径或摘要；第一版默认不把该步骤失败变成 final export 的硬阻塞

### Task-Driven Runtime Hardening

- [ ] **OPS-REL-01**: `task upload -> keep-list` 文本 LLM review 路径必须显式暴露最终选中的 provider/base/model/wire_api 与候选链，鉴权或路由失败时能在 `llm_review` 阶段直接归因，而不是依赖视觉模型配置猜测
- [ ] **OPS-REL-02**: operator-facing `summary.json` / 中间产物不得持久化 raw `imapCode`、飞书 file token、未脱敏邮箱地址或整包 raw task/mail payload；如 downstream 仍需精确 owner 上下文，必须通过单独的最小 handoff artifact 提供
- [ ] **OPS-REL-03**: 上游任务驱动运行必须把缺 `发信名单`、任务上传重复/未命中、`llm_candidates_prepare`、`llm_review_call`、`llm_review_writeback` 拆成独立、可机读的诊断块，便于 operator 直接从 summary 归因

### Local Operator UI

- [ ] **OPS-UI-01**: operator 可以在本地页面查看 task-upload 任务列表，并基于任务名选择要执行的 screening run
- [ ] **OPS-UI-02**: operator 可以从本地页面启动现有 canonical `task upload -> final export` runner，并在页面上轮询查看 stage、错误、summary 路径和关键 artifact
- [ ] **OPS-UI-03**: operator 可以在 run 完成后从页面直接拿到最终导出 Excel 与 summary 路径；v1 保持 local-only，不要求飞书写回

### Shared Mailbox Post-Sync Operator Mainline

- [ ] **SMB-01**: operator 可以从本地页面直接启动共享邮箱 post-sync 正式主线，输入预同步 shared mailbox `email_sync.db` / raw dir，而不在 operator 路径里重新抓 IMAP
- [ ] **SMB-02**: operator-facing summary / UI 必须显式暴露共享邮箱主线的 top-level 与 per-task 增量结果，包括 `task_results`、`new_creator_count`、`existing_screened_count`、`existing_unscreened_count`、`full_screening_count`、`mail_only_update_count`、以及 aggregate local archive / task-level payload / upload result 路径
- [ ] **SMB-03**: local operator launch contract 必须把 dry-run 与 live writeback intent、owner ambiguity / duplicate guards、以及“已筛号达人只更新邮件字段不覆盖 AI 字段”的边界暴露清楚，避免页面入口误触发不可解释的写回

## Out of Scope

| Feature | Reason |
|---------|--------|
| 报价结果写入筛号运行态或最终导出 | 本里程碑先解决 dependency removal，避免与数据接入改动互相耦合 |
| 多平台/大批量 live stability proof | 当前先确保 decoupling 不破坏既有主链，再扩大稳定性验证样本 |
| 重写 `email_sync` 或 `筛号` 核心业务逻辑 | 本里程碑目标是依赖拆除与入口收口，不是业务重构 |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| OPS-REL-01 | Phase 25.1 | Pending |
| OPS-REL-02 | Phase 25.1 | Pending |
| OPS-REL-03 | Phase 25.1 | Pending |
| DEP-04 | Phase 26 | Pending |
| DEP-01 | Phase 26 | Pending |
| DEP-02 | Phase 26 | Pending |
| DEP-03 | Phase 26 | Pending |
| SAF-01 | Phase 27 | Pending |
| SAF-02 | Phase 27 | Pending |
| VIS-01 | Phase 28 | Pending |
| VIS-02 | Phase 28 | Pending |
| VIS-03 | Phase 28 | Pending |
| POS-01 | Phase 28 | Pending |
| POS-02 | Phase 28 | Pending |
| POS-03 | Phase 28 | Pending |
| OPS-UI-01 | Phase 29 | Pending |
| OPS-UI-02 | Phase 29 | Pending |
| OPS-UI-03 | Phase 29 | Pending |
| SMB-01 | Phase 30 | Pending |
| SMB-02 | Phase 30 | Pending |
| SMB-03 | Phase 30 | Pending |

**Coverage:**
- v1 requirements: 6 total
- Follow-up requirements: 17 total
- Mapped to phases: 21
- Unmapped follow-ups: 2 (`QTE-01`, `REL-01`)

---
*Requirements defined: 2026-03-29*
*Last updated: 2026-04-01 after Phase 30 planning*
