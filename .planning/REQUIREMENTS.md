# Requirements: chuhaihai

**Defined:** 2026-03-29
**Milestone:** `v1.3.0 External Email Dependency Decoupling`
**Core Value:** 在不打断现有本地工作流的前提下，把飞书内容获取、筛选导入和相关配置集中到一个可持续维护的仓库里。

## v1 Requirements

### Dependency Decoupling

- [x] **DEP-01**: 操作人可以在当前仓库直接完成 workbook 相关流程，不再要求 external full `email` 项目存在
- [x] **DEP-02**: 操作人可以在当前仓库直接完成 dashboard 相关流程，不再要求 external full `email` 项目存在
- [x] **DEP-03**: 操作人可以在当前仓库直接完成 project-home 相关流程，不再要求 external full `email` 项目存在
- [x] **DEP-04**: 所有 legacy external dependency 检查点都改为 repo-local contract，错误信息只给出当前仓库内可执行 remediation

### Runtime Safety

- [x] **SAF-01**: 拆依赖后，`task upload -> final export` 的 bounded runner 回归不出现功能倒退
- [x] **SAF-02**: 迁移结果提供明确的兼容与回退策略（配置开关或文档化手工回退步骤），避免一次性切换导致主流程不可恢复

## v2 Requirements

### Deferred Follow-Ups

- **QTE-01**: 把报价结果正式接入 `筛号` 运行态或最终导出链
- **REL-01**: 为多平台或更大批量补充稳定性证明，而不只停留在 bounded validation

### Visual Runtime Contract

- [ ] **VIS-01**: 如果模板编译产出了 `visual_prompts.json`，runtime visual review 必须按 platform/provider 消费它，而不是继续对所有品牌复用同一套通用 prompt
- [ ] **VIS-02**: `active_rulespec.rules` 中的 `visual_feature_group` 与支持的视觉排除项必须在 runtime 有实际消费面，至少影响 cover count、prompt contract 或等价 runtime diagnostics
- [ ] **VIS-03**: 保留 `mimo` / `qwen-vl-max` 的 provider-specific prompt capability，且无模板场景继续 fallback 到现有通用 prompt 变体

## Out of Scope

| Feature | Reason |
|---------|--------|
| 报价结果写入筛号运行态或最终导出 | 本里程碑先解决 dependency removal，避免与数据接入改动互相耦合 |
| 多平台/大批量 live stability proof | 当前先确保 decoupling 不破坏既有主链，再扩大稳定性验证样本 |
| 重写 `email_sync` 或 `筛号` 核心业务逻辑 | 本里程碑目标是依赖拆除与入口收口，不是业务重构 |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| DEP-04 | Phase 20 | Completed |
| DEP-01 | Phase 21 | Completed |
| DEP-02 | Phase 21 | Completed |
| DEP-03 | Phase 21 | Completed |
| SAF-01 | Phase 22 | Completed |
| SAF-02 | Phase 22 | Completed |
| VIS-01 | Phase 23 | Planned |
| VIS-02 | Phase 23 | Planned |
| VIS-03 | Phase 23 | Planned |

**Coverage:**
- v1 requirements: 6 total
- Follow-up requirements: 5 total
- Mapped to phases: 9
- Unmapped follow-ups: 2 (`QTE-01`, `REL-01`)

---
*Requirements defined: 2026-03-29*
*Last updated: 2026-03-30 after Phase 23 planning*
