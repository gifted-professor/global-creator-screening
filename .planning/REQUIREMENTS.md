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

## Out of Scope

| Feature | Reason |
|---------|--------|
| 报价结果写入筛号运行态或最终导出 | 本里程碑先解决 dependency removal，避免与数据接入改动互相耦合 |
| 多平台/大批量 live stability proof | 当前先确保 decoupling 不破坏既有主链，再扩大稳定性验证样本 |
| 重写 `email_sync` 或 `筛号` 核心业务逻辑 | 本里程碑目标是依赖拆除与入口收口，不是业务重构 |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| DEP-04 | Phase 20 | Pending |
| DEP-01 | Phase 21 | Pending |
| DEP-02 | Phase 21 | Pending |
| DEP-03 | Phase 21 | Pending |
| SAF-01 | Phase 22 | Pending |
| SAF-02 | Phase 22 | Pending |

**Coverage:**
- v1 requirements: 6 total
- Mapped to phases: 6
- Unmapped: 0

---
*Requirements defined: 2026-03-29*
*Last updated: 2026-03-29 after milestone v1.3.0 initialization*
