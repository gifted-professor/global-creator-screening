# Requirements: chuhaihai

**Defined:** 2026-03-28
**Core Value:** 在不打断现有本地工作流的前提下，把飞书内容获取、筛选导入和相关配置集中到一个可持续维护的仓库里。

## v1 Requirements

### End-to-End Orchestration

- [ ] **E2E-01**: 操作人可以只用一次命令和任务标识，在当前仓库里启动从任务上传到最终导出的全流程运行
- [ ] **E2E-02**: 单入口 runner 会按固定顺序串起 task upload asset prep、mail sync、creator enrichment、duplicate review、keep-list staging 和 screening downstream，不要求手工搬运中间文件
- [ ] **E2E-03**: 单入口 runner 支持 bounded validation 参数和 resume 点，而不需要改代码或手工改路径

### Runtime Contracts

- [ ] **RTM-01**: 全流程运行所需的 env、任务附件、数据库和中间产物路径在当前仓库中有明确解析规则，不依赖隐式人工前置
- [ ] **RTM-02**: 如果某一步仍依赖外部全量 `email` 项目或其他 legacy 资产，runner 会在早期明确暴露依赖和失败原因，而不是在中途模糊失败
- [ ] **RTM-03**: duplicate review 与 screening downstream 之间使用稳定的 handoff contract，允许从中间 artifact resume，而不破坏单入口 orchestration

### Validation and Ops

- [ ] **VAL-01**: 真实 `MINISO` bounded run 可以从任务上传起点跑到最终导出，并留下机器可读 summary 与关键产物路径
- [ ] **VAL-02**: README 和 planning 文档明确给出单入口 E2E 的前置条件、复跑命令、边界参数和 artifact 位置

## v2 Requirements

### Deferred Follow-Ups

- **QTE-01**: 把报价结果正式接入 `筛号` 运行态或最终导出链
- **DEP-01**: 移除 workbook / dashboard / project-home 对外部全量 `email` 项目的剩余依赖
- **REL-01**: 为多平台或更大批量补充稳定性证明，而不只停留在 bounded validation

## Out of Scope

| Feature | Reason |
|---------|--------|
| 彻底重写 duplicate review 或 `筛号` 业务逻辑 | 当前目标是 orchestration 和 runtime contract 收口，不是重构已验证链路 |
| 云端部署、队列化或服务化调度 | 本 milestone 仍以本地单入口可复跑验证为主 |
| 为所有视觉 provider 补完整 live proof matrix | 先证明单入口 E2E 可跑通，再决定是否扩 provider 覆盖 |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| E2E-01 | Phase 16 | Pending |
| E2E-02 | Phase 16 | Pending |
| RTM-03 | Phase 16 | Pending |
| RTM-01 | Phase 17 | Pending |
| RTM-02 | Phase 17 | Pending |
| E2E-03 | Phase 18 | Pending |
| VAL-01 | Phase 18 | Pending |
| VAL-02 | Phase 18 | Pending |

**Coverage:**
- v1 requirements: 8 total
- Mapped to phases: 8
- Unmapped: 0

---
*Requirements defined: 2026-03-28*
*Last updated: 2026-03-28 after v1.2.0 milestone start*
