# Harness Workflow Handoff Standard

`workflow_handoff.json` 是给外部 `$workflow` 的瘦读取面，不替代 `summary.json` 或 `task_spec.json`。

如果 `$workflow` 需要把 run 结果继续回传给人，默认汇报格式遵循
[`workflow-reporting-contract.md`](./workflow-reporting-contract.md)。

## Run Selection Rule

`$workflow` v1 在恢复或续读时，默认采用：

- 显式路径优先

也就是说，默认应由调用方显式提供本轮要汇报的 run 路径，例如：

- `run_root`
- `workflow_handoff.json` 路径
- `summary.json` 路径

不要把“自动发现最新 run”当成默认定位策略。

原因很简单：

- 当前仓库可能同时存在 repo root run、integration worktree run、operator run 和其他临时 run
- `latest` 不一定等于“这次真正要汇报的 run”
- 一旦选错 run，后续读取 `workflow_handoff.json`、`task_spec.json` 和 `summary.json` 都会建立在错误对象上

如果显式路径缺失，`latest run` 最多只应作为人工辅助兜底，不应静默成为默认恢复来源。

当前标准读取顺序：

1. 先读 `workflow_handoff.json`
2. 先用 `verdict.outcome` 和 `recommended_action` 判断本轮结论
3. 再用 `failure_decision` 判断是补配置、补输入、人工排查还是直接重试
4. 只有需要还原规范化意图时，才继续读 `task_spec.json`
5. 只有需要 runtime 细节、step 进度或 artifact 细节时，才继续读 `summary.json`

第一版 handoff 额外保证两类最小稳定字段：

- `current_stage`
- `next_report_triggers`

这样外部 `$workflow` 在 `running` 状态下，不需要先回退到完整 `summary.json`，也能按汇报契约说出：

- 当前阶段
- 下一次汇报触发条件

第一版 handoff 只覆盖 canonical runs：

- `task-upload-to-final-export`
- `task-upload-to-keep-list`
- `keep-list-screening`

字段原则：

- `summary.json` 仍是详细执行记录
- `task_spec.json` 仍是单次 run 的意图真源
- `workflow_handoff.json` 只暴露 verdict-first 的动作分流信息，不重复整个 artifact 面
- `resume` 只暴露稳定摘要：`available / canonical_resume_point / resume_point_keys`
- 不再把整块 `resume_points` 原样透传给外部 workflow
