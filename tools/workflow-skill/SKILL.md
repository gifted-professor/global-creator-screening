---
name: workflow
description: 读取 canonical run 的 workflow_handoff.json，并按统一汇报契约输出 verdict-first 回报。当用户提到"$workflow"、"汇报这个 run"、"看这轮 run 结果"、"按 workflow contract 回报"时使用。
---

# Workflow

## Overview

把当前仓库已经稳定下来的 harness 输出转成统一的人类回报。

这个 skill 只负责消费现有 run 输出：
- 第一入口：`workflow_handoff.json`
- 可选补充：`task_spec.json`
- 深挖细节：`summary.json`

它不启动 run，不重试 run，也不替代 repo 内已有的 machine contract。

## When To Use

用户出现下面这类请求时使用：
- `"$workflow 看一下这个 run"`
- `"按 workflow contract 汇报这轮 run"`
- `"给我一个 verdict-first 的 run 回报"`
- `"这个 run_root 现在是什么状态"`

## Required Input

必须要求用户给出一个显式目标路径。只支持下面四类输入：
- `run_root`
- `workflow_handoff.json`
- `summary.json`
- `task_spec.json`

不要默认自动找最新 run。

如果用户没有给路径，先补问一句：

`请给我这轮 run 的显式路径：run_root、workflow_handoff.json、summary.json 或 task_spec.json 之一。`

## Supported Boundary

`$workflow` v1 只支持：
- 能解析到有效 `workflow_handoff.json`
- 且 `scope` 属于 canonical runs 的 run

这里的“有效”至少包括：
- `workflow_handoff_version=harness.workflow-handoff.v1-draft`
- 顶层 required 字段齐全
- `verdict`、`resume`、`intent_summary`、`pointers` 等关键嵌套结构可用

当前 canonical scopes 只有：
- `task-upload-to-final-export`
- `task-upload-to-keep-list`
- `keep-list-screening`

如果 handoff 不存在，或 scope 不在上面这三类里，直接明确说明：

`这不是当前 $workflow v1 正式支持的 canonical run。`

不要偷偷回退到旧 summary-only 逻辑。

## Workflow

### Step 1. Resolve the Target

先运行：

```bash
python3 ~/.codex/skills/workflow/scripts/resolve_run_handoff.py "<target-path>"
```

这一步负责：
- 规范化显式路径
- 定位 `workflow_handoff.json`
- 校验 `workflow_handoff_version`
- 校验 v1 handoff 的必需字段和关键嵌套结构
- 校验 canonical scope
- 尝试读取 `task_spec.json`

### Step 2. Render the Report

如果 Step 1 成功，再运行：

```bash
python3 ~/.codex/skills/workflow/scripts/render_workflow_report.py "<target-path>"
```

默认回报必须遵守 8 段汇报契约：
1. 本轮目标
2. 执行命令/输入
3. 当前状态
4. 已确认事实
5. 未确认部分
6. 结论
7. 下一步建议
8. 是否需要我决策

## Reporting Rules

- `running` 状态下不要输出“结论”
- `running` 状态下必须直接使用 handoff 的：
  - `current_stage`
  - `next_report_triggers`
- `task_spec.json` 只是可选补充源，不是隐含必需品
- 如果 handoff 明示 `task_spec_available=false`，仍然正常汇报
- “是否需要我决策”在 v1 固定输出 `不需要`
- 纯 `manual_fix` / `manual_investigation` 场景，放到“下一步建议”，不要误报成“需要我决策”
- 如果 handoff version 不匹配、缺少 required 字段，或关键嵌套结构损坏，直接按“不支持的 canonical run”处理
- 如果未来真的要输出 `需要`，必须先由 repo 的 machine contract 明确定义正式决策信号，再由 `$workflow` 接入

## Notes

- `$workflow` 是人类汇报入口，不是新的执行壳
- machine side 仍以 repo 内已有的 `workflow_handoff.json` 为准
- 如果需要深挖 runtime 细节，再去读 `summary.json`
