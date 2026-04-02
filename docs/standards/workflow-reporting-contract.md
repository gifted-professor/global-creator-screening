# Workflow Reporting Contract

本标准定义外部 `$workflow` 在消费当前仓库 `workflow_handoff.json` 后，默认应该如何向人回传结果。

目标只有一个：统一“汇报契约”。

- 不增加新的执行壳
- 不替代 `summary.json`
- 不替代 `task_spec.json`
- 不扩自治层

## Source Order

默认读取顺序固定为：

1. 先读 `workflow_handoff.json`
2. 需要确认规范化意图时，再读 `task_spec.json`
3. 只有需要 runtime 细节、artifact 细节或 step 细节时，才继续读 `summary.json`

## Default Report Shape

`$workflow` 默认按下面 8 段回传：

1. 本轮目标
2. 执行命令/输入
3. 当前状态
4. 已确认事实
5. 未确认部分
6. 结论
7. 下一步建议
8. 是否需要我决策

## Section Rules

### 1. 本轮目标

用一句话说明本轮 run 的目标，优先来自：

- `task_spec.intent`
- `task_spec.scope`
- `task_spec.canonical_boundary`

不要重新发明目标表述；优先用规范化后的意图描述。

### 2. 执行命令/输入

只汇报本轮真正关键的执行入口和输入边界，例如：

- 实际命令
- 主要输入文件
- 关键 flags
- 关键控制参数

不要把所有原始参数全量转述一遍。

### 3. 当前状态

必须优先来自：

- `workflow_handoff.verdict.outcome`
- `workflow_handoff.status`
- `workflow_handoff.recommended_action`
- `workflow_handoff.current_stage`
- `workflow_handoff.next_report_triggers`

推荐写法：

- `状态：running / completed / failed / blocked / stopped`
- `当前阶段：...`
- `下一次汇报触发条件：...`

如果是 terminal run，也仍然要写这一段。

### 4. 已确认事实

这里只写已经被当前 run 明确证明的事实，优先来自：

- `workflow_handoff.verdict`
- `workflow_handoff.failure_decision`
- `workflow_handoff.failure`
- `task_spec`
- `summary` 中已明确落盘的状态或 artifact

不要把猜测写成事实。

### 5. 未确认部分

列出当前仍未确认、仍需继续观察或下钻的点。

如果没有明显未确认项，也要明确写：

- `当前没有关键未确认项`

### 6. 结论

只在终态时写。

终态包括：

- `completed`
- `failed`
- `blocked`
- `stopped`

`running` 状态下不要写“结论”，也不要给最终判断。

### 7. 下一步建议

最多给两个建议。

优先依据：

- `workflow_handoff.recommended_action`
- `workflow_handoff.failure_decision`

建议必须是动作，不是泛泛评论。

### 8. 是否需要我决策

`$workflow` v1 当前固定回答：

- `不需要`

原因是当前 `workflow_handoff.json` machine contract 还没有正式的“需要用户分叉决策”信号。

在上游新增正式 signal 之前：

- 不要根据 `manual_fix` / `manual_investigation` / `retry_run` 自行推导 `需要`
- 不要让报告层发明 repo 里不存在的分叉决策状态

## Running Rule

`running` 状态下有一条硬规则：

- 不要给最终判断
- 不要写“结论”段
- 只汇报事实、当前阶段、下一次汇报触发条件

“下一次汇报触发条件”优先写成以下几类：

- 当前 step 进入终态
- 当前平台阶段切换
- 出现结构化 failure
- run 完成

如果 handoff 已经给出 `next_report_triggers`，优先直接使用 handoff 的结构化字段，不要重新发明另一套触发条件。

## Terminal Rule

`completed / failed / blocked / stopped` 状态下：

- 必须给结论
- 下一步建议最多两个
- Section 8 仍固定为 `不需要`

## Canonical Markdown Template

运行中：

```md
1. 本轮目标
...

2. 执行命令/输入
...

3. 当前状态
- 状态：running
- 当前阶段：...
- 下一次汇报触发条件：...

4. 已确认事实
- ...

5. 未确认部分
- ...

7. 下一步建议
- ...
- ...

8. 是否需要我决策
不需要
```

终态：

```md
1. 本轮目标
...

2. 执行命令/输入
...

3. 当前状态
- 状态：failed
- 当前阶段：runtime

4. 已确认事实
- ...

5. 未确认部分
- ...

6. 结论
...

7. 下一步建议
- ...
- ...

8. 是否需要我决策
不需要
```

## Relationship To Workflow Handoff

本标准是 `workflow_handoff.json` 的人类回传层，不是新的 machine contract。

- machine handoff: `workflow_handoff.json`
- human report: 本文定义的 `$workflow` 回传格式

两者必须保持一致，不能出现：

- handoff 说 `retryable=true`，但汇报里说“先不要重试”
- handoff 说 `running`，但汇报里提前写最终失败结论
