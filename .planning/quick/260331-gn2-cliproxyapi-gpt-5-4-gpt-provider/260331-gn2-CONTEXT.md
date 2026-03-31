# Quick Task 260331-gn2: 验证 CLIProxyAPI 本地 gpt-5.4 文字与图片链路，判断是否可作为首选 GPT provider 接入现有视觉主链 - Context

**Gathered:** 2026-03-31
**Status:** Ready for planning

## Task Boundary

验证本机 `CLIProxyAPI` 暴露的本地 OpenAI-compatible HTTP 接口是否可稳定完成：

- 文字 `chat/completions`
- 图片 `responses`

并判断当前仓库现有 `openai` 视觉 provider 是否可以通过纯配置方式接入这条通道。

## Implementation Decisions

### 验证范围
- 先做真实 HTTP 请求验证，不先改仓库代码。
- 先判断“通道是否可用”和“现有 provider 是否 wire-compatible”，不在本任务里切默认配置。

### 接入判断标准
- 文字请求成功返回 `gpt-5.4`
- 图片 `responses` 请求成功返回图片描述
- 当前仓库 `openai` provider 的 `base_url`、`api_style`、`model` 约定与该本地接口兼容

### Claude's Discretion
- 如果仓库内缺少可直接运行 backend probe 的 Python 环境，允许退回到代码路径核对 + 原始 HTTP 实测，不把环境缺口误判成 provider 逻辑不兼容。

## Specific Ideas

- 候选配置：
  - `OPENAI_BASE_URL=http://127.0.0.1:8317/v1`
  - `OPENAI_API_KEY=cliproxyapi-local`
  - `OPENAI_VISION_MODEL=gpt-5.4`

## Canonical References

- 用户给出的 `CLIProxyAPI` 本地接口说明与样例 `curl`
- [backend/app.py](/Users/a1234/Desktop/Coding/chuhaihai/backend/app.py)
