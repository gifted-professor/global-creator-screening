# Quick Task 260331-gru: 将本地 CLIProxyAPI gpt-5.4 接成默认 openai 视觉 provider，并补最小验证 - Context

**Gathered:** 2026-03-31
**Status:** Ready for execution

## Task Boundary

把这台机器上的默认 `openai` 视觉 provider 从旧的第三方 OpenAI-compatible 地址切到本地 `CLIProxyAPI`：

- `OPENAI_BASE_URL=http://127.0.0.1:8317/v1`
- `OPENAI_API_KEY=cliproxyapi-local`
- `OPENAI_VISION_MODEL=gpt-5.4`

并做最小真实验证，确认文字与图片接口都可用。

## Decisions

- 不修改仓库代码默认值，只改本机 `.env.local`
- 不移除现有 `qiandao` / `quan2go` / 其他 fallback 路由
- 以真实 HTTP 请求成功作为本轮完成标准

## References

- [backend/app.py](/Users/a1234/Desktop/Coding/chuhaihai/backend/app.py)
- [260331-gn2-SUMMARY.md](/Users/a1234/Desktop/Coding/chuhaihai/.planning/quick/260331-gn2-cliproxyapi-gpt-5-4-gpt-provider/260331-gn2-SUMMARY.md)
